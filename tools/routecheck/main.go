// routecheck compares the routing table with the source address selected by a
// fresh socket. It never starts a kubevpn daemon or changes network settings.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/wencaiwulue/kubevpn/v2/pkg/routeprobe"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "routecheck:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("routecheck", flag.ContinueOnError)
	flags.SetOutput(out)
	port := flags.Int("port", 0, "optionally test a TCP handshake on this port; no application data is sent")
	timeout := flags.Duration("timeout", 3*time.Second, "timeout for socket probes")
	flags.Usage = func() {
		fmt.Fprintln(out, "Usage: routecheck [--port PORT] [--timeout 3s] IP")
		fmt.Fprintln(out, "Without --port, only a UDP connect is used to select a source; no packet is sent.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("exactly one numeric destination IP is required")
	}
	dst := net.ParseIP(flags.Arg(0))
	if dst == nil {
		return fmt.Errorf("invalid destination IP %q (hostnames and scoped IPv6 are not supported)", flags.Arg(0))
	}
	if *port < 0 || *port > 65535 {
		return errors.New("port must be between 1 and 65535 when enabled")
	}
	if *timeout <= 0 {
		return errors.New("timeout must be positive")
	}

	probeCtx, cancel := context.WithTimeout(ctx, *timeout)
	diagnosis, probeErr := routeprobe.DiagnoseRoute(probeCtx, dst)
	cancel()
	if err := writeDiagnosis(out, diagnosis, probeErr == nil); err != nil {
		return err
	}
	if probeErr != nil {
		return fmt.Errorf("diagnose route: %w", probeErr)
	}
	if *port == 0 {
		return nil
	}

	conn, err := (&net.Dialer{Timeout: *timeout}).DialContext(ctx, "tcp", net.JoinHostPort(dst.String(), strconv.Itoa(*port)))
	if err != nil {
		return fmt.Errorf("TCP handshake to %s failed: %w", net.JoinHostPort(dst.String(), strconv.Itoa(*port)), err)
	}
	defer conn.Close()
	_, err = fmt.Fprintf(out, "TCP handshake: connected (local %s, remote %s); no application data sent\n", conn.LocalAddr(), conn.RemoteAddr())
	return err
}

func writeDiagnosis(out io.Writer, d routeprobe.RouteDiagnosis, complete bool) error {
	if _, err := fmt.Fprintf(out, "Destination: %s\nRouting table: interface=%s source=%s gateway=%s MTU=%d\n", d.Destination, d.RouteInterface, d.RouteSource, d.Gateway, d.MTU); err != nil {
		return err
	}
	if !complete {
		source := "unavailable"
		if d.SocketSource != nil {
			source = d.SocketSource.String()
		}
		_, err := fmt.Fprintf(out, "UDP socket source: %s\nPartial diagnosis: interface ownership checks unavailable; no mismatch classification made.\n", source)
		return err
	}
	if _, err := fmt.Fprintf(out, "UDP socket source: %s (interfaces=%v)\n", d.SocketSource, d.SocketInterfaces); err != nil {
		return err
	}
	if d.RouteSource != nil && !d.RouteSourceAssigned {
		if _, err := fmt.Fprintln(out, "WARNING: the route's stored source is not assigned to its interface; check stale routes after an address change."); err != nil {
			return err
		}
	}
	if !d.SocketSourceOnRouteInterface {
		if _, err := fmt.Fprintln(out, "WARNING: the kernel-selected socket source is not assigned to the routing-table interface. VPN policy or source-selection rules may override the table; this does not identify a specific VPN as the cause."); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintln(out, "Source selection is not proof of packet egress or end-to-end reachability. This tool does not change routes, DNS, hosts, or daemon state.")
	return err
}
