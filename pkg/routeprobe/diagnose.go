// Package routeprobe compares the routing table with a socket's kernel-selected
// source address without changing network configuration or sending UDP data.
package routeprobe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"

	"github.com/libp2p/go-netroute"
)

// RouteDiagnosis is a point-in-time comparison of two different observations.
// SocketInterfaces identifies interfaces owning SocketSource; it is not a packet
// capture and does not establish the interface on which traffic actually leaves.
// A successful probe does not establish remote connectivity.
type RouteDiagnosis struct {
	Destination                  net.IP
	Gateway                      net.IP
	RouteInterface               string
	RouteSource                  net.IP
	MTU                          int
	SocketSource                 net.IP
	SocketInterfaces             []string
	RouteSourceAssigned          bool
	SocketSourceOnRouteInterface bool
}

type diagnosisDeps struct {
	route      func(net.IP) (*net.Interface, net.IP, net.IP, error)
	source     func(context.Context, net.IP) (net.IP, error)
	interfaces func() (map[string][]net.IP, error)
}

// DiagnoseRoute reads the route and current interface addresses, then connects a
// UDP socket to dst:9 solely to observe source-address selection. It never writes
// to that socket and does not need a running kubevpn daemon or cluster access.
// The returned diagnosis may contain partial results when an observation fails.
// A nil RouteSource means the route lookup did not report one, rather than that
// the route has a stale source address.
func DiagnoseRoute(ctx context.Context, dst net.IP) (RouteDiagnosis, error) {
	return diagnoseRoute(ctx, dst, diagnosisDeps{
		route: func(ip net.IP) (*net.Interface, net.IP, net.IP, error) {
			router, err := netroute.New()
			if err != nil {
				return nil, nil, nil, fmt.Errorf("read route table: %w", err)
			}
			return router.Route(ip)
		},
		source: func(ctx context.Context, ip net.IP) (net.IP, error) {
			var dialer net.Dialer
			return probeSocketSource(ctx, ip, "9", dialer.DialContext)
		},
		interfaces: interfaceAddresses,
	})
}

func diagnoseRoute(ctx context.Context, dst net.IP, deps diagnosisDeps) (RouteDiagnosis, error) {
	diagnosis := RouteDiagnosis{Destination: append(net.IP(nil), dst...)}
	if dst.To16() == nil || dst.IsUnspecified() {
		return diagnosis, fmt.Errorf("invalid route probe destination: %v", dst)
	}
	if err := ctx.Err(); err != nil {
		return diagnosis, err
	}
	iface, gateway, source, err := deps.route(dst)
	if err != nil {
		return diagnosis, fmt.Errorf("find route to %s: %w", dst, err)
	}
	if iface == nil || iface.Name == "" {
		return diagnosis, fmt.Errorf("route to %s has no interface", dst)
	}
	diagnosis.RouteInterface = iface.Name
	diagnosis.MTU = iface.MTU
	diagnosis.Gateway = append(net.IP(nil), gateway...)
	diagnosis.RouteSource = append(net.IP(nil), source...)
	if err := ctx.Err(); err != nil {
		return diagnosis, err
	}
	diagnosis.SocketSource, err = deps.source(ctx, dst)
	if err != nil {
		return diagnosis, fmt.Errorf("probe socket source for %s: %w", dst, err)
	}
	if diagnosis.SocketSource.To16() == nil || diagnosis.SocketSource.IsUnspecified() {
		return diagnosis, fmt.Errorf("probe socket for %s returned no usable source address", dst)
	}
	if err := ctx.Err(); err != nil {
		return diagnosis, err
	}
	addresses, err := deps.interfaces()
	if err != nil {
		return diagnosis, fmt.Errorf("read interface addresses: %w", err)
	}
	if _, ok := addresses[iface.Name]; !ok {
		return diagnosis, fmt.Errorf("route interface %q is no longer present", iface.Name)
	}
	for name, ips := range addresses {
		for _, ip := range ips {
			if ip.Equal(diagnosis.SocketSource) {
				diagnosis.SocketInterfaces = append(diagnosis.SocketInterfaces, name)
				if name == iface.Name {
					diagnosis.SocketSourceOnRouteInterface = true
				}
				break
			}
		}
	}
	sort.Strings(diagnosis.SocketInterfaces)
	for _, ip := range addresses[iface.Name] {
		if ip.Equal(diagnosis.RouteSource) {
			diagnosis.RouteSourceAssigned = true
			break
		}
	}
	return diagnosis, ctx.Err()
}

func interfaceAddresses() (map[string][]net.IP, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	result := make(map[string][]net.IP, len(interfaces))
	for _, iface := range interfaces {
		addresses, err := iface.Addrs()
		if err != nil {
			return nil, fmt.Errorf("read addresses of %s: %w", iface.Name, err)
		}
		result[iface.Name] = nil
		for _, address := range addresses {
			switch value := address.(type) {
			case *net.IPNet:
				result[iface.Name] = append(result[iface.Name], value.IP)
			case *net.IPAddr:
				result[iface.Name] = append(result[iface.Name], value.IP)
			}
		}
	}
	return result, nil
}

func probeSocketSource(ctx context.Context, dst net.IP, port string, dial func(context.Context, string, string) (net.Conn, error)) (source net.IP, err error) {
	network := "udp4"
	if dst.To4() == nil {
		network = "udp6"
	}
	conn, err := dial(ctx, network, net.JoinHostPort(dst.String(), port))
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close source probe socket: %w", closeErr))
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	address, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return nil, fmt.Errorf("unexpected source probe local address type: %T", conn.LocalAddr())
	}
	return append(net.IP(nil), address.IP...), nil
}
