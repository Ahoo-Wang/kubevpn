package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wencaiwulue/kubevpn/v2/pkg/routeprobe"
)

func TestRunRejectsInvalidRequestsBeforeProbing(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"missing destination", nil, "exactly one"},
		{"multiple destinations", []string{"127.0.0.1", "127.0.0.2"}, "exactly one"},
		{"hostname", []string{"service.example.invalid"}, "invalid destination IP"},
		{"scoped IPv6", []string{"fe80::1%lo0"}, "invalid destination IP"},
		{"negative port", []string{"--port=-1", "127.0.0.1"}, "port must be"},
		{"large port", []string{"--port=65536", "127.0.0.1"}, "port must be"},
		{"zero timeout", []string{"--timeout=0s", "127.0.0.1"}, "timeout must be positive"},
		{"unknown flag", []string{"--unknown", "127.0.0.1"}, "flag provided but not defined"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := run(context.Background(), tc.args, &out)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
			if strings.Contains(out.String(), "Routing table:") {
				t.Fatal("invalid input reached network probing")
			}
		})
	}
}

func TestRunHelpDoesNotProbe(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"--help"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "no packet is sent") {
		t.Fatalf("missing read-only help: %s", out.String())
	}
	if strings.Contains(out.String(), "Routing table:") {
		t.Fatal("help probed the network")
	}
}

// Wire the real route/source probe and command into a local TCP listener. The
// listener must see only a handshake and EOF, never an application request.
func TestRunLoopbackTCPHandshakeWithoutPayload(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	received := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			received <- err
			return
		}
		defer conn.Close()
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			received <- err
			return
		}
		var data [1]byte
		n, err := conn.Read(data[:])
		if n != 0 || !errors.Is(err, io.EOF) {
			received <- errors.New("probe sent data or did not close the connection")
			return
		}
		received <- nil
	}()
	var out bytes.Buffer
	port := listener.Addr().(*net.TCPAddr).Port
	args := []string{"--port", strconv.Itoa(port), "--timeout", "2s", "127.0.0.1"}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := run(ctx, args, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "TCP handshake: connected") {
		t.Fatalf("missing successful handshake report: %s", out.String())
	}
	if strings.Contains(out.String(), "WARNING:") {
		t.Fatalf("loopback route unexpectedly classified as a mismatch: %s", out.String())
	}
	select {
	case err := <-received:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("probe left the local connection open")
	}
}

func TestIncompleteDiagnosisKeepsRouteEvidenceWithoutWarnings(t *testing.T) {
	d := routeprobe.RouteDiagnosis{
		Destination:    net.ParseIP("192.0.2.10"),
		RouteInterface: "tun-test",
		RouteSource:    net.ParseIP("198.51.100.2"),
		MTU:            1415,
	}
	var out bytes.Buffer
	if err := writeDiagnosis(&out, d, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"interface=tun-test", "source=198.51.100.2", "UDP socket source: unavailable", "no mismatch classification made"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("partial evidence missing %q: %s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "WARNING:") {
		t.Fatalf("incomplete evidence classified as a mismatch: %s", out.String())
	}
}
