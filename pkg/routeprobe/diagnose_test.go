package routeprobe

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func fixtureDeps(routeSource, socketSource string, addresses map[string][]net.IP) diagnosisDeps {
	return diagnosisDeps{
		route: func(net.IP) (*net.Interface, net.IP, net.IP, error) {
			return &net.Interface{Name: "tun-test", MTU: 1415}, nil, net.ParseIP(routeSource), nil
		},
		source: func(context.Context, net.IP) (net.IP, error) {
			return net.ParseIP(socketSource), nil
		},
		interfaces: func() (map[string][]net.IP, error) { return addresses, nil },
	}
}

func TestDiagnoseRouteSourceOwnership(t *testing.T) {
	for _, tt := range []struct {
		name                  string
		destination           string
		routeSource           string
		socketSource          string
		addresses             map[string][]net.IP
		wantRouteAssigned     bool
		wantSocketOnInterface bool
		wantSocketInterfaces  []string
	}{
		{
			name: "IPv4 follows route", destination: "192.0.2.10", routeSource: "198.51.100.2", socketSource: "198.51.100.2",
			addresses:         map[string][]net.IP{"tun-test": {net.ParseIP("198.51.100.2")}},
			wantRouteAssigned: true, wantSocketOnInterface: true, wantSocketInterfaces: []string{"tun-test"},
		},
		{
			name: "stale route source", destination: "192.0.2.10", routeSource: "198.51.100.1", socketSource: "198.51.100.2",
			addresses:         map[string][]net.IP{"tun-test": {net.ParseIP("198.51.100.2")}},
			wantRouteAssigned: false, wantSocketOnInterface: true, wantSocketInterfaces: []string{"tun-test"},
		},
		{
			name: "socket source belongs to another interface", destination: "192.0.2.10", routeSource: "198.51.100.2", socketSource: "203.0.113.2",
			addresses:         map[string][]net.IP{"tun-test": {net.ParseIP("198.51.100.2")}, "physical-test": {net.ParseIP("203.0.113.2")}},
			wantRouteAssigned: true, wantSocketOnInterface: false, wantSocketInterfaces: []string{"physical-test"},
		},
		{
			name: "IPv6 link-local route and global socket source", destination: "2001:db8:1::10", routeSource: "fe80::1", socketSource: "2001:db8:2::2",
			addresses:         map[string][]net.IP{"tun-test": {net.ParseIP("fe80::1"), net.ParseIP("2001:db8:2::2")}},
			wantRouteAssigned: true, wantSocketOnInterface: true, wantSocketInterfaces: []string{"tun-test"},
		},
		{
			name: "IPv6 stale route source", destination: "2001:db8:1::10", routeSource: "fe80::dead", socketSource: "2001:db8:2::2",
			addresses:         map[string][]net.IP{"tun-test": {net.ParseIP("fe80::1"), net.ParseIP("2001:db8:2::2")}},
			wantRouteAssigned: false, wantSocketOnInterface: true, wantSocketInterfaces: []string{"tun-test"},
		},
		{
			name: "route source absent", destination: "192.0.2.10", routeSource: "", socketSource: "198.51.100.2",
			addresses:         map[string][]net.IP{"tun-test": {net.ParseIP("198.51.100.2")}},
			wantRouteAssigned: false, wantSocketOnInterface: true, wantSocketInterfaces: []string{"tun-test"},
		},
		{
			name: "source has multiple owners", destination: "192.0.2.10", routeSource: "198.51.100.2", socketSource: "198.51.100.2",
			addresses:         map[string][]net.IP{"tun-test": {net.ParseIP("198.51.100.2")}, "alias-test": {net.ParseIP("198.51.100.2")}},
			wantRouteAssigned: true, wantSocketOnInterface: true, wantSocketInterfaces: []string{"alias-test", "tun-test"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dst := net.ParseIP(tt.destination)
			got, err := diagnoseRoute(context.Background(), dst, fixtureDeps(tt.routeSource, tt.socketSource, tt.addresses))
			if err != nil {
				t.Fatal(err)
			}
			if !got.Destination.Equal(dst) || got.RouteInterface != "tun-test" || got.MTU != 1415 {
				t.Fatalf("route details lost: %+v", got)
			}
			if got.RouteSourceAssigned != tt.wantRouteAssigned || got.SocketSourceOnRouteInterface != tt.wantSocketOnInterface {
				t.Fatalf("source ownership: route=%v socket=%v, want route=%v socket=%v", got.RouteSourceAssigned, got.SocketSourceOnRouteInterface, tt.wantRouteAssigned, tt.wantSocketOnInterface)
			}
			if !reflect.DeepEqual(got.SocketInterfaces, tt.wantSocketInterfaces) {
				t.Fatalf("socket interfaces: got %v want %v", got.SocketInterfaces, tt.wantSocketInterfaces)
			}
		})
	}
}

func TestDiagnoseRouteErrorsKeepPartialResults(t *testing.T) {
	failure := errors.New("observation failed")
	for _, stage := range []string{"route", "source", "interfaces"} {
		t.Run(stage, func(t *testing.T) {
			deps := fixtureDeps("198.51.100.2", "198.51.100.2", map[string][]net.IP{"tun-test": {net.ParseIP("198.51.100.2")}})
			switch stage {
			case "route":
				deps.route = func(net.IP) (*net.Interface, net.IP, net.IP, error) { return nil, nil, nil, failure }
			case "source":
				deps.source = func(context.Context, net.IP) (net.IP, error) { return nil, failure }
			case "interfaces":
				deps.interfaces = func() (map[string][]net.IP, error) { return nil, failure }
			}
			got, err := diagnoseRoute(context.Background(), net.ParseIP("192.0.2.10"), deps)
			if !errors.Is(err, failure) {
				t.Fatalf("cause not preserved: %v", err)
			}
			if got.Destination == nil || (stage != "route" && got.RouteInterface != "tun-test") || (stage == "interfaces" && got.SocketSource == nil) {
				t.Fatalf("partial result lost: %+v", got)
			}
		})
	}
}

func TestDiagnoseRouteRejectsInvalidDestinationAndCancellation(t *testing.T) {
	// Validation and an already canceled context must not observe host networking.
	for _, ip := range []net.IP{nil, {1, 2, 3}, net.IPv4zero, net.IPv6zero} {
		if _, err := diagnoseRoute(context.Background(), ip, diagnosisDeps{}); err == nil {
			t.Fatalf("accepted invalid destination %v", ip)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := DiagnoseRoute(ctx, net.ParseIP("192.0.2.10")); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled probe: %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	deps := fixtureDeps("198.51.100.2", "198.51.100.2", nil)
	deps.route = func(net.IP) (*net.Interface, net.IP, net.IP, error) {
		cancel()
		return &net.Interface{Name: "tun-test"}, nil, net.ParseIP("198.51.100.2"), nil
	}
	deps.source = func(context.Context, net.IP) (net.IP, error) { t.Fatal("probed after cancellation"); return nil, nil }
	if _, err := diagnoseRoute(ctx, net.ParseIP("192.0.2.10"), deps); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation between observations: %v", err)
	}
}

func TestDiagnoseRouteRejectsIncompleteObservations(t *testing.T) {
	for _, stage := range []string{"missing interface", "missing source", "disappeared interface"} {
		t.Run(stage, func(t *testing.T) {
			deps := fixtureDeps("198.51.100.2", "198.51.100.2", map[string][]net.IP{})
			switch stage {
			case "missing interface":
				deps.route = func(net.IP) (*net.Interface, net.IP, net.IP, error) { return nil, nil, nil, nil }
			case "missing source":
				deps.source = func(context.Context, net.IP) (net.IP, error) { return nil, nil }
			}
			if _, err := diagnoseRoute(context.Background(), net.ParseIP("192.0.2.10"), deps); err == nil {
				t.Fatal("accepted an incomplete observation")
			}
		})
	}
}

type trackedConn struct {
	net.Conn
	closed   bool
	local    net.Addr
	closeErr error
}

func (c *trackedConn) Close() error {
	c.closed = true
	if c.Conn != nil {
		if err := c.Conn.Close(); err != nil {
			return err
		}
	}
	return c.closeErr
}

func (c *trackedConn) LocalAddr() net.Addr {
	if c.local != nil {
		return c.local
	}
	return c.Conn.LocalAddr()
}

func TestDiagnoseRouteLoopbackProbeSendsNothingAndCloses(t *testing.T) {
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := strconv.Itoa(listener.LocalAddr().(*net.UDPAddr).Port)
	var connection *trackedConn
	deps := fixtureDeps("127.0.0.1", "127.0.0.1", map[string][]net.IP{"tun-test": {net.ParseIP("127.0.0.1")}})
	deps.source = func(ctx context.Context, dst net.IP) (net.IP, error) {
		var dialer net.Dialer
		return probeSocketSource(ctx, dst, port, func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			connection = &trackedConn{Conn: conn}
			return connection, nil
		})
	}
	got, err := diagnoseRoute(context.Background(), net.ParseIP("127.0.0.1"), deps)
	if err != nil {
		t.Fatal(err)
	}
	if !got.SocketSource.Equal(net.ParseIP("127.0.0.1")) || !got.SocketSourceOnRouteInterface {
		t.Fatalf("wrong loopback source: %+v", got)
	}
	if connection == nil || !connection.closed {
		t.Fatal("probe socket was not closed")
	}
	if err := listener.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if n, _, err := listener.ReadFromUDP(make([]byte, 1)); err == nil || n != 0 {
		t.Fatalf("probe unexpectedly sent a datagram: n=%d err=%v", n, err)
	} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("read failed for a reason other than no datagrams: %v", err)
	}
}

func TestProbeSocketSourceClosesOnAddressFailureAndCancellation(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(strconv.FormatBool(canceled), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			conn := &trackedConn{local: &net.TCPAddr{IP: net.ParseIP("127.0.0.1")}}
			_, err := probeSocketSource(ctx, net.ParseIP("127.0.0.1"), "9", func(context.Context, string, string) (net.Conn, error) {
				if canceled {
					cancel()
				}
				return conn, nil
			})
			if err == nil || (canceled && !errors.Is(err, context.Canceled)) || !conn.closed {
				t.Fatalf("probe failure not propagated/closed: err=%v closed=%v", err, conn.closed)
			}
		})
	}
}

func TestProbeSocketSourcePreservesDialAndCloseErrors(t *testing.T) {
	failure := errors.New("socket failure")
	if _, err := probeSocketSource(context.Background(), net.ParseIP("127.0.0.1"), "9", func(context.Context, string, string) (net.Conn, error) { return nil, failure }); !errors.Is(err, failure) {
		t.Fatalf("dial cause not preserved: %v", err)
	}
	conn := &trackedConn{local: &net.UDPAddr{IP: net.ParseIP("127.0.0.1")}, closeErr: failure}
	if _, err := probeSocketSource(context.Background(), net.ParseIP("127.0.0.1"), "9", func(context.Context, string, string) (net.Conn, error) { return conn, nil }); !errors.Is(err, failure) || !conn.closed {
		t.Fatalf("close cause not preserved: err=%v closed=%v", err, conn.closed)
	}
}
