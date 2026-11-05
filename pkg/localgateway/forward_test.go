package localgateway

import (
	"bufio"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	streamspdy "k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
)

type podFixture struct {
	config   *rest.Config
	port     int
	sessions chan httpstream.Connection
	streams  atomic.Int32
}

type streamPair struct {
	data, failure           httpstream.Stream
	dataReady, failureReady <-chan struct{}
}

// This fixture wires a real HTTP/SPDY API upgrade to a real loopback TCP pod.
// It never uses ambient kubeconfig, a live cluster or privileged listeners.
func newPodFixture(t *testing.T, tls bool) *podFixture {
	t.Helper()
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var backendConns sync.Map
	backendDone := make(chan struct{})
	var backendWG sync.WaitGroup
	go func() {
		defer close(backendDone)
		for {
			conn, err := backend.Accept()
			if err != nil {
				return
			}
			backendConns.Store(conn, struct{}{})
			backendWG.Add(1)
			go func() {
				defer backendWG.Done()
				defer backendConns.Delete(conn)
				defer conn.Close()
				_, _ = io.WriteString(conn, "hello-from-pod\n")
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	fixture := &podFixture{port: backend.Addr().(*net.TCPAddr).Port, sessions: make(chan httpstream.Connection, 16)}
	var sessionsMu sync.Mutex
	var sessions []httpstream.Connection
	closing := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.Path != "/api/v1/namespaces/test/pods/pod-0/portforward" || req.Header.Get("Authorization") != "Bearer fixture-token" {
			http.Error(w, "unexpected API request", http.StatusBadRequest)
			return
		}
		if _, err := httpstream.Handshake(req, w, []string{portforward.PortForwardProtocolV1Name}); err != nil {
			return
		}
		var pairsMu sync.Mutex
		pairs := make(map[string]*streamPair)
		sessionClosed := make(chan struct{})
		conn := streamspdy.NewResponseUpgrader().UpgradeResponse(w, req, func(stream httpstream.Stream, replied <-chan struct{}) error {
			if stream.Headers().Get(corev1.PortHeader) != strconv.Itoa(fixture.port) {
				return fmt.Errorf("unexpected remote pod port")
			}
			id := stream.Headers().Get(corev1.PortForwardRequestIDHeader)
			pairsMu.Lock()
			defer pairsMu.Unlock()
			pair := pairs[id]
			if pair == nil {
				pair = &streamPair{}
				pairs[id] = pair
			}
			switch stream.Headers().Get(corev1.StreamType) {
			case corev1.StreamTypeError:
				pair.failure, pair.failureReady = stream, replied
			case corev1.StreamTypeData:
				fixture.streams.Add(1)
				pair.data, pair.dataReady = stream, replied
			default:
				return fmt.Errorf("unexpected stream type")
			}
			if pair.data != nil && pair.failure != nil {
				delete(pairs, id)
				go relayTestPod(pair, backend.Addr().String(), sessionClosed)
			}
			return nil
		})
		if conn == nil {
			return
		}
		sessionsMu.Lock()
		if closing {
			_ = conn.Close()
		} else {
			sessions = append(sessions, conn)
			fixture.sessions <- conn
		}
		sessionsMu.Unlock()
		<-conn.CloseChan()
		close(sessionClosed)
		_ = conn.Close()
	})
	var server *httptest.Server
	if tls {
		server = httptest.NewTLSServer(handler)
	} else {
		server = httptest.NewServer(handler)
	}
	fixture.config = &rest.Config{Host: server.URL, BearerToken: "fixture-token"}
	if tls {
		fixture.config.CAData = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	}
	t.Cleanup(func() {
		sessionsMu.Lock()
		closing = true
		for _, session := range sessions {
			_ = session.Close()
		}
		sessionsMu.Unlock()
		server.Close()
		_ = backend.Close()
		<-backendDone
		backendConns.Range(func(conn, _ any) bool { _ = conn.(net.Conn).Close(); return true })
		backendWG.Wait()
	})
	return fixture
}

func relayTestPod(pair *streamPair, address string, sessionClosed <-chan struct{}) {
	<-pair.dataReady
	<-pair.failureReady
	defer pair.data.Close()
	defer pair.failure.Close()
	backend, err := net.Dial("tcp4", address)
	if err != nil {
		_, _ = io.WriteString(pair.failure, err.Error())
		return
	}
	defer backend.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-sessionClosed:
			_ = backend.Close()
		case <-done:
		}
	}()
	copied := make(chan struct{})
	go func() {
		_, _ = io.Copy(backend, pair.data)
		_ = backend.(*net.TCPConn).CloseWrite()
		close(copied)
	}()
	_, _ = io.Copy(pair.data, backend)
	_ = backend.Close()
	_ = pair.data.Close()
	<-copied
}

func fixtureBinding(pod *podFixture) Binding {
	return Binding{IP: "127.0.0.1", Namespace: "test", Pod: "pod-0", PodPort: pod.port, PodUID: "fixture-pod-uid"}
}

func assertPodEcho(t *testing.T, session *Session, message string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(session.LocalPort)), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	greeting, err := reader.ReadString('\n')
	if err != nil || greeting != "hello-from-pod\n" {
		t.Fatalf("no real pod greeting: %q err=%v", greeting, err)
	}
	if _, err := io.WriteString(conn, message+"\n"); err != nil {
		t.Fatal(err)
	}
	if reply, err := reader.ReadString('\n'); err != nil || reply != message+"\n" {
		t.Fatalf("wrong pod reply: %q err=%v", reply, err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestForwarderReadyDataAndCancellation(t *testing.T) {
	pod := newPodFixture(t, false)
	forwarder, err := NewForwarder(pod.config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session, err := forwarder.start(ctx, fixtureBinding(pod), 150*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if session.LocalPort == 0 {
		t.Fatal("returned before publishing a ready port")
	}
	// Initialization timeout must stop once ready; it must not expire active streams.
	time.Sleep(200 * time.Millisecond)
	conn := assertPodEcho(t, session, "opaque-client-data")
	defer conn.Close()
	cancel()
	waitForSession(t, session, context.Canceled)
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("active client stayed open after cancellation")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("active client was not closed: %v", err)
	}
	for range 2 {
		if err := session.Close(); err != nil {
			t.Fatalf("close should be idempotent: %v", err)
		}
	}
}

func waitForSession(t *testing.T, session *Session, want error) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- session.Wait() }()
	select {
	case err := <-result:
		if !errors.Is(err, want) {
			t.Fatalf("session ended with %v, want %v", err, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session did not settle")
	}
}

func TestForwarderUpgradeCancellationAndStartupTimeout(t *testing.T) {
	for _, useTimeout := range []bool{false, true} {
		t.Run(strconv.FormatBool(useTimeout), func(t *testing.T) {
			requestSeen := make(chan struct{})
			requestCanceled := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				close(requestSeen)
				<-req.Context().Done()
				close(requestCanceled)
			}))
			defer server.Close()
			forwarder, err := NewForwarder(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			timeout := 5 * time.Second
			if useTimeout {
				timeout = 100 * time.Millisecond
			}
			go func() {
				_, err := forwarder.start(ctx, Binding{IP: "127.0.0.1", Namespace: "test", Pod: "pod-0", PodPort: 80}, timeout)
				result <- err
			}()
			select {
			case <-requestSeen:
			case <-time.After(time.Second):
				t.Fatal("upgrade request did not arrive")
			}
			want := error(context.DeadlineExceeded)
			if !useTimeout {
				cancel()
				want = context.Canceled
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) {
					t.Fatalf("wrong startup termination: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("startup did not honor cancellation")
			}
			select {
			case <-requestCanceled:
			case <-time.After(time.Second):
				t.Fatal("API connection remained open after startup cancellation")
			}
		})
	}
}

func TestForwarderOccupiedPortFailsWithoutTakingListener(t *testing.T) {
	pod := newPodFixture(t, false)
	forwarder, err := NewForwarder(pod.config)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	binding := fixtureBinding(pod)
	binding.Port = listener.Addr().(*net.TCPAddr).Port
	if session, err := forwarder.Start(context.Background(), binding); err == nil || session != nil {
		t.Fatalf("occupied port unexpectedly started: session=%v err=%v", session, err)
	}
	conn, err := net.DialTimeout("tcp4", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("existing listener was damaged: %v", err)
	}
	_ = conn.Close()
}

func TestForwarderTLSAuthenticationAndConcurrentSessions(t *testing.T) {
	pod := newPodFixture(t, true)
	pod.config.Timeout = time.Millisecond
	forwarder, err := NewForwarder(pod.config)
	if err != nil {
		t.Fatal(err)
	}
	if pod.config.Timeout != time.Millisecond || len(pod.config.NextProtos) != 0 {
		t.Fatal("constructor changed caller's REST config")
	}
	type result struct {
		session *Session
		err     error
	}
	results := make(chan result, 3)
	for range 3 {
		go func() {
			session, err := forwarder.Start(context.Background(), fixtureBinding(pod))
			results <- result{session, err}
		}()
	}
	for i := range 3 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		conn := assertPodEcho(t, r.session, fmt.Sprintf("session-%d", i))
		_ = conn.Close()
		if err := r.session.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestForwarderReportsRemoteSessionLoss(t *testing.T) {
	pod := newPodFixture(t, false)
	forwarder, err := NewForwarder(pod.config)
	if err != nil {
		t.Fatal(err)
	}
	session, err := forwarder.Start(context.Background(), fixtureBinding(pod))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	remote := <-pod.sessions
	_ = remote.Close()
	waitForSession(t, session, portforward.ErrLostConnectionToPod)
}

func TestForwarderRejectsInvalidBindingsBeforeAPI(t *testing.T) {
	for _, config := range []*rest.Config{nil, {Host: ""}, {Host: "://invalid"}, {Host: "file:///tmp/api"}, {Host: "https://user:password@example.invalid"}} {
		if _, err := NewForwarder(config); err == nil {
			t.Fatalf("accepted invalid REST config %v", config)
		}
	}
	forwarder, err := NewForwarder(&rest.Config{Host: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	valid := Binding{IP: "127.0.0.1", Namespace: "test", Pod: "pod-0", PodPort: 80}
	for _, change := range []func(*Binding){
		func(b *Binding) { b.IP = "0.0.0.0" },
		func(b *Binding) { b.IP = "invalid" },
		func(b *Binding) { b.IP = "224.0.0.1" },
		func(b *Binding) { b.IP = "127.0.0.2" },
		func(b *Binding) { b.Port = -1 },
		func(b *Binding) { b.Port = 65536 },
		func(b *Binding) { b.PodPort = 0 },
		func(b *Binding) { b.Namespace = "../escape" },
		func(b *Binding) { b.Pod = "../escape" },
	} {
		binding := valid
		change(&binding)
		if _, err := forwarder.Start(context.Background(), binding); err == nil || strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("invalid binding reached API: %+v err=%v", binding, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := forwarder.Start(ctx, valid); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled startup reached API: %v", err)
	}
}

func TestForwarderPreservesConfiguredHTTPProxy(t *testing.T) {
	pod := newPodFixture(t, false)
	apiURL, err := url.Parse(pod.config.Host)
	if err != nil {
		t.Fatal(err)
	}
	reverse := httputil.NewSingleHostReverseProxy(apiURL)
	seen := make(chan struct{}, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		select {
		case seen <- struct{}{}:
		default:
		}
		reverse.ServeHTTP(w, request)
	}))
	defer proxy.Close()
	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	config := rest.CopyConfig(pod.config)
	config.Host = "http://unresolvable-api.invalid"
	config.Proxy = func(*http.Request) (*url.URL, error) { return proxyURL, nil }
	forwarder, err := NewForwarder(config)
	if err != nil {
		t.Fatal(err)
	}
	session, err := forwarder.Start(context.Background(), fixtureBinding(pod))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	select {
	case <-seen:
	default:
		t.Fatal("API proxy was bypassed")
	}
	conn := assertPodEcho(t, session, "through-api-proxy")
	_ = conn.Close()
}

func businessBinding(t *testing.T, pod *podFixture) Binding {
	t.Helper()
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	binding := fixtureBinding(pod)
	binding.Port = reservation.Addr().(*net.TCPAddr).Port
	_ = reservation.Close()
	return binding
}

func TestForwarderBusinessFrontendDataHalfCloseAndCancellation(t *testing.T) {
	pod := newPodFixture(t, false)
	forwarder, err := NewForwarder(pod.config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	binding := businessBinding(t, pod)
	session, err := forwarder.Start(ctx, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if session.LocalPort != binding.Port {
		t.Fatalf("returned private control port instead of business port: %d", session.LocalPort)
	}
	first := assertPodEcho(t, session, "through-checked-frontend")
	defer first.Close()
	if err := first.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := first.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Read(make([]byte, 1)); err == nil {
		t.Fatal("half-close was not relayed")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatalf("half-close did not converge: %v", err)
	}
	var active []net.Conn
	for i := range 3 {
		conn := assertPodEcho(t, session, fmt.Sprintf("active-%d", i))
		active = append(active, conn)
		defer conn.Close()
	}
	cancel()
	waitForSession(t, session, context.Canceled)
	for _, conn := range active {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := conn.Read(make([]byte, 1)); err == nil {
			t.Fatal("active frontend client leaked")
		} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			t.Fatalf("active frontend client was not closed: %v", err)
		}
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort(binding.IP, strconv.Itoa(binding.Port)))
	if err != nil {
		t.Fatalf("business listener leaked: %v", err)
	}
	_ = listener.Close()
}

func TestForwarderBusinessFrontendRejectsPeersBeforeOpeningStream(t *testing.T) {
	for _, snapshotFailure := range []bool{false, true} {
		t.Run(strconv.FormatBool(snapshotFailure), func(t *testing.T) {
			pod := newPodFixture(t, false)
			forwarder, err := NewForwarder(pod.config)
			if err != nil {
				t.Fatal(err)
			}
			checked := make(chan struct{}, 1)
			forwarder.allowPeer = func(net.IP) (bool, error) {
				checked <- struct{}{}
				if snapshotFailure {
					return true, errors.New("interface snapshot failed")
				}
				return false, nil
			}
			session, err := forwarder.Start(context.Background(), businessBinding(t, pod))
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			conn, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(session.LocalPort)), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := conn.Read(make([]byte, 1)); err == nil {
				t.Fatal("rejected peer received pod data")
			} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatalf("rejected peer was left open: %v", err)
			}
			select {
			case <-checked:
			default:
				t.Fatal("peer check was skipped")
			}
			if pod.streams.Load() != 0 {
				t.Fatalf("rejected peer reached private API forwarder: streams=%d", pod.streams.Load())
			}
		})
	}
}
