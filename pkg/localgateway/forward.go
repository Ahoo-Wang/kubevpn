package localgateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/httpstream"
	streamspdy "k8s.io/apimachinery/pkg/util/httpstream/spdy"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
)

// Binding identifies one local TCP listener and the pod port it forwards to.
// PodUID is the discovery identity used by the owner to replace stale bindings;
// Kubernetes' port-forward endpoint does not offer an atomic UID precondition.
type Binding struct {
	IP        string
	Port      int
	Namespace string
	Pod       string
	PodPort   int
	PodUID    string
}

// Forwarder opens independent Kubernetes API port-forward sessions. It does not
// add local addresses, change routes or DNS, or modify cluster resources.
type Forwarder struct {
	client    *http.Client
	base      *url.URL
	allowPeer func(net.IP) (bool, error)
}

// NewForwarder preserves the REST config's authentication, TLS and proxy setup.
// HTTP/1.1 is required for the SPDY upgrade. A session's startup timer bounds
// establishment without imposing a deadline on an established stream.
func NewForwarder(config *rest.Config) (*Forwarder, error) {
	if config == nil {
		return nil, fmt.Errorf("REST config is required")
	}
	base, err := url.Parse(config.Host)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") || base.User != nil {
		return nil, fmt.Errorf("API host must be an HTTP or HTTPS URL without embedded credentials")
	}
	copy := rest.CopyConfig(config)
	copy.NextProtos = []string{"http/1.1"}
	copy.Timeout = 0
	client, err := rest.HTTPClientFor(copy)
	if err != nil {
		return nil, fmt.Errorf("create API client: %w", err)
	}
	return &Forwarder{client: client, base: base, allowPeer: localPeer}, nil
}

// Session owns a ready listener and its upgraded API connection. Wait can be
// called by multiple observers. Close cancels the session and waits for shutdown.
type Session struct {
	LocalPort int
	cancel    context.CancelCauseFunc
	done      chan struct{}
	err       error
}

// Wait returns the session's terminal error, or the cancellation cause.
func (s *Session) Wait() error {
	<-s.done
	return s.err
}

// Close is idempotent. Expected cancellation is not a cleanup error.
func (s *Session) Close() error {
	s.cancel(context.Canceled)
	err := s.Wait()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

// Start returns only once the requested listener is ready. A zero local port is
// allowed only on 127.0.0.1 for private control channels. No UDP is forwarded.
func (f *Forwarder) Start(ctx context.Context, binding Binding) (*Session, error) {
	return f.start(ctx, binding, 10*time.Second)
}

func (f *Forwarder) start(parent context.Context, binding Binding, startupTimeout time.Duration) (*Session, error) {
	ip := net.ParseIP(binding.IP)
	if ip == nil || ip.IsUnspecified() || ip.IsMulticast() {
		return nil, fmt.Errorf("a specific unicast listen IP is required")
	}
	if binding.Port < 0 || binding.Port > 65535 || (binding.Port == 0 && binding.IP != "127.0.0.1") || binding.PodPort < 1 || binding.PodPort > 65535 {
		return nil, fmt.Errorf("invalid local or pod port")
	}
	if len(validation.IsDNS1123Label(binding.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(binding.Pod)) != 0 {
		return nil, fmt.Errorf("valid namespace and pod names are required")
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(parent)
	session := &Session{cancel: cancel, done: make(chan struct{})}
	var frontend *tcpFrontend
	if binding.Port != 0 {
		listener, err := net.Listen("tcp", net.JoinHostPort(ip.String(), strconv.Itoa(binding.Port)))
		if err != nil {
			cancel(err)
			return nil, fmt.Errorf("listen on business address: %w", err)
		}
		frontend = &tcpFrontend{listener: listener, allowPeer: f.allowPeer, backend: make(chan string, 1), ready: make(chan struct{}), done: make(chan struct{})}
		go frontend.run(ctx, cancel)
	}
	ready := make(chan struct{})
	endpoint := *f.base
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/api/v1/namespaces/" + binding.Namespace + "/pods/" + binding.Pod + "/portforward"
	endpoint.RawPath = ""
	dialer := &apiStreamDialer{ctx: ctx, client: f.client, endpoint: &endpoint}
	// The API listener is always private. The business listener validates local
	// peers before reaching it, since a lo0 alias alone is not an access boundary.
	forwarder, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"}, []string{"0:" + strconv.Itoa(binding.PodPort)}, ctx.Done(), ready, io.Discard, io.Discard)
	if err != nil {
		cancel(err)
		if frontend != nil {
			<-frontend.done
		}
		return nil, err
	}
	timer := time.AfterFunc(startupTimeout, func() { cancel(context.DeadlineExceeded) })
	defer timer.Stop()
	go func() {
		err := forwarder.ForwardPorts()
		if ctx.Err() != nil {
			err = context.Cause(ctx)
		}
		cancel(err)
		if frontend != nil {
			<-frontend.done
		}
		session.err = err
		close(session.done)
	}()
	select {
	case <-ready:
		if !timer.Stop() {
			cancel(context.DeadlineExceeded)
			_ = session.Close()
			return nil, context.Cause(ctx)
		}
		if ctx.Err() != nil {
			_ = session.Close()
			return nil, context.Cause(ctx)
		}
		ports, err := forwarder.GetPorts()
		if err != nil || len(ports) != 1 || ports[0].Local == 0 {
			_ = session.Close()
			if err != nil {
				return nil, fmt.Errorf("read ready listener port: %w", err)
			}
			return nil, fmt.Errorf("port-forward did not publish one ready listener")
		}
		session.LocalPort = int(ports[0].Local)
		if frontend != nil {
			frontend.backend <- net.JoinHostPort("127.0.0.1", strconv.Itoa(session.LocalPort))
			select {
			case <-frontend.ready:
				if ctx.Err() != nil {
					_ = session.Close()
					return nil, context.Cause(ctx)
				}
			case <-session.done:
				return nil, session.err
			}
			session.LocalPort = binding.Port
		}
		return session, nil
	case <-session.done:
		if session.err == nil {
			return nil, fmt.Errorf("port-forward ended before becoming ready")
		}
		return nil, session.err
	case <-ctx.Done():
		_ = session.Close()
		return nil, context.Cause(ctx)
	}
}

// apiStreamDialer uses the standard HTTP transport so cancellation also closes
// connections stalled in proxy negotiation, TLS or upgrade-response headers.
// An HTTP 101 response body owns the upgraded connection, including buffered
// bytes. The raw connection is retained only for net.Conn address/deadline APIs.
type apiStreamDialer struct {
	ctx      context.Context
	client   *http.Client
	endpoint *url.URL
}

func (d *apiStreamDialer) Dial(protocols ...string) (httpstream.Connection, string, error) {
	var raw net.Conn
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { raw = info.Conn }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(d.ctx, trace), http.MethodPost, d.endpoint.String(), nil)
	if err != nil {
		return nil, "", fmt.Errorf("create API upgrade request: %w", err)
	}
	req.Header.Set(httpstream.HeaderConnection, httpstream.HeaderUpgrade)
	req.Header.Set(httpstream.HeaderUpgrade, streamspdy.HeaderSpdy31)
	for _, protocol := range protocols {
		req.Header.Add(httpstream.HeaderProtocolVersion, protocol)
	}
	response, err := d.client.Do(req)
	if err != nil {
		// URL errors can contain kubeconfig query credentials; retain the cause.
		if urlError, ok := err.(*url.Error); ok {
			err = urlError.Err
		}
		return nil, "", fmt.Errorf("upgrade API connection: %w", err)
	}
	if response.StatusCode != http.StatusSwitchingProtocols || !strings.EqualFold(response.Header.Get(httpstream.HeaderUpgrade), streamspdy.HeaderSpdy31) {
		_ = response.Body.Close()
		return nil, "", fmt.Errorf("API refused SPDY upgrade: %s", response.Status)
	}
	body, ok := response.Body.(io.ReadWriteCloser)
	if !ok || raw == nil {
		_ = response.Body.Close()
		return nil, "", fmt.Errorf("API upgrade transport did not expose a duplex connection")
	}
	conn := &upgradedConn{Conn: raw, stream: body}
	context.AfterFunc(d.ctx, func() { _ = conn.Close() })
	stream, err := streamspdy.NewClientConnectionWithPings(conn, 5*time.Second)
	if err != nil {
		_ = conn.Close()
		return nil, "", fmt.Errorf("create SPDY connection: %w", err)
	}
	return stream, response.Header.Get(httpstream.HeaderProtocolVersion), nil
}

type upgradedConn struct {
	net.Conn
	stream io.ReadWriteCloser
	once   sync.Once
	err    error
}

func (c *upgradedConn) Read(p []byte) (int, error)  { return c.stream.Read(p) }
func (c *upgradedConn) Write(p []byte) (int, error) { return c.stream.Write(p) }
func (c *upgradedConn) Close() error {
	c.once.Do(func() { c.err = c.stream.Close() })
	return c.err
}

// tcpFrontend limits business-IP listeners to clients on this machine. It owns
// every accepted connection so cancellation can drain both relay directions.
type tcpFrontend struct {
	listener  net.Listener
	allowPeer func(net.IP) (bool, error)
	backend   chan string
	ready     chan struct{}
	done      chan struct{}
}

func (f *tcpFrontend) run(ctx context.Context, cancel context.CancelCauseFunc) {
	defer close(f.done)
	var workers sync.WaitGroup
	var mu sync.Mutex
	active := make(map[net.Conn]struct{})
	closeActive := func() {
		mu.Lock()
		defer mu.Unlock()
		for conn := range active {
			_ = conn.Close()
		}
	}
	cleanupDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cleanupDone)
		_ = f.listener.Close()
		closeActive()
	})
	defer func() {
		_ = f.listener.Close()
		closeActive()
		workers.Wait()
		if !stop() {
			<-cleanupDone
		}
	}()
	var backend string
	select {
	case backend = <-f.backend:
	case <-ctx.Done():
		return
	}
	if ctx.Err() != nil {
		return
	}
	close(f.ready)
	track := func(conn net.Conn) {
		mu.Lock()
		active[conn] = struct{}{}
		mu.Unlock()
	}
	release := func(conn net.Conn) {
		_ = conn.Close()
		mu.Lock()
		delete(active, conn)
		mu.Unlock()
	}
	for {
		local, err := f.listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				cancel(fmt.Errorf("accept business connection: %w", err))
			}
			return
		}
		track(local)
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer release(local)
			peer, ok := local.RemoteAddr().(*net.TCPAddr)
			if !ok || ctx.Err() != nil {
				return
			}
			allowed, err := f.allowPeer(peer.IP)
			if err != nil || !allowed || ctx.Err() != nil {
				return
			}
			var dialer net.Dialer
			remote, err := dialer.DialContext(ctx, "tcp4", backend)
			if err != nil {
				return
			}
			track(remote)
			defer release(remote)
			if ctx.Err() != nil {
				return
			}
			var copied sync.WaitGroup
			copyHalf := func(dst, src net.Conn) {
				defer copied.Done()
				if _, err := io.Copy(dst, src); err != nil {
					_ = local.Close()
					_ = remote.Close()
					return
				}
				if half, ok := dst.(interface{ CloseWrite() error }); ok {
					_ = half.CloseWrite()
				}
			}
			copied.Add(2)
			go copyHalf(local, remote)
			go copyHalf(remote, local)
			copied.Wait()
		}()
	}
}

func localPeer(peer net.IP) (bool, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return false, err
	}
	allowed := peer.IsLoopback()
	for _, iface := range interfaces {
		addresses, err := iface.Addrs()
		if err != nil {
			return false, err
		}
		for _, address := range addresses {
			switch value := address.(type) {
			case *net.IPNet:
				allowed = allowed || value.IP.Equal(peer)
			case *net.IPAddr:
				allowed = allowed || value.IP.Equal(peer)
			}
		}
	}
	return allowed, nil
}
