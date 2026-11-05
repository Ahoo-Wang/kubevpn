package localgateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
)

// DNSRelay serves only the configured cluster domain on a shared loopback UDP
// and TCP port. Every upstream exchange uses a private TCP API-forward channel.
type DNSRelay struct {
	Port       int
	cancel     context.CancelCauseFunc
	done       chan struct{}
	err        error
	cleanupErr error
}

// Wait reports listener failure or the parent's cancellation cause.
func (r *DNSRelay) Wait() error {
	<-r.done
	return r.err
}

// Close is idempotent and uses cleanup independent of the canceled run context.
func (r *DNSRelay) Close() error {
	r.cancel(context.Canceled)
	_ = r.Wait()
	if r.cleanupErr != nil {
		return r.cleanupErr
	}
	if errors.Is(r.err, context.Canceled) || errors.Is(r.err, context.DeadlineExceeded) {
		return nil
	}
	return r.err
}

// StartDNSRelay returns only after both loopback listeners are serving. It never
// changes system resolvers or hosts files and never forwards public DNS names.
func StartDNSRelay(parent context.Context, upstreamTCP, clusterDomain string) (*DNSRelay, error) {
	host, port, err := net.SplitHostPort(upstreamTCP)
	ip := net.ParseIP(host)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || ip == nil || !ip.IsLoopback() || portErr != nil || portNumber < 1 || portNumber > 65535 {
		return nil, fmt.Errorf("DNS upstream must be a literal loopback TCP address and port")
	}
	domain := dns.Fqdn(strings.ToLower(strings.TrimSpace(clusterDomain)))
	if _, valid := dns.IsDomainName(domain); !valid || domain == "." || strings.ContainsAny(domain, "\\* \t\r\n") {
		return nil, fmt.Errorf("a valid cluster DNS domain is required")
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	tcp, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("reserve TCP DNS listener: %w", err)
	}
	return startDNSRelay(parent, tcp, upstreamTCP, domain)
}

// startDNSRelay owns tcp even when binding the matching UDP port fails.
func startDNSRelay(parent context.Context, tcp net.Listener, upstream, domain string) (*DNSRelay, error) {
	udp, err := net.ListenPacket("udp4", tcp.Addr().String())
	if err != nil {
		_ = tcp.Close()
		return nil, fmt.Errorf("reserve UDP DNS listener: %w", err)
	}
	ctx, cancel := context.WithCancelCause(parent)
	relay := &DNSRelay{Port: tcp.Addr().(*net.TCPAddr).Port, cancel: cancel, done: make(chan struct{})}
	tcpReady, udpReady := make(chan struct{}), make(chan struct{})
	handler := clusterDNSHandler(ctx, upstream, domain)
	newServer := func() *dns.Server {
		return &dns.Server{
			Handler: handler, ReadTimeout: 3 * time.Second, WriteTimeout: time.Second,
			IdleTimeout: func() time.Duration { return 3 * time.Second },
			UDPSize:     dns.MaxMsgSize,
			// Let the handler issue REFUSED/FORMERR consistently after parsing.
			MsgAcceptFunc: func(dns.Header) dns.MsgAcceptAction { return dns.MsgAccept },
		}
	}
	tcpServer, udpServer := newServer(), newServer()
	tcpServer.Listener, tcpServer.NotifyStartedFunc = tcp, func() { close(tcpReady) }
	udpServer.PacketConn, udpServer.NotifyStartedFunc = udp, func() { close(udpReady) }
	results := make(chan error, 2)
	go func() { results <- tcpServer.ActivateAndServe() }()
	go func() { results <- udpServer.ActivateAndServe() }()
	go func() {
		consumed := 0
		var cause error
		select {
		case <-ctx.Done():
			cause = context.Cause(ctx)
		case err := <-results:
			consumed++
			if err == nil {
				err = fmt.Errorf("DNS listener ended unexpectedly")
			}
			cause = err
		}
		cancel(cause)
		// Closing first also prevents a delayed ActivateAndServe from starting
		// after cancellation; ShutdownContext then interrupts accepted TCP reads.
		_ = tcp.Close()
		_ = udp.Close()
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cleanupCancel()
		var wg sync.WaitGroup
		cleanupErrors := make(chan error, 2)
		for _, server := range []*dns.Server{tcpServer, udpServer} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := server.ShutdownContext(cleanup); err != nil && err.Error() != "dns: server not started" {
					cleanupErrors <- err
				}
			}()
		}
		wg.Wait()
		close(cleanupErrors)
		for err := range cleanupErrors {
			relay.cleanupErr = errors.Join(relay.cleanupErr, err)
		}
		for consumed < 2 {
			select {
			case <-results:
				consumed++
			case <-cleanup.Done():
				relay.cleanupErr = errors.Join(relay.cleanupErr, cleanup.Err())
				consumed = 2
			}
		}
		relay.err = errors.Join(cause, relay.cleanupErr)
		close(relay.done)
	}()
	for tcpReady != nil || udpReady != nil {
		select {
		case <-tcpReady:
			tcpReady = nil
		case <-udpReady:
			udpReady = nil
		case <-relay.done:
			return nil, relay.err
		}
	}
	if err := ctx.Err(); err != nil {
		_ = relay.Close()
		return nil, context.Cause(ctx)
	}
	return relay, nil
}

func clusterDNSHandler(parent context.Context, upstream, domain string) dns.Handler {
	return dns.HandlerFunc(func(writer dns.ResponseWriter, request *dns.Msg) {
		response := new(dns.Msg)
		switch {
		case len(request.Question) != 1:
			response.SetRcode(request, dns.RcodeFormatError)
			response.Question = nil
		case request.Response || request.Opcode != dns.OpcodeQuery:
			response.SetRcode(request, dns.RcodeRefused)
		case !dns.IsSubDomain(domain, dns.CanonicalName(request.Question[0].Name)):
			response.SetRcode(request, dns.RcodeRefused)
		case request.Question[0].Qtype == dns.TypeAXFR || request.Question[0].Qtype == dns.TypeIXFR:
			// Zone transfers require a multi-message protocol, not one exchange.
			response.SetRcode(request, dns.RcodeRefused)
		default:
			ctx, cancel := context.WithTimeout(parent, 3*time.Second)
			defer cancel()
			client := &dns.Client{Net: "tcp", Timeout: 3 * time.Second}
			conn, err := client.DialContext(ctx, upstream)
			if err == nil {
				stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
				response, _, err = client.ExchangeWithConnContext(ctx, request, conn)
				stop()
				_ = conn.Close()
			}
			if err != nil || response == nil {
				response = new(dns.Msg).SetRcode(request, dns.RcodeServerFailure)
			}
		}
		if _, tcp := writer.RemoteAddr().(*net.TCPAddr); !tcp {
			size := dns.MinMsgSize
			if option := request.IsEdns0(); option != nil {
				size = min(int(option.UDPSize()), 65507)
			}
			response.Truncate(size)
		}
		_ = writer.WriteMsg(response)
	})
}
