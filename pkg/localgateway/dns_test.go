package localgateway

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func newTCPDNSFixture(t *testing.T, handler dns.Handler) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	server := &dns.Server{Listener: listener, Handler: handler, NotifyStartedFunc: func() { close(ready) }}
	done := make(chan error, 1)
	go func() { done <- server.ActivateAndServe() }()
	<-ready
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.ShutdownContext(cleanup)
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("upstream fixture did not shut down")
		}
	})
	return listener.Addr().String()
}

func askDNS(t *testing.T, relay *DNSRelay, network string, question *dns.Msg) *dns.Msg {
	t.Helper()
	client := &dns.Client{Net: network, Timeout: 5 * time.Second, UDPSize: 4096}
	response, _, err := client.Exchange(question, net.JoinHostPort("127.0.0.1", strconv.Itoa(relay.Port)))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func questionDNS(name string, qtype uint16) *dns.Msg {
	return new(dns.Msg).SetQuestion(name, qtype)
}

func TestDNSRelayTCPUpstreamAndRecordTypes(t *testing.T) {
	var calls atomic.Int32
	upstream := newTCPDNSFixture(t, dns.HandlerFunc(func(writer dns.ResponseWriter, request *dns.Msg) {
		calls.Add(1)
		if _, tcp := writer.RemoteAddr().(*net.TCPAddr); !tcp {
			t.Error("upstream exchange did not use TCP")
		}
		response := new(dns.Msg).SetReply(request)
		response.RecursionAvailable = true
		name := request.Question[0].Name
		var record dns.RR
		switch request.Question[0].Qtype {
		case dns.TypeA:
			record = &dns.A{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 30}, A: net.ParseIP("192.0.2.10")}
		case dns.TypeAAAA:
			// Valid NODATA must remain a successful, empty response.
		case dns.TypeSRV:
			record = &dns.SRV{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeSRV, Class: dns.ClassINET, Ttl: 30}, Port: 9092, Target: "pod-0.kafka.test.svc.cluster.local."}
		case dns.TypeTXT:
			record = &dns.TXT{Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: 30}, Txt: []string{"fixture=value"}}
		}
		if record != nil {
			response.Answer = []dns.RR{record}
		}
		_ = writer.WriteMsg(response)
	}))
	relay, err := StartDNSRelay(context.Background(), upstream, "CLUSTER.LOCAL")
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	for _, network := range []string{"udp", "tcp"} {
		for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA, dns.TypeSRV, dns.TypeTXT} {
			response := askDNS(t, relay, network, questionDNS("service.test.svc.cluster.local.", qtype))
			if response.Rcode != dns.RcodeSuccess || !response.RecursionAvailable {
				t.Fatalf("upstream flags changed: %+v", response.MsgHdr)
			}
			if qtype == dns.TypeAAAA {
				if len(response.Answer) != 0 {
					t.Fatal("AAAA NODATA changed")
				}
			} else if len(response.Answer) != 1 || response.Answer[0].Header().Rrtype != qtype {
				t.Fatalf("record type %d was not preserved: %v", qtype, response.Answer)
			}
		}
	}
	if calls.Load() != 8 {
		t.Fatalf("unexpected number of TCP upstream queries: %d", calls.Load())
	}
}

func TestDNSRelayRejectsOutsideScopeAndMalformedQueries(t *testing.T) {
	var calls atomic.Int32
	upstream := newTCPDNSFixture(t, dns.HandlerFunc(func(writer dns.ResponseWriter, request *dns.Msg) {
		calls.Add(1)
		_ = writer.WriteMsg(new(dns.Msg).SetReply(request))
	}))
	relay, err := StartDNSRelay(context.Background(), upstream, "cluster.local")
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	for _, name := range []string{"example.com.", "notcluster.local.", "cluster.local.example.com.", "outside\\.cluster.local."} {
		response := askDNS(t, relay, "udp", questionDNS(name, dns.TypeA))
		if response.Rcode != dns.RcodeRefused {
			t.Fatalf("query outside domain was not refused: %s rcode=%d", name, response.Rcode)
		}
	}
	for _, opcode := range []int{dns.OpcodeNotify, dns.OpcodeUpdate} {
		request := questionDNS("service.test.svc.cluster.local.", dns.TypeA)
		request.Opcode = opcode
		if response := askDNS(t, relay, "tcp", request); response.Rcode != dns.RcodeRefused {
			t.Fatalf("non-query opcode %d was not refused: %d", opcode, response.Rcode)
		}
	}
	for _, count := range []int{0, 2} {
		request := questionDNS("service.test.svc.cluster.local.", dns.TypeA)
		if count == 0 {
			request.Question = nil
		} else {
			request.Question = append(request.Question, request.Question[0])
		}
		if response := askDNS(t, relay, "udp", request); response.Rcode != dns.RcodeFormatError {
			t.Fatalf("question count %d was not rejected: %d", count, response.Rcode)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected query leaked upstream: %d", calls.Load())
	}
	// The domain itself and case-insensitive real subdomains are allowed.
	for _, name := range []string{"cluster.local.", "SERVICE.TEST.SVC.CLUSTER.LOCAL."} {
		if response := askDNS(t, relay, "udp", questionDNS(name, dns.TypeA)); response.Rcode != dns.RcodeSuccess {
			t.Fatalf("valid cluster name rejected: %s", name)
		}
	}
}

func TestDNSRelayTruncatesOnlyUDPAccordingToEDNS(t *testing.T) {
	upstream := newTCPDNSFixture(t, dns.HandlerFunc(func(writer dns.ResponseWriter, request *dns.Msg) {
		response := new(dns.Msg).SetReply(request)
		for range 12 {
			response.Answer = append(response.Answer, &dns.TXT{Hdr: dns.RR_Header{Name: request.Question[0].Name, Rrtype: dns.TypeTXT, Class: dns.ClassINET}, Txt: []string{strings.Repeat("x", 200)}})
		}
		_ = writer.WriteMsg(response)
	}))
	relay, err := StartDNSRelay(context.Background(), upstream, "cluster.local")
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	for _, size := range []uint16{0, 128, 1232} {
		request := questionDNS("service.test.svc.cluster.local.", dns.TypeTXT)
		if size != 0 {
			request.SetEdns0(size, false)
		}
		response := askDNS(t, relay, "udp", request)
		limit := max(int(size), dns.MinMsgSize)
		// Unpack does not preserve the wire compression setting.
		response.Compress = true
		if !response.Truncated || response.Len() > limit || len(response.Answer) == 12 {
			t.Fatalf("UDP truncation failed for %d: len=%d records=%d TC=%v", size, response.Len(), len(response.Answer), response.Truncated)
		}
	}
	response := askDNS(t, relay, "tcp", questionDNS("service.test.svc.cluster.local.", dns.TypeTXT))
	if response.Truncated || len(response.Answer) != 12 {
		t.Fatalf("TCP response was truncated: %+v", response)
	}
}

func TestDNSRelayUpstreamFailureReturnsSERVFAIL(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream := listener.Addr().String()
	_ = listener.Close()
	relay, err := StartDNSRelay(context.Background(), upstream, "cluster.local")
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	if response := askDNS(t, relay, "udp", questionDNS("service.test.svc.cluster.local.", dns.TypeA)); response.Rcode != dns.RcodeServerFailure {
		t.Fatalf("upstream failure became rcode %d", response.Rcode)
	}
}

func TestDNSRelayCancellationClosesActiveUpstreamAndListeners(t *testing.T) {
	upstream, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	seen, disconnected := make(chan struct{}), make(chan struct{})
	go func() {
		conn, err := upstream.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		dnsConn := &dns.Conn{Conn: conn}
		if _, err := dnsConn.ReadMsg(); err != nil {
			return
		}
		close(seen)
		_, _ = conn.Read(make([]byte, 1))
		close(disconnected)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	relay, err := StartDNSRelay(ctx, upstream.Addr().String(), "cluster.local")
	if err != nil {
		t.Fatal(err)
	}
	queryDone := make(chan struct{})
	go func() {
		client := &dns.Client{Net: "tcp", Timeout: 2 * time.Second}
		_, _, _ = client.Exchange(questionDNS("service.test.svc.cluster.local.", dns.TypeA), net.JoinHostPort("127.0.0.1", strconv.Itoa(relay.Port)))
		close(queryDone)
	}()
	select {
	case <-seen:
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive query")
	}
	cancel()
	closed := make(chan error, 1)
	go func() { closed <- relay.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("relay waited for query timeout instead of cancellation")
	}
	if err := relay.Close(); err != nil || !errors.Is(relay.Wait(), context.Canceled) {
		t.Fatalf("idempotent close or wait failed: %v", err)
	}
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("upstream TCP request leaked")
	}
	<-queryDone
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(relay.Port))
	tcp, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatalf("TCP port leaked: %v", err)
	}
	defer tcp.Close()
	udp, err := net.ListenPacket("udp4", address)
	if err != nil {
		t.Fatalf("UDP port leaked: %v", err)
	}
	_ = udp.Close()
}

func TestDNSRelayUDPBindFailureReleasesReservedTCP(t *testing.T) {
	tcp, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := tcp.Addr().String()
	udp, err := net.ListenPacket("udp4", address)
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	if relay, err := startDNSRelay(context.Background(), tcp, "127.0.0.1:1", "cluster.local."); err == nil || relay != nil {
		t.Fatalf("partially ready relay returned: relay=%v err=%v", relay, err)
	}
	listener, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatalf("reserved TCP listener leaked: %v", err)
	}
	_ = listener.Close()
}

func TestDNSRelayRejectsInvalidConfiguration(t *testing.T) {
	for _, upstream := range []string{"127.0.0.1", "localhost:53", "192.0.2.10:53", "127.0.0.1:0", "127.0.0.1:65536"} {
		if _, err := StartDNSRelay(context.Background(), upstream, "cluster.local"); err == nil {
			t.Fatalf("accepted upstream %q", upstream)
		}
	}
	for _, domain := range []string{"", ".", "*.local", "bad domain", "bad\\.local"} {
		if _, err := StartDNSRelay(context.Background(), "127.0.0.1:53", domain); err == nil {
			t.Fatalf("accepted cluster domain %q", domain)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := StartDNSRelay(ctx, "127.0.0.1:53", "cluster.local"); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled start: %v", err)
	}
}
