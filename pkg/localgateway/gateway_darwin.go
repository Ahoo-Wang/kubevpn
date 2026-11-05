//go:build darwin

package localgateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	miekgdns "github.com/miekg/dns"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// GatewayConfig selects explicit client Services in one cluster. Shadowrocket,
// existing kubevpn daemons and Kubernetes resources are not modified.
type GatewayConfig struct {
	Services      []ServiceRef
	DNSService    ServiceRef
	ClusterDomain string
	StateDir      string
	Refresh       time.Duration
}

type activeBinding struct {
	binding Binding
	session *Session
}
type sessionEvent struct {
	key     string
	session *Session
}

func bindingKey(b Binding) string { return net.JoinHostPort(b.IP, strconv.Itoa(b.Port)) }

// RunGateway owns a foreground, recoverable IPv4 TCP gateway. DNS uses a private
// API port-forward to CoreDNS, not the host's original TUN/DNS source selection.
func RunGateway(parent context.Context, config *rest.Config, c GatewayConfig, out io.Writer) (result error) {
	if err := parent.Err(); err != nil {
		return err
	}
	if config == nil {
		return errors.New("REST config is required")
	}
	if os.Geteuid() != 0 {
		return errors.New("gateway activation requires administrator authentication")
	}
	if c.ClusterDomain == "" {
		c.ClusterDomain = "cluster.local"
	}
	if c.Refresh <= 0 {
		c.Refresh = 5 * time.Second
	}
	if c.DNSService.Name == "" {
		c.DNSService = ServiceRef{Namespace: "kube-system", Name: "kube-dns"}
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	forwarder, err := NewForwarder(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	state, err := NewSystemState(c.StateDir)
	if err != nil {
		return err
	}
	defer state.Close()
	aliases := NewAliasManager()
	confirmed := map[string]bool{}
	syncAliases := func() error { return state.Aliases(confirmedAliasIPs(aliases.OwnedIPs(), confirmed)) }
	running := map[string]activeBinding{}
	var dnsSession *Session
	var relay *DNSRelay
	defer func() {
		// Restore name resolution before withdrawing local addresses. Use a fresh
		// cleanup budget even when Ctrl-C canceled the running context.
		restoreErr := state.Restore()
		cancel()
		var closeErrs []error
		for _, item := range running {
			_ = item.session.Close()
		}
		if relay != nil {
			if err := relay.Close(); err != nil {
				closeErrs = append(closeErrs, err)
			}
		}
		if dnsSession != nil {
			_ = dnsSession.Close()
		}
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 15*time.Second)
		aliasErr := aliases.Close(cleanCtx)
		cleanCancel()
		journalErr := syncAliases()
		pendingErr := state.PruneAbsentPending()
		_, pending := state.RecordedAliases()
		if len(pending) > 0 {
			pendingErr = errors.Join(pendingErr, fmt.Errorf("unconfirmed addresses remain in journal: %v; inspect before manual removal", pending))
		}
		cleanupErr := errors.Join(restoreErr, errors.Join(closeErrs...), aliasErr, journalErr, pendingErr)
		if cleanupErr == nil {
			cleanupErr = state.Finish()
		}
		if errors.Is(result, context.Canceled) && parent.Err() != nil {
			result = nil
		}
		result = errors.Join(result, cleanupErr)
	}()

	planCtx, planCancel := context.WithTimeout(ctx, 15*time.Second)
	plan, err := BuildPlan(planCtx, client, c.Services, c.ClusterDomain)
	planCancel()
	if err != nil {
		return err
	}
	apiIPs, err := apiServerIPs(ctx, config.Host)
	if err != nil {
		return err
	}
	if err = checkControlIPs(plan, apiIPs); err != nil {
		return err
	}

	dnsBinding, err := findDNSBinding(ctx, client, c)
	if err != nil {
		return err
	}
	dnsTarget := dnsBinding
	dnsSession, err = forwarder.Start(ctx, dnsBinding)
	if err != nil {
		return fmt.Errorf("start private DNS API transport: %w", err)
	}
	relay, err = StartDNSRelay(ctx, net.JoinHostPort("127.0.0.1", strconv.Itoa(dnsSession.LocalPort)), c.ClusterDomain)
	if err != nil {
		return err
	}
	if err = verifyDNS(ctx, relay.Port, c.ClusterDomain); err != nil {
		return err
	}
	events := make(chan sessionEvent, 128)
	watch := func(key string, s *Session) {
		go func() {
			_ = s.Wait()
			select {
			case events <- sessionEvent{key: key, session: s}:
			case <-ctx.Done():
			}
		}()
	}
	watch("dns", dnsSession)
	dnsPrivatePort := dnsSession.LocalPort
	restartDNS := func(next Binding) error {
		if dnsSession != nil {
			_ = dnsSession.Close()
		}
		next.Port = dnsPrivatePort
		session, err := forwarder.Start(ctx, next)
		if err != nil {
			dnsSession = nil
			return fmt.Errorf("reconnect DNS API transport: %w", err)
		}
		dnsSession = session
		next.Port = 0
		dnsTarget = next
		watch("dns", session)
		return verifyDNS(ctx, relay.Port, c.ClusterDomain)
	}
	refreshDNS := func() error {
		next, err := findDNSBinding(ctx, client, c)
		if err != nil {
			return err
		}
		if dnsSession == nil || next != dnsTarget {
			return restartDNS(next)
		}
		if err := verifyDNS(ctx, relay.Port, c.ClusterDomain); err != nil {
			return restartDNS(next)
		}
		return nil
	}

	reconcile := func(plan Plan) error {
		if err := checkControlIPs(plan, apiIPs); err != nil {
			return err
		}
		desired := map[string]Binding{}
		for _, b := range plan.Bindings {
			desired[bindingKey(b)] = b
		}
		// Withdraw changed pod identities before reusing the same listener. Sessions
		// unaffected by a topology change remain established.
		keys := make([]string, 0, len(running))
		for key := range running {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			item := running[key]
			if wanted, ok := desired[key]; ok && wanted == item.binding {
				continue
			}
			delete(running, key)
			if err := item.session.Close(); err != nil {
				fmt.Fprintf(out, "Replacing closed transport %s: %v\n", key, err)
			}
			if err := aliases.Release(ctx, item.binding.IP); err != nil {
				return err
			}
			if err := syncAliases(); err != nil {
				return err
			}
		}
		if err := restartClosedBindings(ctx, forwarder, plan.Bindings, running, watch, out); err != nil {
			return err
		}
		for _, b := range plan.Bindings {
			key := bindingKey(b)
			if _, ok := running[key]; ok {
				continue
			}
			owned := false
			for _, ip := range aliases.OwnedIPs() {
				if ip == b.IP {
					owned = true
					break
				}
			}
			if !owned {
				if err := ensureAddressAbsent(b.IP); err != nil {
					return err
				}
				if err := state.PendingIP(b.IP); err != nil {
					return err
				}
			}
			if err := aliases.Acquire(ctx, b.IP); err != nil {
				return err
			}
			confirmed[b.IP] = true
			if err := syncAliases(); err != nil {
				return err
			}
			session, err := forwarder.Start(ctx, b)
			if err != nil {
				return fmt.Errorf("start %s -> %s/%s:%d: %w", key, b.Namespace, b.Pod, b.PodPort, err)
			}
			running[key] = activeBinding{binding: b, session: session}
			watch(key, session)
		}
		return state.Configure(plan.Hostnames, relay.Port, plan.Hostnames)
	}
	if err = reconcile(plan); err != nil {
		return err
	}
	fmt.Fprintf(out, "READY: %d TCP mappings, DNS 127.0.0.1:%d, recovery state %s\n", len(running), relay.Port, c.StateDir)
	fmt.Fprintln(out, "Shadowrocket and existing kubevpn connections were not changed. Ctrl-C restores this session's local changes.")
	relayDone := make(chan error, 1)
	go func() { relayDone <- relay.Wait() }()
	ticker := time.NewTicker(c.Refresh)
	defer ticker.Stop()
	failures := 0
	refresh := func() error {
		planCtx, stop := context.WithTimeout(ctx, 15*time.Second)
		next, err := BuildPlan(planCtx, client, c.Services, c.ClusterDomain)
		stop()
		if err == nil {
			err = refreshDNS()
		}
		if err != nil {
			failures++
			fmt.Fprintf(out, "DEGRADED: topology refresh failed (%d/3): %v\n", failures, err)
			if failures >= 3 {
				return fmt.Errorf("topology unavailable; withdrawing gateway: %w", err)
			}
			return nil
		}
		failures = 0
		return reconcile(next)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-relayDone:
			return fmt.Errorf("DNS relay stopped: %w", err)
		case event := <-events:
			if event.key == "dns" {
				if event.session != dnsSession {
					continue
				}
				if err := refresh(); err != nil {
					return err
				}
				continue
			}
			item, ok := running[event.key]
			if !ok || item.session != event.session {
				continue
			}
			if err := refresh(); err != nil {
				return err
			}
		case <-ticker.C:
			if err := refresh(); err != nil {
				return err
			}
		}
	}
}

// Rebuild every ended session from one topology snapshot without releasing the
// alias lease of a binding that is still desired. Queued old events become stale.
func restartClosedBindings(ctx context.Context, forwarder *Forwarder, desired []Binding, running map[string]activeBinding, watch func(string, *Session), out io.Writer) error {
	for _, binding := range desired {
		key := bindingKey(binding)
		item, ok := running[key]
		if !ok || item.binding != binding {
			continue
		}
		select {
		case <-item.session.done:
		default:
			continue
		}
		fmt.Fprintf(out, "Reconnecting %s after API transport closed: %v\n", key, item.session.Wait())
		session, err := forwarder.Start(ctx, binding)
		if err != nil {
			return fmt.Errorf("restart %s -> %s/%s:%d: %w", key, binding.Namespace, binding.Pod, binding.PodPort, err)
		}
		running[key] = activeBinding{binding: binding, session: session}
		watch(key, session)
	}
	return nil
}

func findDNSBinding(ctx context.Context, client kubernetes.Interface, c GatewayConfig) (Binding, error) {
	child, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	plan, err := BuildPlan(child, client, []ServiceRef{c.DNSService}, c.ClusterDomain)
	if err != nil {
		return Binding{}, fmt.Errorf("discover cluster DNS: %w", err)
	}
	for _, b := range plan.Bindings {
		if b.Port == 53 && b.PodPort == 53 {
			b.IP = "127.0.0.1"
			b.Port = 0
			return b, nil
		}
	}
	return Binding{}, errors.New("cluster DNS has no ready TCP port 53 endpoint")
}
func verifyDNS(ctx context.Context, port int, domain string) error {
	msg := new(miekgdns.Msg)
	msg.SetQuestion("kubernetes.default.svc."+domain+".", miekgdns.TypeA)
	client := &miekgdns.Client{Net: "udp", Timeout: 3 * time.Second}
	reply, _, err := client.ExchangeContext(ctx, msg, net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("verify DNS API data path: %w", err)
	}
	if reply == nil || reply.Rcode != miekgdns.RcodeSuccess || len(reply.Answer) == 0 {
		return errors.New("cluster DNS API data path returned no Kubernetes service answer")
	}
	return nil
}
func ensureAddressAbsent(ip string) error {
	wanted := net.ParseIP(ip)
	interfaces, err := net.Interfaces()
	if err != nil {
		return err
	}
	for _, iface := range interfaces {
		addresses, err := iface.Addrs()
		if err != nil {
			return err
		}
		for _, a := range addresses {
			value, _, err := net.ParseCIDR(a.String())
			if err == nil && value.Equal(wanted) {
				return fmt.Errorf("address %s already belongs to %s", ip, iface.Name)
			}
		}
	}
	return nil
}
func apiServerIPs(ctx context.Context, host string) ([]net.IP, error) {
	endpoint, err := url.Parse(host)
	if err != nil {
		return nil, err
	}
	if (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.Hostname() == "" || endpoint.User != nil {
		return nil, errors.New("invalid API server URL")
	}
	if ip := net.ParseIP(endpoint.Hostname()); ip != nil {
		return []net.IP{ip}, nil
	}
	child, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	addresses, err := net.DefaultResolver.LookupIPAddr(child, endpoint.Hostname())
	if err != nil {
		return nil, err
	}
	ips := make([]net.IP, 0, len(addresses))
	for _, a := range addresses {
		ips = append(ips, a.IP)
	}
	return ips, nil
}
func checkControlIPs(plan Plan, apiIPs []net.IP) error {
	for _, b := range plan.Bindings {
		for _, ip := range apiIPs {
			if net.ParseIP(b.IP).Equal(ip) {
				return fmt.Errorf("refusing to capture API server address %s", b.IP)
			}
		}
	}
	return nil
}

// Failed, partially applied acquisitions remain Pending, never recoverable Owned.
// Forget successful ownership once the last local lease has been released.
func confirmedAliasIPs(current []string, confirmed map[string]bool) []string {
	present := map[string]bool{}
	for _, ip := range current {
		present[ip] = true
	}
	for ip := range confirmed {
		if !present[ip] {
			delete(confirmed, ip)
		}
	}
	var result []string
	for _, ip := range current {
		if confirmed[ip] {
			result = append(result, ip)
		}
	}
	return result
}
