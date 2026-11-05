//go:build darwin

package localgateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
)

const aliasCommandTimeout = 3 * time.Second

type aliasInterfaceAddress struct {
	name   string
	prefix netip.Prefix
}

// AliasManager owns only IPv4 /32 aliases it added to lo0 in this process.
// All ownership checks and mutations are serialized; existing addresses are never adopted.
type AliasManager struct {
	mu             sync.Mutex
	owned          map[netip.Addr]int
	closed         bool
	interfaceAddrs func() ([]aliasInterfaceAddress, error)
	runCommand     func(context.Context, string, ...string) error
	commandTimeout time.Duration
}

func NewAliasManager() *AliasManager {
	return &AliasManager{
		owned:          make(map[netip.Addr]int),
		interfaceAddrs: snapshotAliasAddresses,
		runCommand:     runAliasCommand,
		commandTimeout: aliasCommandTimeout,
	}
}

// RecoverAliases removes only the recorded lo0 /32 aliases from a trusted journal.
// It is for explicit crash recovery, never arbitrary discovered IPs. All inputs
// are validated before mutation; callers must retain the journal if cleanup fails.
func RecoverAliases(ctx context.Context, recordedIPs []string) error {
	return recoverAliases(ctx, recordedIPs, NewAliasManager())
}

func recoverAliases(ctx context.Context, recordedIPs []string, manager *AliasManager) error {
	ips := make([]netip.Addr, 0, len(recordedIPs))
	for _, text := range recordedIPs {
		ip, err := parseAliasIPv4(text)
		if err != nil {
			return fmt.Errorf("invalid recovery journal: %w", err)
		}
		ips = append(ips, ip)
	}
	manager.mu.Lock()
	if manager.closed || len(manager.owned) != 0 {
		manager.mu.Unlock()
		return errors.New("recovery requires a fresh alias manager")
	}
	for _, ip := range ips {
		manager.owned[ip] = 0
	}
	manager.mu.Unlock()
	return manager.Close(ctx)
}

func (m *AliasManager) Acquire(ctx context.Context, text string) error {
	ip, err := parseAliasIPv4(text)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("alias manager is closed")
	}
	addrs, err := m.snapshot(ctx)
	if err != nil {
		return err
	}
	if refs, exists := m.owned[ip]; exists {
		if refs == 0 {
			return fmt.Errorf("alias %s is pending cleanup; release it before acquiring again", ip)
		}
		present, exact := loopbackAlias(addrs, ip)
		if !present || !exact {
			return fmt.Errorf("owned alias %s is no longer a lo0 /32", ip)
		}
		m.owned[ip]++
		return nil
	}
	for _, addr := range addrs {
		if addr.prefix.Addr() == ip {
			return fmt.Errorf("IP %s is already assigned to %s; refusing to adopt it", ip, addr.name)
		}
		if isAliasBroadcast(ip, addr.prefix) {
			return fmt.Errorf("IP %s is the broadcast address of %s", ip, addr.prefix)
		}
	}

	// Record the cleanup obligation before ifconfig: it may mutate the host and then fail.
	m.owned[ip] = 0
	runErr := m.ifconfig(ctx, "lo0", "inet", ip.String(), "netmask", "255.255.255.255", "alias")
	addrs, verifyErr := m.snapshot(ctx)
	if runErr == nil && verifyErr == nil {
		present, exact := loopbackAlias(addrs, ip)
		if present && exact {
			m.owned[ip] = 1
			return nil
		}
		verifyErr = fmt.Errorf("ifconfig did not install %s as a lo0 /32", ip)
	}
	err = fmt.Errorf("acquire alias %s: %w", ip, errors.Join(runErr, verifyErr))
	// Cancellation must not prevent rolling back an address already added by ifconfig.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*m.commandTimeout)
	defer cancel()
	if cleanupErr := m.remove(cleanupCtx, ip); cleanupErr != nil {
		return errors.Join(err, fmt.Errorf("rollback alias %s: %w", ip, cleanupErr))
	}
	return err
}

func (m *AliasManager) Release(ctx context.Context, text string) error {
	ip, err := parseAliasIPv4(text)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	refs, exists := m.owned[ip]
	if !exists {
		return nil
	}
	if refs > 1 {
		m.owned[ip]--
		return nil
	}
	m.owned[ip] = 0
	return m.remove(ctx, ip)
}

// OwnedIPs includes aliases awaiting cleanup after a failed operation, for journaling.
func (m *AliasManager) OwnedIPs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ownedIPs()
}

func (m *AliasManager) ownedIPs() []string {
	ips := make([]string, 0, len(m.owned))
	for ip := range m.owned {
		ips = append(ips, ip.String())
	}
	sort.Strings(ips)
	return ips
}

// Close prevents new acquisitions and attempts every owned alias, even after a failure.
// Failed cleanup remains owned so Close or Release can retry it.
func (m *AliasManager) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	var errs []error
	for _, text := range m.ownedIPs() {
		ip := netip.MustParseAddr(text)
		m.owned[ip] = 0
		if err := m.remove(ctx, ip); err != nil {
			errs = append(errs, fmt.Errorf("cleanup alias %s: %w", ip, err))
		}
	}
	return errors.Join(errs...)
}

func (m *AliasManager) remove(ctx context.Context, ip netip.Addr) error {
	addrs, err := m.snapshot(ctx)
	if err != nil {
		return err
	}
	present, exact := loopbackAlias(addrs, ip)
	if !present {
		delete(m.owned, ip)
		return nil
	}
	if !exact {
		return fmt.Errorf("refusing to remove %s: its lo0 prefix is no longer /32", ip)
	}
	runErr := m.ifconfig(ctx, "lo0", "inet", ip.String(), "-alias")
	addrs, err = m.snapshot(ctx)
	if err != nil {
		return errors.Join(runErr, err)
	}
	if present, _ := loopbackAlias(addrs, ip); present {
		return errors.Join(runErr, fmt.Errorf("alias %s is still assigned to lo0", ip))
	}
	delete(m.owned, ip)
	return runErr
}

func (m *AliasManager) snapshot(ctx context.Context) ([]aliasInterfaceAddress, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	addrs, err := m.interfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("inspect interface addresses: %w", err)
	}
	return addrs, ctx.Err()
}

func (m *AliasManager) ifconfig(ctx context.Context, args ...string) error {
	callCtx, cancel := context.WithTimeout(ctx, m.commandTimeout)
	defer cancel()
	if err := callCtx.Err(); err != nil {
		return err
	}
	err := m.runCommand(callCtx, "/sbin/ifconfig", args...)
	if err = errors.Join(err, callCtx.Err()); err != nil {
		return fmt.Errorf("/sbin/ifconfig %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func parseAliasIPv4(text string) (netip.Addr, error) {
	ip, err := netip.ParseAddr(text)
	if err == nil && ip.Is4() && ip.IsGlobalUnicast() {
		octets := ip.As4()
		if octets[0] != 0 && octets[0] < 240 {
			return ip, nil
		}
	}
	return netip.Addr{}, fmt.Errorf("alias IP %q must be an IPv4 unicast host address", text)
}

func isAliasBroadcast(ip netip.Addr, prefix netip.Prefix) bool {
	if !prefix.Addr().Is4() || prefix.Bits() >= 31 || !prefix.Contains(ip) {
		return false
	}
	octets := ip.As4()
	hostMask := ^uint32(0) >> prefix.Bits()
	return binary.BigEndian.Uint32(octets[:])&hostMask == hostMask
}

func loopbackAlias(addrs []aliasInterfaceAddress, ip netip.Addr) (present, exact bool) {
	for _, addr := range addrs {
		if addr.name == "lo0" && addr.prefix.Addr() == ip {
			present = true
			if addr.prefix.Bits() != 32 {
				return true, false
			}
		}
	}
	return present, present
}

func snapshotAliasAddresses() ([]aliasInterfaceAddress, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var result []aliasInterfaceAddress
	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", iface.Name, err)
		}
		for _, addr := range addrs {
			prefix, err := netip.ParsePrefix(addr.String())
			if err != nil {
				return nil, fmt.Errorf("%s address %q: %w", iface.Name, addr, err)
			}
			if prefix.Addr().Is4() {
				result = append(result, aliasInterfaceAddress{name: iface.Name, prefix: prefix})
			}
		}
	}
	return result, nil
}

func runAliasCommand(ctx context.Context, name string, args ...string) error {
	output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil && len(output) != 0 {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return err
}
