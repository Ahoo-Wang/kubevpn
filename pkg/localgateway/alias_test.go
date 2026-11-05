//go:build darwin

package localgateway

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"
)

type fakeAliasHost struct {
	addrs       []aliasInterfaceAddress
	calls       [][]string
	snapshots   int
	snapshotErr func(int) error
	onAdd       func(context.Context, netip.Addr) error
	onDelete    func(context.Context, netip.Addr) error
}

func aliasAddr(name, prefix string) aliasInterfaceAddress {
	return aliasInterfaceAddress{name: name, prefix: netip.MustParsePrefix(prefix)}
}

func newFakeAliasManager(t *testing.T) (*AliasManager, *fakeAliasHost) {
	t.Helper()
	f := &fakeAliasHost{addrs: []aliasInterfaceAddress{aliasAddr("lo0", "127.0.0.1/8")}}
	m := NewAliasManager()
	m.interfaceAddrs = func() ([]aliasInterfaceAddress, error) {
		f.snapshots++
		if f.snapshotErr != nil {
			if err := f.snapshotErr(f.snapshots); err != nil {
				return nil, err
			}
		}
		return append([]aliasInterfaceAddress(nil), f.addrs...), nil
	}
	m.runCommand = func(ctx context.Context, name string, args ...string) error {
		if name != "/sbin/ifconfig" {
			t.Fatalf("untrusted command %q", name)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("ifconfig has no deadline")
		}
		f.calls = append(f.calls, append([]string{name}, args...))
		if len(args) < 3 || args[0] != "lo0" || args[1] != "inet" {
			t.Fatalf("unexpected command arguments: %v", args)
		}
		ip := netip.MustParseAddr(args[2])
		switch {
		case len(args) == 6 && args[3] == "netmask" && args[4] == "255.255.255.255" && args[5] == "alias":
			if f.onAdd != nil {
				return f.onAdd(ctx, ip)
			}
			f.add(ip)
		case len(args) == 4 && args[3] == "-alias":
			if f.onDelete != nil {
				return f.onDelete(ctx, ip)
			}
			f.remove(ip)
		default:
			t.Fatalf("unexpected command arguments: %v", args)
		}
		return nil
	}
	return m, f
}

func (f *fakeAliasHost) add(ip netip.Addr) {
	f.addrs = append(f.addrs, aliasInterfaceAddress{name: "lo0", prefix: netip.PrefixFrom(ip, 32)})
}

func (f *fakeAliasHost) remove(ip netip.Addr) {
	kept := f.addrs[:0]
	for _, addr := range f.addrs {
		if addr.name != "lo0" || addr.prefix.Addr() != ip {
			kept = append(kept, addr)
		}
	}
	f.addrs = kept
}

func TestAliasRejectsNonHostAddresses(t *testing.T) {
	for _, ip := range []string{"", "example.com", "10.0.0.1/32", "::1", "::ffff:10.0.0.1", "0.0.0.0", "0.1.2.3", "127.1.2.3", "169.254.1.2", "224.0.0.1", "240.0.0.1", "255.255.255.255"} {
		t.Run(ip, func(t *testing.T) {
			m, host := newFakeAliasManager(t)
			if err := m.Acquire(context.Background(), ip); err == nil {
				t.Fatal("accepted a non-host address")
			}
			if len(host.calls) != 0 || len(m.OwnedIPs()) != 0 {
				t.Fatal("invalid input mutated host or ownership")
			}
		})
	}
}

func TestAliasRefusesExistingAddressOnAnyInterface(t *testing.T) {
	for _, iface := range []string{"en0", "lo0", "utun14"} {
		t.Run(iface, func(t *testing.T) {
			m, host := newFakeAliasManager(t)
			host.addrs = append(host.addrs, aliasAddr(iface, "10.0.106.233/32"))
			before := append([]aliasInterfaceAddress(nil), host.addrs...)
			if err := m.Acquire(context.Background(), "10.0.106.233"); err == nil {
				t.Fatal("adopted an external address")
			}
			if err := m.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(host.calls) != 0 || !reflect.DeepEqual(host.addrs, before) {
				t.Fatal("external address was changed")
			}
		})
	}
}

func TestAliasRejectsKnownSubnetBroadcastButAllowsHostEnding255(t *testing.T) {
	m, host := newFakeAliasManager(t)
	host.addrs = append(host.addrs, aliasAddr("en0", "192.168.71.40/24"))
	if err := m.Acquire(context.Background(), "192.168.71.255"); err == nil {
		t.Fatal("accepted the local subnet broadcast")
	}
	if err := m.Acquire(context.Background(), "10.0.1.255"); err != nil {
		t.Fatalf("a host address is not globally invalid just because it ends in 255: %v", err)
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAliasSharesReferencesAndOnlyDeletesItsAddress(t *testing.T) {
	m, host := newFakeAliasManager(t)
	host.addrs = append(host.addrs, aliasAddr("lo0", "10.0.106.234/32"))
	ctx := context.Background()
	for range 2 {
		if err := m.Acquire(ctx, "10.0.106.233"); err != nil {
			t.Fatal(err)
		}
	}
	if len(host.calls) != 1 {
		t.Fatalf("duplicate acquire ran commands: %v", host.calls)
	}
	if err := m.Release(ctx, "10.0.106.233"); err != nil || len(host.calls) != 1 {
		t.Fatalf("first reference removed shared alias: %v", err)
	}
	if err := m.Release(ctx, "10.0.106.233"); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(ctx, "10.0.106.233"); err != nil || len(host.calls) != 2 {
		t.Fatalf("release is not idempotent: %v", err)
	}
	if !reflect.DeepEqual(host.addrs, []aliasInterfaceAddress{aliasAddr("lo0", "127.0.0.1/8"), aliasAddr("lo0", "10.0.106.234/32")}) {
		t.Fatalf("unrelated aliases changed: %v", host.addrs)
	}
}

func TestAliasRollsBackPartiallySuccessfulAdd(t *testing.T) {
	m, host := newFakeAliasManager(t)
	addErr := errors.New("ifconfig failed after adding")
	host.onAdd = func(_ context.Context, ip netip.Addr) error { host.add(ip); return addErr }
	if err := m.Acquire(context.Background(), "10.0.106.233"); !errors.Is(err, addErr) {
		t.Fatalf("lost original failure: %v", err)
	}
	if len(m.OwnedIPs()) != 0 || len(host.calls) != 2 || len(host.addrs) != 1 {
		t.Fatalf("partial addition leaked: owned=%v calls=%v addrs=%v", m.OwnedIPs(), host.calls, host.addrs)
	}
}

func TestAliasFailedAddWithoutMutationDoesNotDelete(t *testing.T) {
	m, host := newFakeAliasManager(t)
	host.onAdd = func(context.Context, netip.Addr) error { return errors.New("not permitted") }
	if err := m.Acquire(context.Background(), "10.0.106.233"); err == nil {
		t.Fatal("missing add failure")
	}
	if len(host.calls) != 1 || len(m.OwnedIPs()) != 0 {
		t.Fatal("attempted to remove an address never added")
	}
}

func TestAliasFailedRollbackRetainsOwnershipForRetry(t *testing.T) {
	m, host := newFakeAliasManager(t)
	addErr, deleteErr := errors.New("add failed"), errors.New("delete failed")
	host.onAdd = func(_ context.Context, ip netip.Addr) error { host.add(ip); return addErr }
	host.onDelete = func(context.Context, netip.Addr) error { return deleteErr }
	err := m.Acquire(context.Background(), "10.0.106.233")
	if !errors.Is(err, addErr) || !errors.Is(err, deleteErr) {
		t.Fatalf("missing acquisition or rollback failure: %v", err)
	}
	if !reflect.DeepEqual(m.OwnedIPs(), []string{"10.0.106.233"}) {
		t.Fatal("failed cleanup lost ownership")
	}
	calls := len(host.calls)
	if err := m.Acquire(context.Background(), "10.0.106.233"); err == nil || len(host.calls) != calls {
		t.Fatal("pending cleanup was silently adopted")
	}
	host.onDelete = nil
	if err := m.Release(context.Background(), "10.0.106.233"); err != nil || len(m.OwnedIPs()) != 0 {
		t.Fatalf("cleanup retry failed: %v", err)
	}
}

func TestAliasCanceledAddStillRollsBack(t *testing.T) {
	m, host := newFakeAliasManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host.onAdd = func(_ context.Context, ip netip.Addr) error { host.add(ip); cancel(); return ctx.Err() }
	host.onDelete = func(cleanup context.Context, ip netip.Addr) error {
		if err := cleanup.Err(); err != nil {
			t.Fatalf("cleanup inherited canceled context: %v", err)
		}
		host.remove(ip)
		return nil
	}
	if err := m.Acquire(ctx, "10.0.106.233"); !errors.Is(err, context.Canceled) {
		t.Fatalf("missing cancellation: %v", err)
	}
	if len(m.OwnedIPs()) != 0 || len(host.addrs) != 1 {
		t.Fatal("cancellation leaked an alias")
	}
}

func TestAliasVerificationFailureRollsBack(t *testing.T) {
	m, host := newFakeAliasManager(t)
	snapshotErr := errors.New("interface snapshot failed")
	host.snapshotErr = func(n int) error {
		if n == 2 {
			return snapshotErr
		}
		return nil
	}
	if err := m.Acquire(context.Background(), "10.0.106.233"); !errors.Is(err, snapshotErr) {
		t.Fatalf("missing verification failure: %v", err)
	}
	if len(m.OwnedIPs()) != 0 || len(host.addrs) != 1 {
		t.Fatal("verification failure leaked an alias")
	}
}

func TestAliasDeletionMustBeVerifiedAndRetryable(t *testing.T) {
	m, host := newFakeAliasManager(t)
	if err := m.Acquire(context.Background(), "10.0.106.233"); err != nil {
		t.Fatal(err)
	}
	host.onDelete = func(context.Context, netip.Addr) error { return nil }
	if err := m.Release(context.Background(), "10.0.106.233"); err == nil || len(m.OwnedIPs()) != 1 {
		t.Fatal("unverified deletion discarded ownership")
	}
	ips := m.OwnedIPs()
	ips[0] = "external mutation"
	host.onDelete = nil
	if err := m.Release(context.Background(), "10.0.106.233"); err != nil || len(m.OwnedIPs()) != 0 {
		t.Fatalf("retry failed: %v", err)
	}
}

func TestAliasExternallyMovedAddressIsNotDeleted(t *testing.T) {
	m, host := newFakeAliasManager(t)
	if err := m.Acquire(context.Background(), "10.0.106.233"); err != nil {
		t.Fatal(err)
	}
	host.remove(netip.MustParseAddr("10.0.106.233"))
	host.addrs = append(host.addrs, aliasAddr("en0", "10.0.106.233/32"))
	if err := m.Release(context.Background(), "10.0.106.233"); err != nil {
		t.Fatal(err)
	}
	if len(host.calls) != 1 || len(m.OwnedIPs()) != 0 {
		t.Fatal("attempted to remove an address from another interface")
	}
}

func TestAliasChangedPrefixIsNotDeleted(t *testing.T) {
	m, host := newFakeAliasManager(t)
	if err := m.Acquire(context.Background(), "10.0.106.233"); err != nil {
		t.Fatal(err)
	}
	host.addrs[1] = aliasAddr("lo0", "10.0.106.233/24")
	if err := m.Release(context.Background(), "10.0.106.233"); err == nil {
		t.Fatal("accepted changed ownership boundary")
	}
	if len(host.calls) != 1 || len(m.OwnedIPs()) != 1 {
		t.Fatal("removed a changed prefix or lost cleanup state")
	}
	host.addrs[1] = aliasAddr("lo0", "10.0.106.233/32")
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAliasCloseAttemptsAllAndKeepsFailures(t *testing.T) {
	m, host := newFakeAliasManager(t)
	ctx := context.Background()
	for _, ip := range []string{"10.0.106.234", "10.0.106.233"} {
		if err := m.Acquire(ctx, ip); err != nil {
			t.Fatal(err)
		}
	}
	deleteErr := errors.New("delete blocked")
	host.onDelete = func(_ context.Context, ip netip.Addr) error {
		if ip.String() == "10.0.106.233" {
			return deleteErr
		}
		host.remove(ip)
		return nil
	}
	if err := m.Close(ctx); !errors.Is(err, deleteErr) {
		t.Fatalf("lost close failure: %v", err)
	}
	if !reflect.DeepEqual(m.OwnedIPs(), []string{"10.0.106.233"}) || len(host.calls) != 4 {
		t.Fatalf("did not attempt all aliases: owned=%v calls=%v", m.OwnedIPs(), host.calls)
	}
	if err := m.Acquire(ctx, "10.0.106.235"); err == nil {
		t.Fatal("closed manager accepted new work")
	}
	host.onDelete = nil
	if err := m.Close(ctx); err != nil || len(m.OwnedIPs()) != 0 {
		t.Fatalf("close retry failed: %v", err)
	}
}

func TestAliasCommandTimeoutDoesNotLeak(t *testing.T) {
	m, host := newFakeAliasManager(t)
	m.commandTimeout = 10 * time.Millisecond
	host.onAdd = func(ctx context.Context, _ netip.Addr) error { <-ctx.Done(); return ctx.Err() }
	if err := m.Acquire(context.Background(), "10.0.106.233"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing short command deadline: %v", err)
	}
	if len(m.OwnedIPs()) != 0 {
		t.Fatal("timeout retained an address known not to exist")
	}
}

func TestAliasConcurrentReferences(t *testing.T) {
	m, host := newFakeAliasManager(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if err := m.Acquire(ctx, "10.0.106.233"); err != nil {
				t.Errorf("acquire: %v", err)
			}
		})
	}
	wg.Wait()
	if len(host.calls) != 1 {
		t.Fatalf("concurrent acquire added more than once: %v", host.calls)
	}
	for range 16 {
		wg.Go(func() {
			if err := m.Release(ctx, "10.0.106.233"); err != nil {
				t.Errorf("release: %v", err)
			}
		})
	}
	wg.Wait()
	if len(host.calls) != 2 || len(m.OwnedIPs()) != 0 {
		t.Fatalf("concurrent release leaked or deleted twice: calls=%v owned=%v", host.calls, m.OwnedIPs())
	}
}

func TestRecoverAliasesValidatesWholeJournalBeforeMutation(t *testing.T) {
	m, host := newFakeAliasManager(t)
	host.add(netip.MustParseAddr("10.0.106.233"))
	if err := recoverAliases(context.Background(), []string{"10.0.106.233", "127.1.2.3"}, m); err == nil {
		t.Fatal("accepted an invalid recovery journal")
	}
	if len(host.calls) != 0 || len(m.OwnedIPs()) != 0 || len(host.addrs) != 2 {
		t.Fatal("invalid journal partially changed ownership or host addresses")
	}
}

func TestRecoverAliasesOnlyDeletesRecordedLoopbackAliases(t *testing.T) {
	m, host := newFakeAliasManager(t)
	host.add(netip.MustParseAddr("10.0.106.233"))
	host.add(netip.MustParseAddr("10.0.106.234"))
	host.addrs = append(host.addrs, aliasAddr("en0", "10.0.106.235/32"))
	if err := recoverAliases(context.Background(), []string{"10.0.106.233", "10.0.106.235", "10.0.106.233"}, m); err != nil {
		t.Fatal(err)
	}
	if len(host.calls) != 1 || len(m.OwnedIPs()) != 0 {
		t.Fatalf("unexpected recovery commands: %v", host.calls)
	}
	want := []aliasInterfaceAddress{aliasAddr("lo0", "127.0.0.1/8"), aliasAddr("lo0", "10.0.106.234/32"), aliasAddr("en0", "10.0.106.235/32")}
	if !reflect.DeepEqual(host.addrs, want) {
		t.Fatalf("recovery changed an unrecorded or external alias: %v", host.addrs)
	}
}

func TestRecoverAliasesKeepsChangedPrefixAndReportsAllFailures(t *testing.T) {
	m, host := newFakeAliasManager(t)
	host.addrs = append(host.addrs, aliasAddr("lo0", "10.0.106.233/24"), aliasAddr("lo0", "10.0.106.234/32"))
	if err := recoverAliases(context.Background(), []string{"10.0.106.233", "10.0.106.234"}, m); err == nil {
		t.Fatal("recovery silently removed an externally changed prefix")
	}
	if !reflect.DeepEqual(m.OwnedIPs(), []string{"10.0.106.233"}) || len(host.calls) != 1 {
		t.Fatalf("changed alias was deleted or other cleanup skipped: owned=%v calls=%v", m.OwnedIPs(), host.calls)
	}
	if host.addrs[1].prefix.Bits() != 24 {
		t.Fatal("changed prefix was modified")
	}
}

func TestAliasSnapshotFailureKeepsCleanupOwnership(t *testing.T) {
	m, host := newFakeAliasManager(t)
	if err := m.Acquire(context.Background(), "10.0.106.233"); err != nil {
		t.Fatal(err)
	}
	snapshotErr := errors.New("snapshot unavailable during cleanup")
	host.snapshotErr = func(int) error { return snapshotErr }
	if err := m.Release(context.Background(), "10.0.106.233"); !errors.Is(err, snapshotErr) {
		t.Fatalf("lost snapshot failure: %v", err)
	}
	if len(m.OwnedIPs()) != 1 || len(host.calls) != 1 {
		t.Fatal("cleanup proceeded without knowing the current address ownership")
	}
	host.snapshotErr = nil
	if err := m.Release(context.Background(), "10.0.106.233"); err != nil || len(m.OwnedIPs()) != 0 {
		t.Fatalf("snapshot recovery did not permit cleanup retry: %v", err)
	}
}

func TestAliasDoesNotReacquireExternallyRemovedAddress(t *testing.T) {
	m, host := newFakeAliasManager(t)
	ctx := context.Background()
	if err := m.Acquire(ctx, "10.0.106.233"); err != nil {
		t.Fatal(err)
	}
	host.remove(netip.MustParseAddr("10.0.106.233"))
	if err := m.Acquire(ctx, "10.0.106.233"); err == nil || len(host.calls) != 1 {
		t.Fatal("silently reacquired an externally removed alias")
	}
	if err := m.Release(ctx, "10.0.106.233"); err != nil || len(m.OwnedIPs()) != 0 {
		t.Fatalf("did not release the obsolete ownership record: %v", err)
	}
}

func TestAliasCloseJoinsIndependentFailures(t *testing.T) {
	m, host := newFakeAliasManager(t)
	ctx := context.Background()
	for _, ip := range []string{"10.0.106.233", "10.0.106.234"} {
		if err := m.Acquire(ctx, ip); err != nil {
			t.Fatal(err)
		}
	}
	first, second := errors.New("first removal failed"), errors.New("second removal failed")
	host.onDelete = func(_ context.Context, ip netip.Addr) error {
		if ip.String() == "10.0.106.233" {
			return first
		}
		return second
	}
	err := m.Close(ctx)
	if !errors.Is(err, first) || !errors.Is(err, second) || len(host.calls) != 4 || len(m.OwnedIPs()) != 2 {
		t.Fatalf("Close lost an independent failure or cleanup record: %v", err)
	}
}
