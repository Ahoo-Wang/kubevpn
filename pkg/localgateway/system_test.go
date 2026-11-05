//go:build darwin

package localgateway

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
)

type systemTestPaths struct {
	state, hosts, resolvers string
}

func newSystemTestPaths(t *testing.T, hosts []byte) systemTestPaths {
	t.Helper()
	root := t.TempDir()
	paths := systemTestPaths{state: filepath.Join(root, "state"), hosts: filepath.Join(root, "hosts"), resolvers: filepath.Join(root, "resolver")}
	if err := os.Mkdir(paths.resolvers, 0755); err != nil {
		t.Fatal(err)
	}
	writeSystemTestFile(t, paths.hosts, hosts, 0604)
	return paths
}

func (p systemTestPaths) open(t *testing.T, recovering bool) *SystemState {
	t.Helper()
	state, err := newSystemState(p.state, p.hosts, p.resolvers, recovering)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := state.Close(); err != nil {
			t.Error(err)
		}
	})
	return state
}

func writeSystemTestFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func readSystemTestFile(t *testing.T, path string) ([]byte, os.FileMode) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return data, info.Mode().Perm()
}

func readSystemTestJournal(t *testing.T, paths systemTestPaths) systemJournal {
	t.Helper()
	data, mode := readSystemTestFile(t, filepath.Join(paths.state, "journal.json"))
	if mode != 0600 {
		t.Fatalf("journal permissions = %o, want 0600", mode)
	}
	var journal systemJournal
	if err := json.Unmarshal(data, &journal); err != nil {
		t.Fatal(err)
	}
	return journal
}

func systemTestAddresses(data []byte, name string) []string {
	var addresses []string
	for _, line := range strings.Split(string(data), "\n") {
		body, _, _ := strings.Cut(line, "#")
		fields := strings.Fields(body)
		if len(fields) < 2 {
			continue
		}
		for _, field := range fields[1:] {
			if strings.EqualFold(strings.TrimSuffix(field, "."), strings.TrimSuffix(name, ".")) {
				addresses = append(addresses, fields[0])
			}
		}
	}
	return addresses
}

func TestSystemConfigureAndExactRestore(t *testing.T) {
	const hostname = "redis.dev.svc.cluster.local"
	originalHosts := []byte("# original hosts\r\n127.0.0.1 localhost\r\n127.1.27.1 redis redis.dev REDIS.dev.svc.cluster.local. other.dev.svc.cluster.local # old forwarding\r\n10.0.0.7 redis.dev.svc.cluster.local\r\n::1 redis.dev.svc.cluster.local\r\n# no final newline")
	paths := newSystemTestPaths(t, originalHosts)
	resolverPath := filepath.Join(paths.resolvers, "svc.cluster.local")
	originalResolver := []byte("# original resolver\nnameserver 192.0.2.53\n")
	writeSystemTestFile(t, resolverPath, originalResolver, 0640)
	state := paths.open(t, false)
	initial := readSystemTestJournal(t, paths)
	if !bytes.Equal(initial.Hosts.Data, originalHosts) || initial.Hosts.Mode != 0604 || !bytes.Equal(initial.ExpectedHosts, originalHosts) {
		t.Fatalf("initial recovery image missing: %+v", initial)
	}
	if err := state.Configure([]string{"svc.cluster.local", "dev.svc.cluster.local"}, 53053, []string{hostname}); err != nil {
		t.Fatal(err)
	}
	current, hostsMode := readSystemTestFile(t, paths.hosts)
	if got := systemTestAddresses(current, hostname); !reflect.DeepEqual(got, []string{"10.0.0.7", "::1"}) {
		t.Fatalf("selected non-127 mappings changed: %v\n%s", got, current)
	}
	for _, name := range []string{"redis", "redis.dev", "other.dev.svc.cluster.local"} {
		if got := systemTestAddresses(current, name); !reflect.DeepEqual(got, []string{"127.1.27.1"}) {
			t.Fatalf("unselected/short mapping %s changed: %v", name, got)
		}
	}
	if hostsMode != 0604 {
		t.Fatalf("hosts permissions changed during configure: %o", hostsMode)
	}
	journal := readSystemTestJournal(t, paths)
	if !bytes.Equal(journal.Hosts.Data, originalHosts) || !bytes.Equal(journal.ExpectedHosts, current) || len(journal.Removed) != 1 {
		t.Fatalf("recovery journal does not describe applied hosts: %+v", journal)
	}
	for _, domain := range []string{"svc.cluster.local", "dev.svc.cluster.local"} {
		data, _ := readSystemTestFile(t, filepath.Join(paths.resolvers, domain))
		for _, field := range []string{"domain " + domain + "\n", "nameserver 127.0.0.1\n", "port 53053\n"} {
			if !bytes.Contains(data, []byte(field)) {
				t.Fatalf("resolver %s missing %q: %s", domain, field, data)
			}
		}
		if !bytes.Equal(journal.Resolvers[domain].Written, data) {
			t.Fatalf("resolver write was not recorded: %s", domain)
		}
	}
	if before := journal.Resolvers["svc.cluster.local"].Before; !before.Exists || !bytes.Equal(before.Data, originalResolver) || before.Mode != 0640 {
		t.Fatalf("original resolver image missing: %+v", before)
	}
	if journal.Resolvers["dev.svc.cluster.local"].Before.Exists {
		t.Fatal("new resolver was incorrectly recorded as preexisting")
	}
	if err := state.Restore(); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		path string
		data []byte
		mode os.FileMode
	}{{paths.hosts, originalHosts, 0604}, {resolverPath, originalResolver, 0640}} {
		data, mode := readSystemTestFile(t, check.path)
		if !bytes.Equal(data, check.data) || mode != check.mode {
			t.Fatalf("exact image not restored for %s: mode=%o data=%q", check.path, mode, data)
		}
	}
	if _, err := os.Lstat(filepath.Join(paths.resolvers, "dev.svc.cluster.local")); !os.IsNotExist(err) {
		t.Fatalf("session-created resolver remains: %v", err)
	}
	if err := state.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(paths.state, "journal.json")); !os.IsNotExist(err) {
		t.Fatalf("finished journal remains: %v", err)
	}
}

func TestSystemConfigureRequiresJournalBeforeWriting(t *testing.T) {
	original := []byte("127.1.27.1 redis.dev.svc.cluster.local\n")
	paths := newSystemTestPaths(t, original)
	resolver := filepath.Join(paths.resolvers, "svc.cluster.local")
	originalResolver := []byte("nameserver 192.0.2.53\n")
	writeSystemTestFile(t, resolver, originalResolver, 0640)
	state := paths.open(t, false)
	journal := filepath.Join(paths.state, "journal.json")
	if err := os.Remove(journal); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(journal, 0700); err != nil {
		t.Fatal(err)
	}
	if err := state.Configure([]string{"svc.cluster.local"}, 53053, []string{"redis.dev.svc.cluster.local"}); err == nil {
		t.Fatal("configure continued after the recovery journal could not be saved")
	}
	for path, expected := range map[string][]byte{paths.hosts: original, resolver: originalResolver} {
		data, _ := readSystemTestFile(t, path)
		if !bytes.Equal(data, expected) {
			t.Fatalf("%s changed before the journal was saved: %q", path, data)
		}
	}
}

func TestSystemRestoreMergesExternalHostsWithoutReplacingNames(t *testing.T) {
	const redis = "redis.dev.svc.cluster.local"
	const api = "api.dev.svc.cluster.local"
	original := []byte("127.1.27.1 redis " + redis + "\n127.1.27.2 " + api + "\n")
	paths := newSystemTestPaths(t, original)
	state := paths.open(t, false)
	if err := state.Configure([]string{"svc.cluster.local"}, 53053, []string{redis, api}); err != nil {
		t.Fatal(err)
	}
	external := []byte("# edited by another application\n10.0.0.9 REDIS.dev.svc.cluster.local.\n192.0.2.8 new.example\n")
	writeSystemTestFile(t, paths.hosts, external, 0640)
	if err := state.Restore(); err != nil {
		t.Fatal(err)
	}
	data, mode := readSystemTestFile(t, paths.hosts)
	if !bytes.HasPrefix(data, external) || mode != 0640 {
		t.Fatalf("external content or permissions were replaced: mode=%o data=%q", mode, data)
	}
	if got := systemTestAddresses(data, redis); !reflect.DeepEqual(got, []string{"10.0.0.9"}) {
		t.Fatalf("new external mapping was overridden: %v", got)
	}
	if got := systemTestAddresses(data, api); !reflect.DeepEqual(got, []string{"127.1.27.2"}) {
		t.Fatalf("missing removed mapping was not recovered: %v", got)
	}
	if got := systemTestAddresses(data, "redis"); len(got) != 0 {
		t.Fatalf("recreated an unrelated row that the external writer removed: %v", got)
	}
	if err := state.Restore(); err != nil {
		t.Fatal(err)
	}
	again, _ := readSystemTestFile(t, paths.hosts)
	if !bytes.Equal(again, data) {
		t.Fatalf("repeated recovery duplicated mappings: %q", again)
	}
}

func TestSystemRestoreRecoversEveryOriginalIPForMissingName(t *testing.T) {
	const hostname = "redis.dev.svc.cluster.local"
	original := []byte("127.1.27.1 " + hostname + "\n127.1.27.2 REDIS.dev.svc.cluster.local.\n")
	paths := newSystemTestPaths(t, original)
	state := paths.open(t, false)
	if err := state.Configure(nil, 53053, []string{hostname}); err != nil {
		t.Fatal(err)
	}
	// An empty external file exercises recovery without an existing final line.
	writeSystemTestFile(t, paths.hosts, nil, 0644)
	if err := state.Restore(); err != nil {
		t.Fatal(err)
	}
	data, _ := readSystemTestFile(t, paths.hosts)
	if got := systemTestAddresses(data, hostname); !reflect.DeepEqual(got, []string{"127.1.27.1", "127.1.27.2"}) {
		t.Fatalf("lost one of the original addresses: %v\n%s", got, data)
	}
	if err := state.Restore(); err != nil {
		t.Fatal(err)
	}
	again, _ := readSystemTestFile(t, paths.hosts)
	if !bytes.Equal(again, data) {
		t.Fatalf("multi-address recovery was not idempotent: %q", again)
	}
}

func TestSystemRestoreLeavesFullyRemappedHostsBytesUntouched(t *testing.T) {
	const hostname = "redis.dev.svc.cluster.local"
	paths := newSystemTestPaths(t, []byte("127.1.27.1 "+hostname+"\n"))
	state := paths.open(t, false)
	if err := state.Configure(nil, 53053, []string{hostname}); err != nil {
		t.Fatal(err)
	}
	external := []byte("# external replacement without a final newline\n10.0.0.9 " + hostname)
	writeSystemTestFile(t, paths.hosts, external, 0640)
	if err := state.Restore(); err != nil {
		t.Fatal(err)
	}
	actual, mode := readSystemTestFile(t, paths.hosts)
	if !bytes.Equal(actual, external) || mode != 0640 {
		t.Fatalf("changed externally replaced hosts despite no missing mappings: mode=%o data=%q", mode, actual)
	}
}

func TestSystemRestorePreservesExternalResolverAndJournal(t *testing.T) {
	original := []byte("127.1.27.1 redis.dev.svc.cluster.local\n")
	paths := newSystemTestPaths(t, original)
	state := paths.open(t, false)
	if err := state.Configure([]string{"svc.cluster.local", "dev.svc.cluster.local"}, 53053, []string{"redis.dev.svc.cluster.local"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(paths.resolvers, "svc.cluster.local")
	external := []byte("# resolver changed externally\nnameserver 192.0.2.54\n")
	writeSystemTestFile(t, path, external, 0600)
	if err := state.Restore(); err == nil || !strings.Contains(err.Error(), "changed externally") {
		t.Fatalf("resolver conflict was not reported: %v", err)
	}
	data, mode := readSystemTestFile(t, path)
	if !bytes.Equal(data, external) || mode != 0600 {
		t.Fatalf("external resolver was overwritten: mode=%o data=%q", mode, data)
	}
	if _, err := os.Stat(filepath.Join(paths.state, "journal.json")); err != nil {
		t.Fatalf("conflicted recovery lost its journal: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(paths.resolvers, "dev.svc.cluster.local")); !os.IsNotExist(err) {
		t.Fatalf("unconflicted resolver was not restored: %v", err)
	}
	hosts, _ := readSystemTestFile(t, paths.hosts)
	if !bytes.Equal(hosts, original) {
		t.Fatalf("hosts recovery did not progress despite resolver conflict: %q", hosts)
	}
}

func TestSystemRestoreRefusesMissingHostsAndKeepsJournal(t *testing.T) {
	const hostname = "redis.dev.svc.cluster.local"
	paths := newSystemTestPaths(t, []byte("127.1.27.1 "+hostname+"\n"))
	state := paths.open(t, false)
	if err := state.Configure(nil, 53053, []string{hostname}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(paths.hosts); err != nil {
		t.Fatal(err)
	}
	if err := state.Restore(); err == nil {
		t.Fatal("reported complete recovery after the hosts file disappeared")
	}
	if _, err := os.Lstat(paths.hosts); !os.IsNotExist(err) {
		t.Fatalf("silently recreated an externally removed hosts file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(paths.state, "journal.json")); err != nil {
		t.Fatalf("incomplete hosts recovery lost its journal: %v", err)
	}
}

func TestSystemRestorePreservesConcurrentPermissionChanges(t *testing.T) {
	for _, target := range []string{"hosts", "resolver"} {
		t.Run(target, func(t *testing.T) {
			const hostname = "redis.dev.svc.cluster.local"
			paths := newSystemTestPaths(t, []byte("127.1.27.1 "+hostname+"\n"))
			resolver := filepath.Join(paths.resolvers, "svc.cluster.local")
			writeSystemTestFile(t, resolver, []byte("nameserver 192.0.2.53\n"), 0644)
			state := paths.open(t, false)
			if err := state.Configure([]string{"svc.cluster.local"}, 53053, []string{hostname}); err != nil {
				t.Fatal(err)
			}
			path := paths.hosts
			if target == "resolver" {
				path = resolver
			}
			written, _ := readSystemTestFile(t, path)
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			err := state.Restore()
			data, mode := readSystemTestFile(t, path)
			if mode != 0600 {
				t.Fatalf("external %s chmod was overwritten: %o", target, mode)
			}
			if target == "resolver" {
				if err == nil || !bytes.Equal(data, written) {
					t.Fatalf("resolver metadata conflict was not left for recovery: err=%v data=%q", err, data)
				}
				if _, err := os.Stat(filepath.Join(paths.state, "journal.json")); err != nil {
					t.Fatal("resolver metadata conflict lost its journal")
				}
			} else if err != nil || !slices.Equal(systemTestAddresses(data, hostname), []string{"127.1.27.1"}) {
				t.Fatalf("hosts mapping was not merged with external metadata: err=%v data=%q", err, data)
			}
		})
	}
}

func TestSystemLockRequiresExplicitRecovery(t *testing.T) {
	paths := newSystemTestPaths(t, []byte("127.0.0.1 localhost\n"))
	first := paths.open(t, false)
	if second, err := newSystemState(paths.state, paths.hosts, paths.resolvers, false); err == nil {
		_ = second.Close()
		t.Fatal("second process acquired an owned state directory")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if second, err := newSystemState(paths.state, paths.hosts, paths.resolvers, false); err == nil {
		_ = second.Close()
		t.Fatal("new session silently reused an unclean journal")
	} else if !strings.Contains(err.Error(), "unclean session journal") {
		t.Fatalf("wrong unclean-session diagnosis: %v", err)
	}
	recovery := paths.open(t, true)
	if err := recovery.Restore(); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Close(); err != nil {
		t.Fatal(err)
	}
	_ = paths.open(t, false)
}

func TestSystemAliasJournalKeepsPendingSeparateFromOwned(t *testing.T) {
	paths := newSystemTestPaths(t, []byte("127.0.0.1 localhost\n"))
	state := paths.open(t, false)
	for _, ip := range []string{"10.0.0.10", "172.16.1.20"} {
		if err := state.PendingIP(ip); err != nil {
			t.Fatal(err)
		}
	}
	owned := []string{"10.0.0.10"}
	if err := state.Aliases(owned); err != nil {
		t.Fatal(err)
	}
	owned[0] = "192.0.2.1"
	journal := readSystemTestJournal(t, paths)
	if !slices.Equal(journal.OwnedIPs, []string{"10.0.0.10"}) || !slices.Equal(journal.PendingIPs, []string{"172.16.1.20"}) {
		t.Fatalf("pending ownership was lost/promoted: %+v", journal)
	}
	reportedOwned, reportedPending := state.RecordedAliases()
	reportedOwned[0], reportedPending[0] = "192.0.2.1", "192.0.2.2"
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := paths.open(t, true)
	actualOwned, actualPending := recovered.RecordedAliases()
	if !slices.Equal(actualOwned, []string{"10.0.0.10"}) || !slices.Equal(actualPending, []string{"172.16.1.20"}) {
		t.Fatalf("recovery confused pending and owned aliases: owned=%v pending=%v", actualOwned, actualPending)
	}
}

func TestSystemRefusesSymlinkFiles(t *testing.T) {
	for _, target := range []string{"initial hosts", "lock", "recovery journal", "configure resolver", "update hosts", "restore hosts", "restore resolver"} {
		t.Run(target, func(t *testing.T) {
			paths := newSystemTestPaths(t, []byte("127.1.27.1 redis.dev.svc.cluster.local\n"))
			external := filepath.Join(filepath.Dir(paths.hosts), "external")
			original := []byte("# external file must remain untouched\n")
			writeSystemTestFile(t, external, original, 0640)
			link := func(path string) {
				t.Helper()
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				if err := os.Symlink(external, path); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			switch target {
			case "initial hosts", "lock":
				if target == "initial hosts" {
					link(paths.hosts)
				} else {
					if err := os.Mkdir(paths.state, 0700); err != nil {
						t.Fatal(err)
					}
					link(filepath.Join(paths.state, "lock"))
				}
				var state *SystemState
				state, err = newSystemState(paths.state, paths.hosts, paths.resolvers, false)
				if state != nil {
					_ = state.Close()
				}
			case "recovery journal":
				state := paths.open(t, false)
				if err := state.Close(); err != nil {
					t.Fatal(err)
				}
				link(filepath.Join(paths.state, "journal.json"))
				var recovered *SystemState
				recovered, err = newSystemState(paths.state, paths.hosts, paths.resolvers, true)
				if recovered != nil {
					_ = recovered.Close()
				}
			default:
				state := paths.open(t, false)
				resolver := filepath.Join(paths.resolvers, "svc.cluster.local")
				switch target {
				case "configure resolver":
					link(resolver)
					err = state.Configure([]string{"svc.cluster.local"}, 53053, []string{"redis.dev.svc.cluster.local"})
				case "update hosts":
					link(paths.hosts)
					err = state.UpdateHosts([]string{"redis.dev.svc.cluster.local"})
				default:
					if err := state.Configure([]string{"svc.cluster.local"}, 53053, []string{"redis.dev.svc.cluster.local"}); err != nil {
						t.Fatal(err)
					}
					if target == "restore hosts" {
						link(paths.hosts)
					} else {
						link(resolver)
					}
					err = state.Restore()
					if _, err := os.Stat(filepath.Join(paths.state, "journal.json")); err != nil {
						t.Fatal("unsafe recovery lost its journal")
					}
				}
			}
			if err == nil {
				t.Fatal("followed a symlink instead of refusing the operation")
			}
			data, mode := readSystemTestFile(t, external)
			if !bytes.Equal(data, original) || mode != 0640 {
				t.Fatalf("symlink target changed: mode=%o data=%q", mode, data)
			}
		})
	}
}

func TestSystemStateRejectsNonPrivateAndSymlinkDirectories(t *testing.T) {
	paths := newSystemTestPaths(t, []byte("127.0.0.1 localhost\n"))
	if err := os.Mkdir(paths.state, 0755); err != nil {
		t.Fatal(err)
	}
	if state, err := newSystemState(paths.state, paths.hosts, paths.resolvers, false); err == nil {
		_ = state.Close()
		t.Fatal("accepted a state directory visible to other users")
	}
	if err := os.Chmod(paths.state, 0700); err != nil {
		t.Fatal(err)
	}
	alias := paths.state + "-link"
	if err := os.Symlink(paths.state, alias); err != nil {
		t.Fatal(err)
	}
	if state, err := newSystemState(alias, paths.hosts, paths.resolvers, false); err == nil {
		_ = state.Close()
		t.Fatal("followed a symlink state directory")
	}
	// The lock must reject a FIFO without waiting for another writer.
	if err := syscall.Mkfifo(filepath.Join(paths.state, "lock"), 0600); err != nil {
		t.Fatal(err)
	}
	if state, err := newSystemState(paths.state, paths.hosts, paths.resolvers, false); err == nil {
		_ = state.Close()
		t.Fatal("accepted a nonregular lock file")
	}
}
