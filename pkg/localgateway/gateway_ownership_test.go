//go:build darwin

package localgateway

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGatewayAliasJournalConfirmsSuccessfulAcquisitionsOnly(t *testing.T) {
	confirmed := map[string]bool{"10.42.0.10": true}
	// The alias manager may retain a failed partial acquisition while cleanup is
	// pending. It must not become recoverable Owned merely because it is present.
	got := confirmedAliasIPs([]string{"10.42.0.10", "10.42.0.20"}, confirmed)
	if !reflect.DeepEqual(got, []string{"10.42.0.10"}) {
		t.Fatalf("failed acquisition upgraded to owned: %v", got)
	}
	if confirmed["10.42.0.20"] {
		t.Fatal("pending address added to confirmation set")
	}
	// Several bindings can share one confirmed address until its last lease ends.
	got = confirmedAliasIPs([]string{"10.42.0.10", "10.42.0.20"}, confirmed)
	if !reflect.DeepEqual(got, []string{"10.42.0.10"}) {
		t.Fatalf("shared successful ownership lost: %v", got)
	}
	if got := confirmedAliasIPs([]string{"10.42.0.20"}, confirmed); len(got) != 0 {
		t.Fatalf("released address retained: %v", got)
	}
	if confirmed["10.42.0.10"] {
		t.Fatal("last release did not forget successful confirmation")
	}
	// A later failed attempt for the same IP must not inherit an earlier lease.
	if got := confirmedAliasIPs([]string{"10.42.0.10", "10.42.0.20"}, confirmed); len(got) != 0 {
		t.Fatalf("old confirmation reused by failed acquisition: %v", got)
	}
	// Only a subsequent successful Acquire authorizes it again.
	confirmed["10.42.0.10"] = true
	if got := confirmedAliasIPs([]string{"10.42.0.10", "10.42.0.20"}, confirmed); !reflect.DeepEqual(got, []string{"10.42.0.10"}) {
		t.Fatalf("new successful acquisition not recorded: %v", got)
	}
}

// Wire the ownership filter into the actual recovery journal. A failed partial
// acquisition must remain pending across later successful writes and releases.
func TestGatewayAliasJournalKeepsUnconfirmedOperationsPending(t *testing.T) {
	root := t.TempDir()
	hosts := filepath.Join(root, "hosts")
	if err := os.WriteFile(hosts, []byte("127.0.0.1 localhost\n"), 0644); err != nil {
		t.Fatal(err)
	}
	state, err := newSystemState(filepath.Join(root, "state"), hosts, filepath.Join(root, "resolver"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	confirmed := map[string]bool{"10.42.0.10": true}
	if err := state.PendingIP("10.42.0.20"); err != nil {
		t.Fatal(err)
	}
	if err := state.Aliases(confirmedAliasIPs([]string{"10.42.0.10", "10.42.0.20"}, confirmed)); err != nil {
		t.Fatal(err)
	}
	owned, pending := state.RecordedAliases()
	if !reflect.DeepEqual(owned, []string{"10.42.0.10"}) || !reflect.DeepEqual(pending, []string{"10.42.0.20"}) {
		t.Fatalf("wrong recovery authority: owned=%v pending=%v", owned, pending)
	}
	if err := state.Aliases(confirmedAliasIPs([]string{"10.42.0.20"}, confirmed)); err != nil {
		t.Fatal(err)
	}
	owned, pending = state.RecordedAliases()
	if len(owned) != 0 || !reflect.DeepEqual(pending, []string{"10.42.0.20"}) {
		t.Fatalf("released lease changed pending authority: owned=%v pending=%v", owned, pending)
	}
	// A later failed attempt for the released address stays outside Owned.
	if err := state.PendingIP("10.42.0.10"); err != nil {
		t.Fatal(err)
	}
	if err := state.Aliases(confirmedAliasIPs([]string{"10.42.0.10", "10.42.0.20"}, confirmed)); err != nil {
		t.Fatal(err)
	}
	owned, pending = state.RecordedAliases()
	if len(owned) != 0 || len(pending) != 2 {
		t.Fatalf("old confirmation promoted a later failure: owned=%v pending=%v", owned, pending)
	}
}
