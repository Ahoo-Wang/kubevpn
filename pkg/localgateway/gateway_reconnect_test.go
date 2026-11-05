//go:build darwin

package localgateway

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/tools/portforward"
)

func TestGatewayRestartsAllClosedSPDYBindingsInOneSnapshot(t *testing.T) {
	pod := newPodFixture(t, false)
	forwarder, err := NewForwarder(pod.config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	running := map[string]activeBinding{}
	defer func() {
		for _, item := range running {
			_ = item.session.Close()
		}
	}()
	var desired []Binding
	var originals []*Session
	var remote []httpstream.Connection
	for range 3 {
		binding := businessBinding(t, pod)
		session, err := forwarder.Start(ctx, binding)
		if err != nil {
			t.Fatal(err)
		}
		desired = append(desired, binding)
		originals = append(originals, session)
		running[bindingKey(binding)] = activeBinding{binding: binding, session: session}
		remote = append(remote, <-pod.sessions)
	}
	healthy := assertPodEcho(t, originals[2], "healthy-before-recovery")
	defer healthy.Close()
	for _, session := range remote[:2] {
		_ = session.Close()
	}
	for _, session := range originals[:2] {
		waitForSession(t, session, portforward.ErrLostConnectionToPod)
	}

	var watched []string
	watch := func(key string, _ *Session) { watched = append(watched, key) }
	var output bytes.Buffer
	if err := restartClosedBindings(ctx, forwarder, desired, running, watch, &output); err != nil {
		t.Fatal(err)
	}
	if len(watched) != 2 || strings.Count(output.String(), "Reconnecting ") != 2 {
		t.Fatalf("one snapshot did not restart both ended sessions: watched=%v output=%q", watched, output.String())
	}
	for i, binding := range desired[:2] {
		replacement := running[bindingKey(binding)]
		if replacement.session == originals[i] || replacement.binding != binding || replacement.session.LocalPort != binding.Port {
			t.Fatalf("binding %s was not replaced in place: %+v", bindingKey(binding), replacement)
		}
		conn := assertPodEcho(t, replacement.session, "recovered-after-shared-outage")
		_ = conn.Close()
	}
	if running[bindingKey(desired[2])].session != originals[2] {
		t.Fatal("recovery replaced the unaffected session")
	}
	if err := healthy.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(healthy, "healthy-after-recovery\n"); err != nil {
		t.Fatal(err)
	}
	if reply, err := bufio.NewReader(healthy).ReadString('\n'); err != nil || reply != "healthy-after-recovery\n" {
		t.Fatalf("unaffected long-lived client did not survive recovery: reply=%q err=%v", reply, err)
	}
	// Already queued events carry the old pointer, so the event loop ignores
	// them. Repeating the same topology snapshot must also leave replacements up.
	if err := restartClosedBindings(ctx, forwarder, desired, running, watch, &output); err != nil {
		t.Fatal(err)
	}
	if len(watched) != 2 {
		t.Fatalf("healthy replacements restarted for stale events: %v", watched)
	}
}
