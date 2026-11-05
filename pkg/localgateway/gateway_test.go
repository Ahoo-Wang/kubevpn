//go:build darwin

package localgateway

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGatewayPreCanceledActivationHasNoSystemEffects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stateDir := filepath.Join(t.TempDir(), "uncreated-state")
	// A nil REST config also prevents any client or filesystem construction if
	// this cancellation guard regresses on a privileged CI worker.
	err := RunGateway(ctx, nil, GatewayConfig{StateDir: stateDir}, io.Discard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled activation returned %v", err)
	}
	if _, err := os.Stat(stateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("activation created state before cancellation: %v", err)
	}
}

func TestGatewayControlAddressesCannotBecomeDataBindings(t *testing.T) {
	plan := Plan{Bindings: []Binding{
		{IP: "10.42.1.20", Port: 6379},
		{IP: "192.0.2.10", Port: 80},
	}}
	for _, apiIP := range []net.IP{net.ParseIP("192.0.2.10"), net.ParseIP("::ffff:192.0.2.10")} {
		if err := checkControlIPs(plan, []net.IP{net.ParseIP("203.0.113.10"), apiIP}); err == nil {
			t.Fatalf("accepted data capture of API address %s", apiIP)
		}
	}
	if err := checkControlIPs(plan, []net.IP{net.ParseIP("203.0.113.10")}); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayAPILiteralAddressesNeedNoDNS(t *testing.T) {
	for _, tc := range []struct{ url, ip string }{
		{"https://192.0.2.10:6443", "192.0.2.10"},
		{"https://[2001:db8::10]:6443", "2001:db8::10"},
		{"https://[::ffff:192.0.2.10]:6443", "192.0.2.10"},
	} {
		// A canceled context prevents a resolver lookup from succeeding. Numeric
		// addresses should be returned directly for the control-address guard.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		got, err := apiServerIPs(ctx, tc.url)
		if err != nil || len(got) != 1 || !got[0].Equal(net.ParseIP(tc.ip)) {
			t.Fatalf("%s: got %v, %v", tc.url, got, err)
		}
	}
}

func TestGatewayRejectsInvalidAPIURLsBeforeResolution(t *testing.T) {
	for _, value := range []string{"ftp://192.0.2.10", "https:///missing-host", "https://user:password@192.0.2.10:6443", "://bad", "https://:6443"} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := apiServerIPs(ctx, value)
		if err == nil {
			t.Fatalf("accepted API URL %q", value)
		}
		if errors.Is(err, context.Canceled) {
			t.Fatalf("invalid URL %q reached DNS: %v", value, err)
		}
		if value == "https://:6443" && !strings.Contains(err.Error(), "invalid API server URL") {
			t.Fatalf("empty hostname reached resolution: %v", err)
		}
	}
}

func TestGatewayDNSSelectionTracksReadyTCP53Pod(t *testing.T) {
	svc := planTestService("kube-system", "kube-dns", "10.0.0.10",
		corev1.ServicePort{Name: "dns-udp", Port: 53, Protocol: corev1.ProtocolUDP},
		planTestTCPPort("metrics", 9153, 9153),
		planTestTCPPort("dns-tcp", 53, 53),
	)
	pod := planTestPod(svc.Namespace, "dns-a", "172.16.0.10", "")
	slice := planTestSlice(svc, "dns-endpoints", []discoveryv1.EndpointPort{
		{Name: &svc.Spec.Ports[0].Name, Port: &svc.Spec.Ports[0].Port, Protocol: &svc.Spec.Ports[0].Protocol},
		planTestEndpointPort("metrics", 9153), planTestEndpointPort("dns-tcp", 53),
	}, planTestEndpoint(pod))
	client := fake.NewSimpleClientset(svc, pod, slice)
	config := GatewayConfig{DNSService: ServiceRef{Namespace: svc.Namespace, Name: svc.Name}, ClusterDomain: "cluster.local"}
	assertBinding := func(wantUID string) {
		t.Helper()
		client.ClearActions()
		got, err := findDNSBinding(context.Background(), client, config)
		if err != nil {
			t.Fatal(err)
		}
		if got.IP != "127.0.0.1" || got.Port != 0 || got.PodPort != 53 || got.Pod != pod.Name || got.PodUID != wantUID || got.Namespace != svc.Namespace {
			t.Fatalf("unexpected private DNS binding: %+v", got)
		}
		for _, action := range client.Actions() {
			if action.GetVerb() != "get" && action.GetVerb() != "list" {
				t.Fatalf("discovery mutated state: %v", action)
			}
		}
	}
	assertBinding(string(pod.UID))
	// Recreated DNS Pods must supply their new identity and endpoint address.
	pod.UID = "replacement-dns-pod"
	pod.Status.PodIP = "172.16.0.11"
	pod.Status.PodIPs = []corev1.PodIP{{IP: pod.Status.PodIP}}
	if _, err := client.CoreV1().Pods(pod.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	slice.Endpoints = []discoveryv1.Endpoint{planTestEndpoint(pod)}
	if _, err := client.DiscoveryV1().EndpointSlices(slice.Namespace).Update(context.Background(), slice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	assertBinding(string(pod.UID))
	svc.Spec.Ports = svc.Spec.Ports[:2]
	if _, err := client.CoreV1().Services(svc.Namespace).Update(context.Background(), svc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := findDNSBinding(context.Background(), client, config); err == nil || !strings.Contains(err.Error(), "no ready TCP port 53") {
		t.Fatalf("DNS selected metrics or UDP after TCP53 removal: %v", err)
	}
}

func TestGatewayCanceledDNSDiscoveryMakesNoAPIRequests(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := findDNSBinding(ctx, client, GatewayConfig{DNSService: ServiceRef{Namespace: "kube-system", Name: "kube-dns"}, ClusterDomain: "cluster.local"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("canceled discovery issued API requests: %v", client.Actions())
	}
}
