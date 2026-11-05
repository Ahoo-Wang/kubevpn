package localgateway

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func planTestService(namespace, name, ip string, ports ...corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID("service-" + namespace + "-" + name)},
		Spec:       corev1.ServiceSpec{ClusterIP: ip, Ports: ports},
	}
}

func planTestPod(namespace, name, ip, subdomain string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID("pod-" + namespace + "-" + name)},
		Spec:       corev1.PodSpec{Subdomain: subdomain},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip, PodIPs: []corev1.PodIP{{IP: ip}}},
	}
}

func planTestEndpoint(pod *corev1.Pod) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses:  []string{pod.Status.PodIP},
		Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
		TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
	}
}

func planTestSlice(svc *corev1.Service, name string, ports []discoveryv1.EndpointPort, endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Name: name, Namespace: svc.Namespace, Labels: map[string]string{discoveryv1.LabelServiceName: svc.Name}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       ports,
		Endpoints:   endpoints,
	}
}

func planTestTCPPort(name string, service, target int32) corev1.ServicePort {
	return corev1.ServicePort{Name: name, Protocol: corev1.ProtocolTCP, Port: service, TargetPort: intstr.FromInt32(target)}
}

func planTestEndpointPort(name string, port int32) discoveryv1.EndpointPort {
	return discoveryv1.EndpointPort{Name: ptr.To(name), Port: ptr.To(port), Protocol: ptr.To(corev1.ProtocolTCP)}
}

func planTestBindings(plan Plan) map[string]Binding {
	result := make(map[string]Binding)
	for _, binding := range plan.Bindings {
		result[fmt.Sprintf("%s:%d", binding.IP, binding.Port)] = binding
	}
	return result
}

// Real client Services select Pods whose DNS names belong to separate headless
// Services. Only the selected client ports should become local listeners.
func TestBuildPlanClientServicesAndPodDiscovery(t *testing.T) {
	ctx := context.Background()
	var objects []runtime.Object
	var refs []ServiceRef
	for index, spec := range []struct {
		name, headless string
		port, target   int32
		pods           int
	}{
		{"redis-dev", "redis-dev-headless", 6379, 6379, 3},
		{"kafka-dev", "kafka-dev-headless", 9092, 9092, 3},
		{"mongodb-dev-mongos", "", 27017, 27017, 3},
		{"dev-es-http", "dev-es-nodes", 9200, 9200, 3},
		{"platform-ui", "", 80, 8080, 1},
	} {
		svc := planTestService("dev", spec.name, fmt.Sprintf("10.0.0.%d", index+10), planTestTCPPort("client", spec.port, spec.target))
		refs = append(refs, ServiceRef{Namespace: svc.Namespace, Name: svc.Name})
		objects = append(objects, svc)
		var endpoints []discoveryv1.Endpoint
		for number := 0; number < spec.pods; number++ {
			pod := planTestPod("dev", fmt.Sprintf("%s-%d", spec.name, number), fmt.Sprintf("172.16.%d.%d", index+10, number+1), spec.headless)
			if spec.name == "kafka-dev" {
				pod.Spec.Hostname = fmt.Sprintf("broker-%d", number)
			}
			endpoints = append(endpoints, planTestEndpoint(pod))
			objects = append(objects, pod)
		}
		objects = append(objects, planTestSlice(svc, svc.Name+"-endpoints", []discoveryv1.EndpointPort{planTestEndpointPort("client", spec.target)}, endpoints...))
		if spec.headless != "" {
			// These control/cluster-bus ports must not leak into the client Service plan.
			headless := planTestService("dev", spec.headless, corev1.ClusterIPNone, planTestTCPPort("internal", 16379, 16379))
			objects = append(objects, headless, planTestSlice(headless, headless.Name+"-endpoints", []discoveryv1.EndpointPort{planTestEndpointPort("internal", 16379)}, endpoints...))
		}
	}
	client := fake.NewSimpleClientset(objects...)
	plan, err := BuildPlan(ctx, client, refs, "cluster.local")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Bindings) != 18 {
		t.Fatalf("got %d bindings, want 5 Service addresses plus 13 Pod addresses: %+v", len(plan.Bindings), plan.Bindings)
	}
	bindings := planTestBindings(plan)
	if got := bindings["10.0.0.14:80"]; got.PodPort != 8080 || got.Pod != "platform-ui-0" || got.PodUID != "pod-dev-platform-ui-0" {
		t.Fatalf("Service port mapping lost: %+v", got)
	}
	for _, binding := range plan.Bindings {
		if binding.Port == 16379 || binding.Port == 9093 || binding.Port == 9094 {
			t.Fatalf("unselected internal port forwarded: %+v", binding)
		}
	}
	for _, host := range []string{"redis-dev.dev.svc.cluster.local", "redis-dev-2.redis-dev-headless.dev.svc.cluster.local", "broker-2.kafka-dev-headless.dev.svc.cluster.local", "platform-ui.dev.svc.cluster.local"} {
		if !slices.Contains(plan.Hostnames, host) {
			t.Fatalf("missing full hostname %s: %v", host, plan.Hostnames)
		}
	}
	if slices.Contains(plan.Hostnames, "kafka-dev-2.kafka-dev-headless.dev.svc.cluster.local") {
		t.Fatal("ignored the Pod's explicit hostname")
	}
	podLists := 0
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("planner mutated source state: %v", action)
		}
		if action.Matches("list", "pods") {
			podLists++
		}
	}
	if podLists != 1 {
		t.Fatalf("Pod metadata was not cached per namespace: %d lists", podLists)
	}
	slices.Reverse(refs)
	refs = append(refs, refs[0])
	again, err := BuildPlan(ctx, client, refs, "cluster.local.")
	if err != nil || !reflect.DeepEqual(again, plan) {
		t.Fatalf("input order/duplicate references changed the plan: err=%v\n%+v\n%+v", err, plan, again)
	}
}

func TestBuildPlanHeadlessAndNamedPortRollout(t *testing.T) {
	ctx := context.Background()
	svc := planTestService("dev", "kafka-headless", corev1.ClusterIPNone,
		corev1.ServicePort{Name: "client", Port: 9092, TargetPort: intstr.FromString("client")},
		corev1.ServicePort{Name: "metrics", Port: 9000, TargetPort: intstr.FromString("metrics")},
	)
	oldPod := planTestPod("dev", "z-old", "172.16.1.2", svc.Name)
	newPod := planTestPod("dev", "a-new", "172.16.1.1", svc.Name)
	oldEndpoint, newEndpoint := planTestEndpoint(oldPod), planTestEndpoint(newPod)
	newEndpoint.Hostname = ptr.To("broker-new")
	newEndpoint.Conditions.Ready = ptr.To(false)
	oldSlice := planTestSlice(svc, "old-slice", []discoveryv1.EndpointPort{planTestEndpointPort("client", 9092), planTestEndpointPort("metrics", 9900)}, oldEndpoint)
	newSlice := planTestSlice(svc, "new-slice", []discoveryv1.EndpointPort{planTestEndpointPort("metrics", 9901), planTestEndpointPort("client", 9095)}, newEndpoint)
	client := fake.NewSimpleClientset(svc, oldPod, newPod, oldSlice, newSlice)
	refs := []ServiceRef{{Namespace: "dev", Name: svc.Name}}
	before, err := BuildPlan(ctx, client, refs, "corp.internal")
	if err != nil || len(before.Bindings) != 2 {
		t.Fatalf("unready replacement was selected: %v %+v", err, before)
	}
	newSlice.Endpoints[0].Conditions.Ready = nil // nil readiness means ready in the EndpointSlice API.
	if _, err := client.DiscoveryV1().EndpointSlices("dev").Update(ctx, newSlice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	ready, err := BuildPlan(ctx, client, refs, "corp.internal")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"172.16.1.1:9095", "172.16.1.1:9901", "172.16.1.2:9092", "172.16.1.2:9900"}
	bindings := planTestBindings(ready)
	if len(bindings) != len(want) {
		t.Fatalf("headless plan invented a VIP or lost a Pod port: %+v", ready)
	}
	for _, key := range want {
		if _, ok := bindings[key]; !ok {
			t.Fatalf("ports from different EndpointSlices were mixed: missing %s in %+v", key, bindings)
		}
	}
	for _, host := range []string{"kafka-headless.dev.svc.corp.internal", "a-new.kafka-headless.dev.svc.corp.internal", "broker-new.kafka-headless.dev.svc.corp.internal"} {
		if !slices.Contains(ready.Hostnames, host) {
			t.Fatalf("missing headless FQDN %s: %v", host, ready.Hostnames)
		}
	}
	// The same named target can use different ports during a rolling update.
	svc.Spec.ClusterIP = "10.0.0.20"
	if _, err := client.CoreV1().Services("dev").Update(ctx, svc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	normal, err := BuildPlan(ctx, client, refs, "corp.internal")
	if err != nil {
		t.Fatal(err)
	}
	if got := planTestBindings(normal)["10.0.0.20:9092"]; got.Pod != newPod.Name || got.PodPort != 9095 {
		t.Fatalf("Service candidate received another subset's port: %+v", got)
	}
	oldSlice.Endpoints[0].Conditions.Terminating = ptr.To(true)
	if _, err := client.DiscoveryV1().EndpointSlices("dev").Update(ctx, oldSlice, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	after, err := BuildPlan(ctx, client, refs, "corp.internal")
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Bindings) != 4 {
		t.Fatalf("terminating Pod retained: %+v", after.Bindings)
	}
	for _, binding := range after.Bindings {
		if binding.Pod == oldPod.Name {
			t.Fatalf("terminating Pod selected: %+v", binding)
		}
	}
}

func TestBuildPlanRejectsStaleAndNonPodEndpoints(t *testing.T) {
	for _, problem := range []string{"unready", "terminating", "external", "missing UID", "recreated Pod", "wrong IP", "pending Pod", "deleted Pod", "missing Pod"} {
		t.Run(problem, func(t *testing.T) {
			svc := planTestService("dev", "redis", "10.0.0.30", planTestTCPPort("client", 6379, 6379))
			pod := planTestPod("dev", "redis-0", "172.16.2.1", "redis-headless")
			endpoint := planTestEndpoint(pod)
			switch problem {
			case "unready":
				endpoint.Conditions.Ready = ptr.To(false)
			case "terminating":
				endpoint.Conditions.Terminating = ptr.To(true)
			case "external":
				endpoint.TargetRef = nil
			case "missing UID":
				endpoint.TargetRef.UID = ""
			case "recreated Pod":
				pod.UID = "replacement-uid"
			case "wrong IP":
				endpoint.Addresses = []string{"172.16.2.99"}
			case "pending Pod":
				pod.Status.Phase = corev1.PodPending
			case "deleted Pod":
				pod.DeletionTimestamp = ptr.To(metav1.Now())
			case "missing Pod":
				endpoint.TargetRef.Name = "not-present"
			}
			client := fake.NewSimpleClientset(svc, pod, planTestSlice(svc, "endpoints", []discoveryv1.EndpointPort{planTestEndpointPort("client", 6379)}, endpoint))
			plan, err := BuildPlan(context.Background(), client, []ServiceRef{{Namespace: "dev", Name: "redis"}}, "cluster.local")
			if err == nil || !strings.Contains(err.Error(), "no ready running Pod endpoints") || !reflect.DeepEqual(plan, Plan{}) {
				t.Fatalf("bad endpoint produced a plan: err=%v plan=%+v", err, plan)
			}
		})
	}
}

func TestBuildPlanPortsAreMatchedWithoutFallback(t *testing.T) {
	for _, problem := range []string{"wrong port name", "wrong numeric target", "unknown port", "UDP endpoint", "UDP service", "IPv6 endpoint", "IPv6 service"} {
		t.Run(problem, func(t *testing.T) {
			svc := planTestService("dev", "web", "10.0.0.40", planTestTCPPort("http", 80, 8080))
			pod := planTestPod("dev", "web-0", "172.16.3.1", "")
			epSlice := planTestSlice(svc, "endpoints", []discoveryv1.EndpointPort{planTestEndpointPort("http", 8080)}, planTestEndpoint(pod))
			switch problem {
			case "wrong port name":
				epSlice.Ports[0].Name = ptr.To("different")
			case "wrong numeric target":
				epSlice.Ports[0].Port = ptr.To(int32(8081))
			case "unknown port":
				epSlice.Ports[0].Port = nil
			case "UDP endpoint":
				epSlice.Ports[0].Protocol = ptr.To(corev1.ProtocolUDP)
			case "UDP service":
				svc.Spec.Ports[0].Protocol = corev1.ProtocolUDP
			case "IPv6 endpoint":
				epSlice.AddressType = discoveryv1.AddressTypeIPv6
				epSlice.Endpoints[0].Addresses = []string{"2001:db8::1"}
			case "IPv6 service":
				svc.Spec.ClusterIPs = []string{svc.Spec.ClusterIP, "2001:db8::2"}
			}
			client := fake.NewSimpleClientset(svc, pod, epSlice)
			plan, err := BuildPlan(context.Background(), client, []ServiceRef{{Namespace: "dev", Name: "web"}}, "cluster.local")
			if err == nil || !reflect.DeepEqual(plan, Plan{}) {
				t.Fatalf("unsupported/mismatched port produced a plan: err=%v plan=%+v", err, plan)
			}
		})
	}
}

func TestBuildPlanDeduplicatesSharedTargetsAndRejectsConflicts(t *testing.T) {
	pod := planTestPod("dev", "shared", "172.16.4.1", "")
	first := planTestService("dev", "first", "10.0.0.50", planTestTCPPort("client", 6379, 6379))
	second := planTestService("dev", "second", "10.0.0.50", planTestTCPPort("client", 6379, 6379))
	firstSlice := planTestSlice(first, "first-endpoints", []discoveryv1.EndpointPort{planTestEndpointPort("client", 6379)}, planTestEndpoint(pod))
	secondSlice := planTestSlice(second, "second-endpoints", firstSlice.Ports, planTestEndpoint(pod))
	client := fake.NewSimpleClientset(pod, first, second, firstSlice, secondSlice)
	refs := []ServiceRef{{Namespace: "dev", Name: first.Name}, {Namespace: "dev", Name: second.Name}}
	plan, err := BuildPlan(context.Background(), client, refs, "cluster.local")
	if err != nil || len(plan.Bindings) != 2 {
		t.Fatalf("identical shared targets were not deduplicated: err=%v %+v", err, plan)
	}
	other := planTestPod("prod", "other", "172.16.4.2", "")
	conflicting := planTestService("prod", "conflicting", first.Spec.ClusterIP, first.Spec.Ports...)
	conflictingSlice := planTestSlice(conflicting, "conflicting-endpoints", firstSlice.Ports, planTestEndpoint(other))
	for _, object := range []runtime.Object{other, conflicting, conflictingSlice} {
		if err := client.Tracker().Add(object); err != nil {
			t.Fatal(err)
		}
	}
	refs = append(refs, ServiceRef{Namespace: "prod", Name: conflicting.Name})
	plan, err = BuildPlan(context.Background(), client, refs, "cluster.local")
	if err == nil || !strings.Contains(err.Error(), "conflicting targets for 10.0.0.50:6379") || !reflect.DeepEqual(plan, Plan{}) {
		t.Fatalf("conflicting Service addresses published a partial plan: err=%v %+v", err, plan)
	}
}

func TestBuildPlanAPIErrorAndCancellationDoNotPublish(t *testing.T) {
	svc := planTestService("dev", "web", "10.0.0.60", planTestTCPPort("", 80, 8080))
	pod := planTestPod("dev", "web-0", "172.16.5.1", "")
	epSlice := planTestSlice(svc, "endpoints", []discoveryv1.EndpointPort{{Port: ptr.To(int32(8080))}}, planTestEndpoint(pod))
	client := fake.NewSimpleClientset(svc, pod, epSlice)
	refs := []ServiceRef{{Namespace: "dev", Name: "web"}}
	if _, err := BuildPlan(context.Background(), client, refs, ""); err != nil {
		t.Fatalf("unnamed/default TCP ports or default cluster domain rejected: %v", err)
	}
	plan, err := BuildPlan(context.Background(), client, append(refs, ServiceRef{Namespace: "dev", Name: "zz-missing"}), "cluster.local")
	if err == nil || !strings.Contains(err.Error(), "get service dev/zz-missing") || !reflect.DeepEqual(plan, Plan{}) {
		t.Fatalf("missing Service published earlier bindings: err=%v %+v", err, plan)
	}
	failure := errors.New("API unavailable")
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, failure })
	plan, err = BuildPlan(context.Background(), client, refs, "cluster.local")
	if !errors.Is(err, failure) || !reflect.DeepEqual(plan, Plan{}) {
		t.Fatalf("API error was lost: err=%v %+v", err, plan)
	}
	client.ClearActions()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	plan, err = BuildPlan(ctx, client, refs, "cluster.local")
	if !errors.Is(err, context.Canceled) || len(client.Actions()) != 0 || !reflect.DeepEqual(plan, Plan{}) {
		t.Fatalf("canceled discovery continued: err=%v actions=%v %+v", err, client.Actions(), plan)
	}
}

func TestBuildPlanRejectsReadyHostNetworkPodWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	first := planTestService("dev", "a-web", "10.0.0.70", planTestTCPPort("http", 80, 8080))
	firstPod := planTestPod("dev", "web-0", "172.16.6.1", "")
	firstSlice := planTestSlice(first, "web-endpoints", []discoveryv1.EndpointPort{planTestEndpointPort("http", 8080)}, planTestEndpoint(firstPod))
	hosted := planTestService("dev", "z-hosted", "10.0.0.71", planTestTCPPort("client", 6379, 6379))
	hostedPod := planTestPod("dev", "hosted-0", "192.0.2.20", "")
	hostedPod.Spec.HostNetwork = true
	hostedSlice := planTestSlice(hosted, "hosted-endpoints", []discoveryv1.EndpointPort{planTestEndpointPort("client", 6379)}, planTestEndpoint(hostedPod))
	client := fake.NewSimpleClientset(first, firstPod, firstSlice, hosted, hostedPod, hostedSlice)
	refs := []ServiceRef{{Namespace: "dev", Name: first.Name}, {Namespace: "dev", Name: hosted.Name}}
	plan, err := BuildPlan(ctx, client, refs, "cluster.local")
	if err == nil || !strings.Contains(err.Error(), "refusing host-network pod dev/hosted-0") || !reflect.DeepEqual(plan, Plan{}) {
		t.Fatalf("ready host-network Pod was not rejected before publishing: err=%v plan=%+v", err, plan)
	}
	// Readiness, identity and port pairing are valid. Changing only HostNetwork
	// allows the same fixture, proving the rejection came from its node IP scope.
	hostedPod.Spec.HostNetwork = false
	if _, err := client.CoreV1().Pods("dev").Update(ctx, hostedPod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	plan, err = BuildPlan(ctx, client, refs, "cluster.local")
	if err != nil || len(plan.Bindings) != 4 {
		t.Fatalf("otherwise valid Pod fixture rejected: err=%v plan=%+v", err, plan)
	}
	if got := planTestBindings(plan)["192.0.2.20:6379"]; got.Pod != hostedPod.Name {
		t.Fatalf("otherwise valid endpoint missing: %+v", got)
	}
}
