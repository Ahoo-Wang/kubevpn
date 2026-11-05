package localgateway

import (
	"context"
	"fmt"
	"net/netip"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// ServiceRef identifies a Service in the connected cluster.
type ServiceRef struct {
	Namespace string
	Name      string
}

// Plan contains a complete, validated snapshot. Hostnames are only used to
// remove obsolete loopback hosts entries; DNS remains authoritative.
type Plan struct {
	Bindings  []Binding
	Hostnames []string
}

type planBindingKey struct {
	ip   string
	port int
}

// BuildPlan reads Service, EndpointSlice and Pod metadata without changing the
// cluster. Errors return no partial snapshot, so callers can retain their last
// working plan while discovery catches up with a rolling update.
func BuildPlan(ctx context.Context, client kubernetes.Interface, refs []ServiceRef, clusterDomain string) (Plan, error) {
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	if client == nil || len(refs) == 0 {
		return Plan{}, fmt.Errorf("a Kubernetes client and at least one service are required")
	}
	domain := strings.TrimSuffix(strings.TrimSpace(clusterDomain), ".")
	if domain == "" {
		domain = "cluster.local"
	}
	if problems := validation.IsDNS1123Subdomain(domain); len(problems) != 0 {
		return Plan{}, fmt.Errorf("invalid cluster domain %q: %s", clusterDomain, strings.Join(problems, "; "))
	}
	refs = append([]ServiceRef(nil), refs...)
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Namespace != refs[j].Namespace {
			return refs[i].Namespace < refs[j].Namespace
		}
		return refs[i].Name < refs[j].Name
	})
	bindings := make(map[planBindingKey]Binding)
	hostnames := make(map[string]struct{})
	podsByNamespace := make(map[string]map[string]*corev1.Pod)
	for i, ref := range refs {
		if err := ctx.Err(); err != nil {
			return Plan{}, err
		}
		if i > 0 && ref == refs[i-1] {
			continue
		}
		if len(validation.IsDNS1123Label(ref.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 {
			return Plan{}, fmt.Errorf("invalid service reference %q/%q", ref.Namespace, ref.Name)
		}
		svc, err := client.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return Plan{}, fmt.Errorf("get service %s/%s: %w", ref.Namespace, ref.Name, err)
		}
		slices, err := client.DiscoveryV1().EndpointSlices(ref.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels.Set{discoveryv1.LabelServiceName: ref.Name}.String(),
		})
		if err != nil {
			return Plan{}, fmt.Errorf("list endpoints for service %s/%s: %w", ref.Namespace, ref.Name, err)
		}
		pods, cached := podsByNamespace[ref.Namespace]
		if !cached {
			list, err := client.CoreV1().Pods(ref.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				return Plan{}, fmt.Errorf("list pods in namespace %s: %w", ref.Namespace, err)
			}
			pods = make(map[string]*corev1.Pod, len(list.Items))
			for p := range list.Items {
				pod := &list.Items[p]
				pods[pod.Name] = pod
			}
			podsByNamespace[ref.Namespace] = pods
		}
		if err := planService(svc, slices.Items, pods, domain, bindings, hostnames); err != nil {
			return Plan{}, fmt.Errorf("plan service %s/%s: %w", ref.Namespace, ref.Name, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	result := Plan{}
	for _, binding := range bindings {
		result.Bindings = append(result.Bindings, binding)
	}
	sort.Slice(result.Bindings, func(i, j int) bool {
		left, right := result.Bindings[i], result.Bindings[j]
		if left.IP != right.IP {
			return netip.MustParseAddr(left.IP).Less(netip.MustParseAddr(right.IP))
		}
		return left.Port < right.Port
	})
	for hostname := range hostnames {
		result.Hostnames = append(result.Hostnames, hostname)
	}
	sort.Strings(result.Hostnames)
	return result, nil
}

func planService(svc *corev1.Service, slices []discoveryv1.EndpointSlice, pods map[string]*corev1.Pod, domain string, bindings map[planBindingKey]Binding, hostnames map[string]struct{}) error {
	if svc.Spec.Type == corev1.ServiceTypeExternalName {
		return fmt.Errorf("ExternalName services have no supported Pod forwarding target")
	}
	headless := svc.Spec.ClusterIP == corev1.ClusterIPNone
	var clusterIPs []string
	if !headless {
		for _, ip := range append([]string{svc.Spec.ClusterIP}, svc.Spec.ClusterIPs...) {
			if ip == "" {
				continue
			}
			address, err := planIPv4(ip)
			if err != nil {
				return fmt.Errorf("service ClusterIP: %w", err)
			}
			clusterIPs = append(clusterIPs, address)
		}
		if len(clusterIPs) == 0 {
			return fmt.Errorf("service has no allocated IPv4 ClusterIP")
		}
	}
	for _, slice := range slices {
		if slice.AddressType != discoveryv1.AddressTypeIPv4 {
			return fmt.Errorf("endpoint slice %s uses %s addresses; only IPv4 is supported", slice.Name, slice.AddressType)
		}
	}
	hostnames[svc.Name+"."+svc.Namespace+".svc."+domain] = struct{}{}
	tcpPorts := 0
	for _, servicePort := range svc.Spec.Ports {
		if servicePort.Protocol != "" && servicePort.Protocol != corev1.ProtocolTCP {
			continue
		}
		tcpPorts++
		if servicePort.Port < 1 || servicePort.Port > 65535 {
			return fmt.Errorf("invalid TCP service port %d", servicePort.Port)
		}
		var candidates []Binding
		for _, slice := range slices {
			podPort, found, err := planEndpointPort(servicePort, slice.Ports)
			if err != nil {
				return fmt.Errorf("endpoint slice %s: %w", slice.Name, err)
			}
			if !found {
				continue
			}
			for _, endpoint := range slice.Endpoints {
				ref := endpoint.TargetRef
				if (endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready) || (endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating) ||
					ref == nil || ref.Kind != "Pod" || ref.Name == "" || ref.UID == "" || (ref.Namespace != "" && ref.Namespace != svc.Namespace) {
					continue
				}
				pod := pods[ref.Name]
				if pod == nil || pod.UID != ref.UID || pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
					continue
				}
				if pod.Spec.HostNetwork {
					return fmt.Errorf("refusing host-network pod %s/%s: node addresses cannot be captured", svc.Namespace, pod.Name)
				}
				for _, ip := range endpoint.Addresses {
					address, err := planIPv4(ip)
					if err != nil {
						return fmt.Errorf("pod %s/%s endpoint: %w", svc.Namespace, pod.Name, err)
					}
					if !planPodHasIP(pod, address) {
						continue
					}
					binding := Binding{IP: address, Port: podPort, Namespace: svc.Namespace, Pod: pod.Name, PodPort: podPort, PodUID: string(pod.UID)}
					if err := addPlanBinding(bindings, binding); err != nil {
						return err
					}
					candidates = append(candidates, binding)
					if pod.Spec.Subdomain != "" {
						hostname := pod.Spec.Hostname
						if hostname == "" {
							hostname = pod.Name
						}
						hostnames[hostname+"."+pod.Spec.Subdomain+"."+svc.Namespace+".svc."+domain] = struct{}{}
					}
					if headless {
						hostnames[pod.Name+"."+svc.Name+"."+svc.Namespace+".svc."+domain] = struct{}{}
						if endpoint.Hostname != nil && *endpoint.Hostname != "" {
							hostnames[*endpoint.Hostname+"."+svc.Name+"."+svc.Namespace+".svc."+domain] = struct{}{}
						}
					}
				}
			}
		}
		if len(candidates) == 0 {
			return fmt.Errorf("no ready running Pod endpoints for TCP port %d (%q); discovery may be updating", servicePort.Port, servicePort.Name)
		}
		sort.Slice(candidates, func(i, j int) bool {
			left, right := candidates[i], candidates[j]
			if left.Pod != right.Pod {
				return left.Pod < right.Pod
			}
			if left.IP != right.IP {
				return left.IP < right.IP
			}
			return left.PodPort < right.PodPort
		})
		for _, ip := range clusterIPs {
			binding := candidates[0]
			binding.IP, binding.Port = ip, int(servicePort.Port)
			if err := addPlanBinding(bindings, binding); err != nil {
				return err
			}
		}
	}
	if tcpPorts == 0 {
		return fmt.Errorf("service has no TCP ports")
	}
	return nil
}

func planEndpointPort(servicePort corev1.ServicePort, ports []discoveryv1.EndpointPort) (int, bool, error) {
	var selected int
	for _, port := range ports {
		name := ""
		if port.Name != nil {
			name = *port.Name
		}
		if name != servicePort.Name || (port.Protocol != nil && *port.Protocol != "" && *port.Protocol != corev1.ProtocolTCP) {
			continue
		}
		if port.Port == nil || *port.Port < 1 || *port.Port > 65535 {
			return 0, false, fmt.Errorf("no concrete TCP endpoint port for service port %q", servicePort.Name)
		}
		// Named targets may resolve to different ports in different slices during a rollout.
		if servicePort.TargetPort.Type == intstr.Int {
			expected := servicePort.TargetPort.IntVal
			if expected == 0 {
				expected = servicePort.Port
			}
			if expected != *port.Port {
				continue
			}
		}
		if selected != 0 && selected != int(*port.Port) {
			return 0, false, fmt.Errorf("ambiguous TCP endpoint ports for service port %q", servicePort.Name)
		}
		selected = int(*port.Port)
	}
	return selected, selected != 0, nil
}

func planIPv4(value string) (string, error) {
	ip, err := netip.ParseAddr(value)
	if err != nil || !ip.Is4() || ip.IsUnspecified() || ip.IsMulticast() {
		return "", fmt.Errorf("address %q is not a supported IPv4 address", value)
	}
	return ip.String(), nil
}

func planPodHasIP(pod *corev1.Pod, address string) bool {
	if pod.Status.PodIP == address {
		return true
	}
	for _, ip := range pod.Status.PodIPs {
		if ip.IP == address {
			return true
		}
	}
	return false
}

func addPlanBinding(bindings map[planBindingKey]Binding, binding Binding) error {
	key := planBindingKey{ip: binding.IP, port: binding.Port}
	if previous, ok := bindings[key]; ok && previous != binding {
		return fmt.Errorf("conflicting targets for %s:%d: %s/%s:%d (UID %s) and %s/%s:%d (UID %s)", binding.IP, binding.Port,
			previous.Namespace, previous.Pod, previous.PodPort, previous.PodUID, binding.Namespace, binding.Pod, binding.PodPort, binding.PodUID)
	}
	bindings[key] = binding
	return nil
}
