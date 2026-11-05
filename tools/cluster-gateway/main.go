//go:build darwin

// cluster-gateway is an opt-in macOS TCP and cluster DNS gateway. It deliberately
// does not use the installed kubevpn daemon's startup or upgrade paths.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/wencaiwulue/kubevpn/v2/pkg/localgateway"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "cluster-gateway:", err)
		os.Exit(1)
	}
}
func run(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("cluster-gateway", flag.ContinueOnError)
	flags.SetOutput(out)
	kubeconfig := flags.String("kubeconfig", "", "path to the existing kubeconfig")
	contextName := flags.String("context", "", "kubeconfig context")
	services := flags.String("services", "", "comma-separated namespace/service client Services")
	dnsService := flags.String("dns-service", "kube-system/kube-dns", "cluster DNS Service with TCP port 53")
	domain := flags.String("cluster-domain", "cluster.local", "cluster DNS suffix")
	stateDir := flags.String("state-dir", "", "private session state directory; required for apply/cleanup")
	apply := flags.Bool("apply", false, "activate local aliases and scoped DNS; otherwise only print the plan")
	cleanup := flags.Bool("cleanup", false, "explicitly restore a previous session's recovery journal")
	refresh := flags.Duration("refresh", 5*time.Second, "metadata refresh interval")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *apply && *cleanup {
		return errors.New("apply and cleanup are mutually exclusive")
	}
	if *cleanup {
		if os.Geteuid() != 0 {
			return errors.New("cleanup requires administrator authentication")
		}
		if *stateDir == "" {
			return errors.New("state-dir is required")
		}
		state, err := localgateway.RecoverSystemState(*stateDir)
		if err != nil {
			return err
		}
		defer state.Close()
		owned, pending := state.RecordedAliases()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		err = errors.Join(state.Restore(), localgateway.RecoverAliases(cleanupCtx, owned), state.PruneAbsentPending())
		_, pending = state.RecordedAliases()
		if len(pending) > 0 {
			err = errors.Join(err, fmt.Errorf("unconfirmed addresses need inspection; not automatically removed: %v", pending))
		}
		if err != nil {
			return err
		}
		return state.Finish()
	}
	refs, err := parseServices(*services)
	if err != nil {
		return err
	}
	dnsRefs, err := parseServices(*dnsService)
	if err != nil || len(dnsRefs) != 1 {
		return errors.New("dns-service must be one namespace/service")
	}
	if *refresh < time.Second {
		return errors.New("refresh must be at least one second")
	}
	if *apply && (*stateDir == "" || os.Geteuid() != 0) {
		return errors.New("apply requires a state-dir and administrator authentication")
	}
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	if *kubeconfig != "" {
		loading.ExplicitPath = *kubeconfig
	}
	override := &clientcmd.ConfigOverrides{CurrentContext: *contextName}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, override).ClientConfig()
	if err != nil {
		return err
	}
	config.Timeout = 10 * time.Second
	if !*apply {
		client, err := kubernetes.NewForConfig(config)
		if err != nil {
			return err
		}
		child, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		plan, err := localgateway.BuildPlan(child, client, refs, *domain)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(plan)
	}
	return localgateway.RunGateway(ctx, config, localgateway.GatewayConfig{Services: refs, DNSService: dnsRefs[0], ClusterDomain: *domain, StateDir: *stateDir, Refresh: *refresh}, out)
}
func parseServices(value string) ([]localgateway.ServiceRef, error) {
	if strings.TrimSpace(value) == "" {
		return nil, errors.New("at least one namespace/service is required")
	}
	var refs []localgateway.ServiceRef
	for _, part := range strings.Split(value, ",") {
		fields := strings.Split(strings.TrimSpace(part), "/")
		if len(fields) != 2 || fields[0] == "" || fields[1] == "" {
			return nil, fmt.Errorf("invalid service reference %q", part)
		}
		refs = append(refs, localgateway.ServiceRef{Namespace: fields[0], Name: fields[1]})
	}
	return refs, nil
}
