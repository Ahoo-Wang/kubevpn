# macOS API gateway

This opt-in foreground mode provides IPv4 TCP access to selected Kubernetes
Services and their ready Pods. Applications keep their Service FQDNs and actual
node addresses, including Redis Cluster redirects and Kafka broker metadata.

The gateway assigns selected cluster IPv4 addresses to lo0 as /32 aliases. Local
TCP listeners forward through authenticated Kubernetes API port-forward sessions.
The business listeners accept connections from local interface addresses. Each
API transport listener uses a private 127.0.0.1 port. TLS, authentication and API
proxy settings come from the existing kubeconfig.

A shared UDP/TCP loopback DNS listener forwards cluster-domain questions through
an API TCP channel to CoreDNS. Each selected Service and Pod FQDN receives its own
macOS resolver file. The longest matching domain selects this listener alongside
existing kubevpn search-domain resolvers. Discovery reconciles these FQDN files
and detects external edits to files owned by this session.
Selected FQDN tokens pointing to old IPv4 loopback entries are temporarily removed
from hosts. Cluster DNS supplies the real Service and Pod addresses.

## Preview and run

```sh
go build -o bin/kubevpn-cluster-gateway ./tools/cluster-gateway
./bin/kubevpn-cluster-gateway --context my-cluster --services dev/api,dev/redis
sudo ./bin/kubevpn-cluster-gateway --apply --kubeconfig "$HOME/.kube/config"   --context my-cluster --services dev/api,dev/redis   --state-dir /private/tmp/kubevpn-gateway-session
```

The default command prints a metadata plan. Activation requires `--apply`, an
administrator session and a private state directory. Keep the foreground session
running. Ctrl-C, SIGTERM and terminal hangup initiate recovery.

The cluster DNS Service defaults to `kube-system/kube-dns`. Its TCP port 53 must
be reachable through the API. The initial DNS check queries the Kubernetes
Service before applying the local resolver configuration. The kubeconfig identity
needs read access to selected Services, EndpointSlices and Pods, plus Pod
port-forward access in those namespaces and the DNS namespace.

## Discovery and recovery

Metadata refreshes every five seconds. Endpoint ports stay paired with their own
Pod identities. UID changes replace affected listeners. Added and removed FQDNs
acquire and restore their own resolver files. DNS refresh checks both
its backend identity and an actual query, then reconnects on the existing private
port. Three consecutive metadata or DNS refresh failures withdraw the gateway
and initiate recovery.

Aliases use per-address references across bindings. Existing local addresses,
host-network Pods, API server addresses, ambiguous IP/port mappings and IPv6
endpoints produce errors. Each alias claims its entire IP address. Forwarding
covers the selected Service TCP client ports. Pod and Service address ranges
must belong to one cluster with distinct address identities.

The private journal records initial files, planned file contents, confirmed
aliases and incomplete operations. Recovery preserves concurrent external file
edits. An incomplete address acquisition stays pending for inspection. Successful
cleanup removes the journal after restoring owned files and addresses.

After an unclean process exit, explicitly recover the recorded session before
starting another one in the same state directory.

```sh
sudo ./bin/kubevpn-cluster-gateway --cleanup   --state-dir /private/tmp/kubevpn-gateway-session
```

Keep the journal when recovery reports a changed resolver or uncertain address.
Those entries require inspection before further cleanup. Journal recovery is
limited to confirmed addresses still represented as lo0 /32 aliases.

## Validation

```sh
go test -race ./pkg/localgateway ./tools/cluster-gateway
go vet ./pkg/localgateway ./tools/cluster-gateway
go build ./...
```

The suite uses fake Kubernetes metadata, temporary files and real local
HTTP/SPDY and DNS servers. It covers endpoint identity and port pairing,
concurrent streams, source restrictions, TLS and authentication, startup and
active cancellation, half-close, DNS scope and truncation, file ownership,
partial cleanup, external edits and explicit journal recovery.

Real deployment acceptance also includes Service IPs with preexisting host routes,
remote application responses, FQDN lookups and a full startup/shutdown recovery
comparison. Middleware authentication and application operations remain separate
acceptance checks using the application's configured credentials.
