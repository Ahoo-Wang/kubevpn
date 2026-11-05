# Compare routing-table and socket source selection

A route through the expected TUN interface is necessary, but does not prove that
an application uses that path. On macOS, Network Extension policies can scope
connections differently from the ordinary routing table. A stale route source
after a TUN address change is another, separate failure mode; see
[the existing route-source repair](52-stale-route-src-after-ip-change.md).

## Independent, read-only diagnostic tool

Build and run the diagnostic separately from the installed CLI:

```sh
go build -o bin/kubevpn-routecheck ./tools/routecheck
./bin/kubevpn-routecheck 10.42.0.15
./bin/kubevpn-routecheck --port 80 --timeout 3s 10.42.0.15
```

The destination must be a numeric IPv4 or IPv6 address. Hostnames are deliberately
not resolved, so a hosts override or a DNS problem cannot be confused with the
IP-path check. Scoped IPv6 literals are rejected because the routing lookup API
does not accept an interface zone. Flags precede the destination.

Without `--port`, the tool reads the route and local interface addresses, connects
an unbound UDP socket to destination port 9, reads its local address, and closes
it **without writing a datagram**. The socket connect asks the kernel to select a
source address; it does not test remote reachability.

With `--port`, it additionally attempts one TCP handshake and closes the socket
without sending application data. This is an active connectivity probe; use a
port you are authorized to test. The timeout applies to each socket probe.

The tool imports only the independent `pkg/routeprobe` package, standard-library
packages, and the existing route-table reader. It does not import kubevpn's
configuration or daemon packages, initialize a Kubernetes client, restart or
upgrade a daemon, or change interfaces, routes, DNS, or hosts. Existing CLI and
connection status behavior are unchanged.

## Interpret observations separately

- **Stored route source absent from the route interface:** inspect stale routes
  after an interface address change. This is different from a policy override.
- **Kernel-selected socket source absent from the route interface:** a policy or
  source-selection rule differs from the ordinary routing-table observation.
  This does not identify a particular VPN as the cause.
- **Different IPv6 source values on the same interface:** not necessarily a
  problem. A route may record its interface's link-local address while a socket
  selects a global address. Membership in the interface, rather than equality of
  the two values, determines the warnings.
- **A source address belongs to multiple interfaces:** all owners are reported;
  ownership by the route interface is sufficient, without choosing the first
  interface from an enumeration.
- **UDP probe success:** source selection only. Use an authorized TCP probe and,
  if necessary, a narrowly filtered packet capture to determine packet egress.
- **TCP handshake success:** transport connectivity only. It does not establish
  that authentication, TLS, service discovery, or a middleware operation works.

Warnings do not automatically repair anything and do not cause a nonzero exit
status. Invalid arguments, failed observations, and failed TCP handshakes return
a nonzero status. Failed observations retain any route or socket observations already obtained,
but do not classify incomplete interface ownership checks as a source mismatch.

## Regression coverage

```sh
go test ./pkg/routeprobe ./tools/routecheck
go vet ./pkg/routeprobe ./tools/routecheck
go build ./...
```

The tests require no kubeconfig, root TUN, or live cluster. They cover IPv4 and
IPv6 source ownership, cancellation and partial failures, socket closure, a real
loopback UDP listener receiving no datagram, and a complete loopback TCP
handshake that receives no application bytes. The diagnostic path remains
separate from the regular connection and route-management lifecycle.
