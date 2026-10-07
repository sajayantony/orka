# AgentSuite connector requirements

AgentSuite should describe what delegated user authorization an agent needs,
but it should not contain a person's mutable account link or any credential.
Orka can satisfy that portable requirement at deployment and dispatch time.

## Portable model

A connector-backed operation remains an ordinary Tool operation. Its portable
definition should carry:

- a stable provider or service requirement;
- the exact operation name, input schema, and read or consequential effect;
- the required delegated-user authorization capability;
- whether approval is required before a consequential call;
- a requirement that credential resolution and injection remain brokered; and
- an unresolved deployment binding slot.

The portable artifact must not carry OAuth client secrets, callback state,
consent state, access or refresh tokens, token expiry, revocation state, a
person's `Connection`, or a deployment-specific `Connection` UID.

This keeps authorization orthogonal to Tool transport. The operation can be an
HTTP Tool today or a remote MCP Tool later without creating a second Tool
taxonomy for connectors.

## Orka binding

Orka binds the portable requirement through the following controller-owned
chain:

1. The API authenticates a person and stores the verified issuer and subject
   in immutable `Task.spec.requestedBy`.
2. Requester provenance is sealed with a controller HMAC. A raw Kubernetes
   Task with copied requester fields is not trusted.
3. A `Connection` belongs to the same exact issuer and subject and references
   an operator-curated `ConnectorProvider`.
4. Dispatch freezes only the Connection identity, generation, grant sequence,
   mode, and policy identity into the encrypted Agent execution snapshot.
5. An agent calls a standard MCP Tool through the runtime's loopback MCP
   server. The supervisor sends Tool and Task authority to the controller MCP
   broker; it does not send the linked credential.
6. The broker reloads the authenticated Task, verifies requester provenance,
   opens the frozen snapshot, and rejects requester, policy, provider,
   generation, or grant drift.
7. The controller opens the sealed credential row and injects the bearer only
   into the curated outbound request. The runtime, agent process, Task, MCP
   payload, and execution snapshot never receive it.

Consequential operations continue through the existing approval and
external-effect ledger. The effect record can identify the Connection by a
non-secret digest for audit.

## Executable spike

`internal/controller/acp_mcp_connector_spike_test.go` exercises the real
controller MCP broker, custom Tool executor, outbound-access resolver,
encrypted SQLite connector custody, and a credential-protected local TLS
application.

The test proves that:

- an MCP request made for Alice contains no token;
- Alice's frozen Connection resolves to the sealed bearer at execution time;
- the protected application receives that bearer and returns its result
  through the MCP broker;
- changing the authenticated Task requester to Bob cannot use Alice's link;
- changing the live grant sequence makes the dispatch-time binding stale and
  fails before the protected application is called; and
- neither the MCP payload nor the execution snapshot contains the credential.

This is the complete controller trust path but not a full runtime-process
conformance test: the request is constructed at the MCP broker boundary rather
than emitted by a running ACP supervisor. A later Kind fixture should add a
deterministic built-in runtime and drive its loopback MCP proxy. External v2
runtime registrations currently do not expose curated connector HTTP Tools, so
that support decision should be explicit rather than silently widening the
runtime contract.
