# Connector identity MCP spike

Date: 2026-10-07

## Session objective

Prove an end-to-end connector trust path in Orka where:

1. a verified person starts an Agent Task;
2. the agent makes an MCP Tool call;
3. Orka binds the call to that Task's requester identity;
4. the controller resolves the requester's linked credential;
5. a separate credential-protected local application receives the injected
   credential; and
6. the credential never enters the agent request, MCP payload, Task, execution
   snapshot, runtime environment, or logs.

The spike also evaluates which parts of this model belong in a portable
AgentSuite.

## Branch and worktree

- Fork: `sajayantony/orka`
- Worktree: `/Users/sajay/code/src/kaimahi-agents/orka-connector-spike`
- Branch: `spike/connector-identity-mcp-e2e`
- Base: `upstream/connectors-7-e2e`
- Base commit: `0be95db1`

The base branch contains the complete staged Orka connector implementation:
provider declarations, user consent, encrypted credential custody, credential
injection, controller-only execution, GitHub support, user surfaces, and the
existing native-worker live E2E.

## What was implemented

`internal/controller/acp_mcp_connector_spike_test.go` adds a focused executable
integration test using:

- a local TLS application that rejects requests without Alice's mock bearer;
- a verified, provenance-stamped Agent Task requested by Alice;
- a curated `ConnectorProvider`, `Connection`, `OutboundAccessPolicy`, and
  brokered custom `Tool`;
- real encrypted SQLite connector credential custody;
- an encrypted Agent execution snapshot containing only the frozen Connection
  identity;
- the real connector credential `Source`;
- the real outbound-access Kubernetes resolver;
- the real `RegistryACPMCPToolExecutor`; and
- the real `ACPMCPBroker` HTTP handler.

The simulated agent MCP request carries the runtime fence, Task identity,
operation identity, Tool descriptor, arguments, and prompt authorization. It
does not carry the linked credential.

## Identity and credential handoff

The proven path is:

```text
verified API caller
  -> Task.spec.requestedBy (issuer + subject)
  -> controller requester-provenance HMAC
  -> dispatch-time frozen Connection identity
  -> agent MCP tools/call
  -> ACP MCP broker authenticates Task and runtime authority
  -> controller verifies requester and frozen binding
  -> encrypted SQLite credential custody
  -> outbound-access resolver injects Authorization header
  -> credential-protected local TLS application
  -> Tool result returned through the MCP broker
```

`Task.spec.requestedBy` alone is not sufficient authority. Orka verifies a
controller-owned provenance stamp before connector use. The frozen execution
snapshot pins the Connection UID, generation, grant sequence, mode, and policy
identity without storing token material.

## Assertions

The spike verifies that:

- Alice's sealed bearer reaches the protected application;
- the application's JSON response returns through the MCP broker;
- the MCP request contains no bearer;
- the frozen execution snapshot contains no bearer;
- raw requester identity is not forwarded to the protected application;
- changing the authenticated Task requester to Bob cannot use Alice's
  Connection;
- changing the live Connection grant sequence makes the dispatch-time binding
  stale; and
- requester mismatch and stale binding fail before another application call.

## Validation

The following passed:

```bash
go test ./internal/controller \
  -run 'TestSpikeAgentMCPCallUsesRequestersSealedConnectorCredential$' \
  -count=1

go test ./internal/outboundaccess \
  ./internal/connectors/credential \
  ./internal/store/sqlite \
  -count=1

KUBEBUILDER_ASSETS="$(pwd)/$(./bin/setup-envtest use 1.37 \
  --bin-dir ./bin -p path)" \
  go test ./internal/controller -count=1
```

The full controller package passed in approximately 167 seconds after the
repository's envtest binaries were installed.

## AgentSuite result

AgentSuite should specify the portable connector requirement, not a live
account link. A connector-backed operation remains an ordinary Tool operation
with:

- a stable provider or service requirement;
- an exact operation name and input schema;
- a read-only or consequential effect;
- a required delegated-user authorization capability;
- an approval requirement for consequential operations;
- a requirement for brokered credential resolution and injection; and
- an unresolved deployment binding slot.

AgentSuite must not contain OAuth client secrets, callback or consent state,
access or refresh tokens, expiry or revocation state, a person's mutable
`Connection`, or a deployment-specific Connection UID.

The detailed portable model is in
[`../agentsuite-connectors.md`](../agentsuite-connectors.md).

## Remaining limitation

This spike executes the complete controller trust path but constructs the
request at the controller MCP broker boundary. It does not start a separate ACP
supervisor process and drive the supervisor's loopback MCP proxy.

A later Kind E2E can add a deterministic built-in AgentRuntime and verify that
additional transport hop. External v2 AgentRuntime registrations currently do
not expose curated connector HTTP Tools, so extending that contract should be
an explicit product and security decision rather than an incidental part of
this spike.
