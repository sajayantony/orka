package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/connectors"
	"github.com/orka-agents/orka/internal/connectors/credential"
	harnessv2 "github.com/orka-agents/orka/internal/harness/v2"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/outboundaccess"
	"github.com/orka-agents/orka/internal/store"
	storesqlite "github.com/orka-agents/orka/internal/store/sqlite"
	workerexecutor "github.com/orka-agents/orka/internal/worker"
)

func TestSpikeAgentMCPCallUsesRequestersSealedConnectorCredential(t *testing.T) {
	const (
		namespace   = "default"
		toolName    = "identityread"
		policyName  = "local-app-as-me"
		provider    = "local-app"
		accessToken = "local-app-alice-token"
		issuer      = "https://issuer.example.test"
		subject     = "alice"
	)

	connectors.SetAllowPrivateEndpoints(true)
	workerexecutor.SetAllowPrivateConnectionEndpoints(true)
	t.Cleanup(func() {
		connectors.SetAllowPrivateEndpoints(false)
		workerexecutor.SetAllowPrivateConnectionEndpoints(false)
	})

	var appCalls atomic.Int32
	app := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appCalls.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer "+accessToken {
			t.Errorf("credentialed app Authorization = %q", got)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var input struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Errorf("decode credentialed app request: %v", err)
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		if input.Query != "whoami" {
			t.Errorf("credentialed app query = %q", input.Query)
			http.Error(w, "bad query", http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-Orka-Requester") != "" {
			t.Error("raw requester identity must not be forwarded to the credentialed app")
			http.Error(w, "identity leaked", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"account":"alice","source":"sealed-connection"}`))
	}))
	defer app.Close()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	request, profile := testMCPBrokerRequest(t, harnessv2.MCPToolEffectReadOnly)
	request.Call.ToolName = toolName
	request.Call.Arguments = json.RawMessage(`{"query":"whoami"}`)
	request.Metadata.OperationID = "connector-identity-read"

	parameters := &apiextensionsv1.JSON{Raw: json.RawMessage(
		`{"type":"object","required":["query"],"properties":{"query":{"type":"string"}},"additionalProperties":false}`,
	)}
	tool := &corev1alpha1.Tool{
		ObjectMeta: metav1.ObjectMeta{Name: toolName, Namespace: namespace, UID: "tool-uid", Generation: 1},
		Spec: corev1alpha1.ToolSpec{
			Description:       "Read the account selected by the requester's linked identity",
			BrokeredToolClass: corev1alpha1.AgentRuntimeBrokeredToolClassRead,
			Parameters:        parameters,
			HTTP: &corev1alpha1.HTTPExecution{
				URL: app.URL, Method: http.MethodGet,
				OutboundAccessPolicyRef: &corev1alpha1.LocalObjectReference{Name: policyName},
			},
		},
	}
	descriptor, err := customACPMCPToolDescriptor(tool)
	if err != nil {
		t.Fatal(err)
	}
	request.Authorization.ToolPolicy.AllowedToolNames = []string{toolName}
	request.Authorization.ToolPolicy.Tools = []harnessv2.MCPToolDescriptor{descriptor}
	request.Authorization.ToolPolicy.DescriptorDigest, err = harnessv2.CanonicalMCPToolDescriptorDigest(request.Authorization.ToolPolicy.Tools)
	if err != nil {
		t.Fatal(err)
	}
	profile.ToolPolicyDigest, err = harnessv2.CanonicalRuntimeToolPolicyDigest([]string{toolName}, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	profile.MCPConfigurationDigest, err = harnessv2.CanonicalMCPConfigurationDigest([]string{toolName})
	if err != nil {
		t.Fatal(err)
	}
	request.Authorization.ToolPolicyDigest = profile.ToolPolicyDigest
	request.Authorization.MCPConfigurationDigest = profile.MCPConfigurationDigest
	request.Metadata.RequestDigest, err = harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}

	connectorProvider := &corev1alpha1.ConnectorProvider{
		ObjectMeta: metav1.ObjectMeta{Name: provider, Namespace: namespace, UID: "provider-uid", Generation: 1},
		Spec: corev1alpha1.ConnectorProviderSpec{
			OAuth: corev1alpha1.ConnectorOAuthConfig{
				AuthorizeURL: "https://auth.example.test/authorize",
				TokenURL:     "https://auth.example.test/token",
				ClientID:     "local-app-client",
				ClientSecretRef: corev1alpha1.SecretKeySelector{
					Name: "local-app-oauth", Key: "client-secret",
				},
			},
			Tools: []corev1alpha1.ConnectorTool{{
				Name: toolName, Class: corev1alpha1.ConnectorToolClassRead,
				Source:      corev1alpha1.ConnectorToolSourceHTTP,
				Description: tool.Spec.Description, Parameters: parameters.DeepCopy(),
				HTTP: &corev1alpha1.ConnectorHTTPTool{URL: app.URL, Method: http.MethodGet},
			}},
		},
		Status: corev1alpha1.ConnectorProviderStatus{
			ObservedGeneration: 1,
			Conditions: []metav1.Condition{
				{Type: corev1alpha1.ConnectorProviderConditionAccepted, Status: metav1.ConditionTrue, ObservedGeneration: 1},
				{Type: corev1alpha1.ConnectorProviderConditionResolvedRefs, Status: metav1.ConditionTrue, ObservedGeneration: 1},
			},
		},
	}
	policy := &corev1alpha1.OutboundAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: namespace, UID: "policy-uid", Generation: 1},
		Spec: corev1alpha1.OutboundAccessPolicySpec{
			Connection: &corev1alpha1.ConnectionOutboundAccess{
				ProviderRef: corev1alpha1.LocalObjectReference{Name: provider},
			},
		},
		Status: corev1alpha1.OutboundAccessPolicyStatus{
			ObservedGeneration: 1,
			Conditions: []metav1.Condition{
				{Type: corev1alpha1.OutboundAccessPolicyConditionAccepted, Status: metav1.ConditionTrue, ObservedGeneration: 1},
				{Type: corev1alpha1.OutboundAccessPolicyConditionResolvedRefs, Status: metav1.ConditionTrue, ObservedGeneration: 1},
			},
		},
	}
	requester := &corev1alpha1.RequestedBy{Issuer: issuer, Subject: subject}
	connection := &corev1alpha1.Connection{
		ObjectMeta: metav1.ObjectMeta{
			Name: connectors.ConnectionName(provider, issuer, subject), Namespace: namespace,
			UID: "connection-uid", Generation: 1,
		},
		Spec: corev1alpha1.ConnectionSpec{
			Subject:     corev1alpha1.ConnectionSubject{Issuer: issuer, Subject: subject},
			ProviderRef: corev1alpha1.LocalObjectReference{Name: provider},
			Mode:        corev1alpha1.ConnectionModeReadOnly,
		},
	}

	controlStore, controllerFence := newMCPBrokerControlStore(t)
	cipher, err := storesqlite.NewAgentExecutionSnapshotCipher(bytes.Repeat([]byte{0x73}, storesqlite.AgentExecutionSnapshotKeyBytes))
	if err != nil {
		t.Fatal(err)
	}
	if err := controlStore.SetAgentExecutionSnapshotCipher(cipher); err != nil {
		t.Fatal(err)
	}
	ref, err := connectors.CredentialRef(connection)
	if err != nil {
		t.Fatal(err)
	}
	if err := controlStore.PutConnectorCredential(context.Background(), ref, store.ConnectorCredential{
		AccessToken: accessToken, TokenType: "Bearer", ExpiresAt: time.Now().Add(time.Hour),
		Scopes: []string{"identity:read"}, AuthorityDigest: connectors.ProviderIssuerDigest(connectorProvider),
	}); err != nil {
		t.Fatal(err)
	}
	held, err := controlStore.GetConnectorCredential(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	connection.Status = corev1alpha1.ConnectionStatus{
		State: corev1alpha1.ConnectionStateReady, GrantSequence: held.GrantSequence,
		Consent: connectors.ConsentFor(connectorProvider),
		Conditions: []metav1.Condition{
			{Type: corev1alpha1.ConnectionConditionReady, Status: metav1.ConditionTrue, Reason: corev1alpha1.ConnectionReasonLinked, ObservedGeneration: 1},
			{Type: corev1alpha1.ConnectionConditionScopesGranted, Status: metav1.ConditionTrue, Reason: corev1alpha1.ConnectionReasonScopesGranted, ObservedGeneration: 1},
			{Type: corev1alpha1.ConnectionConditionProviderResolved, Status: metav1.ConditionTrue, Reason: corev1alpha1.ConnectionReasonProviderResolved, ObservedGeneration: 1},
		},
	}

	stampKey := bytes.Repeat([]byte{0x41}, 32)
	SetRequesterStampKey(stampKey)
	t.Cleanup(func() { SetRequesterStampKey(nil) })
	task := &corev1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name: acpDispatcherTestTaskName, Namespace: namespace, UID: types.UID(request.Metadata.TaskUID),
			Annotations: map[string]string{
				labels.AnnotationRequestedBySource: labels.RequestedBySourceAPI,
				labels.AnnotationRequestedByStamp: connectors.RequesterStamp(
					stampKey, types.UID(request.Metadata.TaskUID), issuer, subject,
				),
			},
		},
		Spec: corev1alpha1.TaskSpec{Type: corev1alpha1.TaskTypeAgent, RequestedBy: requester},
		Status: corev1alpha1.TaskStatus{
			AgentExecutionBinding: &corev1alpha1.AgentExecutionBinding{
				Snapshot: corev1alpha1.AgentExecutionSnapshotRef{Digest: "snapshot-digest"},
			},
		},
	}
	snapshotBody, err := json.Marshal(agentExecutionSnapshotBody{Connections: []agentExecutionSnapshotConnection{{
		PolicyName: policyName, Provider: provider, ConnectionName: connection.Name,
		UID: string(connection.UID), Generation: connection.Generation,
		GrantSequence: held.GrantSequence, Mode: connection.Spec.Mode,
		PolicyUID: string(policy.UID), PolicyGeneration: policy.Generation,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(snapshotBody, []byte(accessToken)) {
		t.Fatal("the frozen execution snapshot must identify the Connection without containing its credential")
	}
	client := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(
		connectorProvider, policy, tool, connection, task,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "local-app-oauth", Namespace: namespace},
			Data:       map[string][]byte{"client-secret": []byte("not-used-by-the-tool-call")},
		},
	).Build()
	source := &credential.Source{
		Client: client, APIReader: client, Credentials: controlStore,
		Now: func() time.Time { return time.Now().UTC() },
	}
	resolver := &outboundaccess.KubernetesResolver{Reader: client, Connections: source}
	executor := RegistryACPMCPToolExecutor{
		Reader: client, KubeClient: k8sfake.NewSimpleClientset(), HTTPClient: app.Client(),
		OutboundAccess:          resolver,
		AgentExecutionSnapshots: fakeSnapshotStore{snapshot: &store.AgentExecutionSnapshot{Body: snapshotBody}},
	}

	bearer := strings.Repeat("b", 32)
	capability := []byte(strings.Repeat("c", 32))
	broker := &ACPMCPBroker{
		Credentials: ACPMCPBrokerCredentialResolverFunc(func(_ context.Context, got harnessv2.MCPBrokerCallRequest) (ACPMCPBrokerCredentials, error) {
			return ACPMCPBrokerCredentials{
				ControllerBearerToken: bearer, CapabilitySecret: capability,
				ExpectedFence: got.Metadata.Fence, RuntimeProfile: profile,
				ControllerFence: controllerFence,
				Task: ACPMCPAuthenticatedTask{
					Name: task.Name, Namespace: task.Namespace, UID: string(task.UID),
				},
			}, nil
		}),
		Prompts:  ACPMCPPromptAuthorizerFunc(func(context.Context, harnessv2.MCPBrokerCallRequest) error { return nil }),
		Executor: executor, Effects: controlStore,
	}

	encodedRequest, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encodedRequest, []byte(accessToken)) {
		t.Fatal("the agent-to-MCP request must not contain the linked credential")
	}
	response := performMCPBrokerCall(t, broker, request, bearer, capability)
	if response.Code != http.StatusOK {
		t.Fatalf("MCP broker status = %d body = %s", response.Code, response.Body.String())
	}
	var decoded harnessv2.MCPBrokerCallResponse
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if string(decoded.Result) != `{"account":"alice","source":"sealed-connection"}` {
		t.Fatalf("MCP result = %s", decoded.Result)
	}
	if appCalls.Load() != 1 {
		t.Fatalf("credentialed app calls = %d, want 1", appCalls.Load())
	}

	foreign := task.DeepCopy()
	foreign.Spec.RequestedBy = &corev1alpha1.RequestedBy{Issuer: issuer, Subject: "bob"}
	foreign.Annotations[labels.AnnotationRequestedByStamp] = connectors.RequesterStamp(
		stampKey, foreign.UID, issuer, "bob",
	)
	if err := client.Update(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	request.Call.CallID = "foreign-call"
	request.Metadata.OperationID = "connector-identity-foreign-read"
	request.Metadata.RequestDigest = ""
	request.Metadata.RequestDigest, err = harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	response = performMCPBrokerCall(t, broker, request, bearer, capability)
	if response.Code == http.StatusOK {
		t.Fatalf("foreign requester unexpectedly used Alice's credential: %s", response.Body.String())
	}
	if appCalls.Load() != 1 {
		t.Fatalf("foreign requester reached the credentialed app; calls = %d", appCalls.Load())
	}

	currentTask := &corev1alpha1.Task{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: task.Name}, currentTask); err != nil {
		t.Fatal(err)
	}
	currentTask.Spec.RequestedBy = requester.DeepCopy()
	currentTask.Annotations[labels.AnnotationRequestedByStamp] = connectors.RequesterStamp(
		stampKey, currentTask.UID, issuer, subject,
	)
	if err := client.Update(context.Background(), currentTask); err != nil {
		t.Fatal(err)
	}
	currentConnection := &corev1alpha1.Connection{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: connection.Name}, currentConnection); err != nil {
		t.Fatal(err)
	}
	currentConnection.Status.GrantSequence++
	if err := client.Update(context.Background(), currentConnection); err != nil {
		t.Fatal(err)
	}
	request.Call.CallID = "stale-grant-call"
	request.Metadata.OperationID = "connector-identity-stale-grant-read"
	request.Metadata.RequestDigest = ""
	request.Metadata.RequestDigest, err = harnessv2.CanonicalRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	response = performMCPBrokerCall(t, broker, request, bearer, capability)
	if response.Code == http.StatusOK {
		t.Fatalf("stale frozen grant unexpectedly reached the connector: %s", response.Body.String())
	}
	if appCalls.Load() != 1 {
		t.Fatalf("stale frozen grant reached the credentialed app; calls = %d", appCalls.Load())
	}
}
