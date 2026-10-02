# node-drain-controller
 
A Kubernetes controller written from scratch in Go with
[client-go](https://github.com/kubernetes/client-go) — no controller-runtime, no
Kubebuilder.
 
It watches nodes for a disruption taint and reports which pods on them **will
not be recreated** if the node goes away.
 
Built to understand what tools like Karpenter are doing internally. The notes
below are as much the point as the code.
 
---
 
## The problem
 
When a node is marked for disruption — a spot interruption notice, a drain, a
maintenance window — not all pods on it are equal:
 
| Pod | What happens when the node dies |
|---|---|
| Owned by a ReplicaSet / StatefulSet / Job | Rescheduled elsewhere. Fine. |
| Owned by a DaemonSet | Goes with the node. Expected. |
| **No controller owner** | **Gone. Nothing will recreate it.** |
 
That last row is the one worth knowing about *before* the drain, not after.
`kubectl drain` refuses to evict those pods without `--force` for exactly this
reason — this controller surfaces the risk earlier.
 
---
 
## What it does
 
- Watches `Node` objects for a configurable taint key
- When one is tainted, lists the pods scheduled to it
- Classifies each by its controller owner reference
- Annotates the at-risk ones and emits a Kubernetes `Warning` Event
- Clears the annotation when the taint is removed
- Runs locally against kubeconfig, or in-cluster with a ServiceAccount
---
 
## Running it
 
```bash
kind create cluster --name clientgo
 
kubectl apply -f deploy/crd.yaml
kubectl apply -f deploy/policy.yaml
 
go mod tidy
go run main.go                 # or --dry-run
```
 
Create something to find, then mark the node:
 
```bash
kubectl run orphan --image=nginx          # a bare pod — no owner
kubectl taint node clientgo-control-plane spot-interruption=true:NoSchedule
```
 
### In-cluster
 
```bash
docker build -t node-drain-controller:dev .
kind load docker-image node-drain-controller:dev --name clientgo
kubectl apply -f deploy/
kubectl -n kube-system logs -f -l app=node-drain-controller
```
 
---
 
## Output
 
```
loaded policy: taintKey=spot-interruption annotate=true
caches synced — starting workers
 
node clientgo-control-plane tainted: spot-interruption=true:NoSchedule
  kube-system/kube-proxy-f9vj2 — daemonset, expected
  kube-system/kindnet-mhj27 — daemonset, expected
  kube-system/coredns-559f6c778d-8j7ft — owned by ReplicaSet coredns-559f6c778d
  default/nginx-69b9cdbbdd-9fw4s — owned by ReplicaSet nginx-69b9cdbbdd
  default/orphan — bare pod, will NOT be recreated
    annotated default/orphan
  1 pod(s) at risk on clientgo-control-plane
```
 
And in `kubectl describe pod orphan`:
 
```
Annotations:  node-drain-controller/at-risk: clientgo-control-plane
 
Events:
  Type     Reason          Age   From                   Message
  ----     ------          ----  ----                   -------
  Warning  DisruptionRisk  5s    node-drain-controller  Node clientgo-control-plane
           is marked for disruption and this pod has no controller owner...
```
 
---
 
# How it works
 
## The pipeline
 
```
kubectl taint node
        │
        ▼
   API server ──────► etcd
        │
        │  WATCH  (opened once at startup, held open)
        ▼
   Node informer ────► local cache  (every Node, in memory)
        │
        │  AddFunc / UpdateFunc / DeleteFunc
        ▼
   enqueue(key)          "clientgo-control-plane"
        │
        ▼
  ┌───────────────┐
  │   workqueue   │   dedupes · rate limits · tracks in-flight
  └───────────────┘
        │
        │  Get()
        ▼
   worker goroutine ×2
        │
        ▼
   reconcile(key)
        │
        ├─► nodeLister.Get(name)        read from cache, no API call
        ├─► find taint in spec.taints
        ├─► podLister.List()            read from cache, no API call
        ├─► filter to this node
        ├─► check owner references
        └─► patch annotation            write via clientset
```
 
Everything left of `reconcile` is plumbing. All the actual decisions live in
~40 lines inside it.
 
## The four moving parts
 
### Informer
 
Keeps an in-memory copy of a resource, kept current by a watch.
 
**LIST once, then WATCH forever.** One API call fetches every Node and returns a
`resourceVersion`; a long-lived connection then streams every change after that
point. If the connection drops, it reconnects from the last version it saw — no
gap, no full re-LIST.
 
> Without informers, every controller would poll. A cluster full of polling
> controllers is how you get 429s from the API server.
 
The cache is also why controller memory scales with cluster size. Karpenter's
controller requests 2Gi because it holds every Pod, Node, NodeClaim and NodePool
in RAM.
 
### Keys, not objects
 
Handlers don't do work. They turn the object into a string — `namespace/name`,
or just `name` for cluster-scoped resources like Nodes — and drop it in a queue.
 
By the time a worker picks that key up, the object may have changed three times.
Carrying the object means acting on stale data. Carrying the key means *go look
up what's true now*.
 
> **Controllers are level-triggered, not edge-triggered.** They react to current
> state, not to which event fired. It's why they survive missed events, dropped
> connections and restarts.
 
This is also why the controller reports the same thing on every heartbeat. It
isn't saying "a taint was added" — it's saying "a taint is present." It has no
memory between reconciles and doesn't need one.
 
### Workqueue
 
Three jobs, and the reason handlers enqueue instead of working:
 
| | Why it matters here |
|---|---|
| **Deduplication** | Nodes heartbeat constantly. A burst of 20 updates collapses to one reconcile — and nothing is lost, because reconcile reads current state anyway. |
| **Decoupling** | Handlers run on the informer's goroutine. Slow work there blocks the watch and the cache falls behind reality. Enqueuing returns in microseconds. |
| **Backoff** | Failed reconciles requeue with growing per-key delay, bounded by an overall rate limit so a widespread failure can't become a self-inflicted DDoS. |
 
Three operations that sound similar and aren't:
 
- `Get()` — removes a key and marks it **in flight**
- `Done()` — "finished, safe to deliver again" → **concurrency**
- `Forget()` — resets the failure count → **backoff**
The in-flight guarantee is why two workers never process the same key at once,
which is why there are no locks anywhere in this code.
 
### Reconcile
 
Reads current state, compares it to what should be true, fixes only the
difference. Returns `nil` on success — anything else means requeue and retry.
 
---
 
# Design decisions
 
Each of these was a choice, and the reasoning matters more than the code.
 
### Read through the lister, write through the clientset
 
| | Lister | Clientset |
|---|---|---|
| Reads from | informer cache, in memory | API server, over HTTPS |
| Cost | negligible | network round trip |
| Freshness | eventually consistent | authoritative |
| Can write | no | yes |
 
Every read in `reconcile` is a cache lookup costing nothing. The only API calls
are the annotation patches. A controller that called
`clientset.CoreV1().Nodes().Get()` in reconcile would make an HTTP request
several times per node per minute — multiply by node count and controller count
and that's how an API server gets throttled.
 
### Nodes are watched. Pods are only read.
 
The pod informer gives a lister, but has **no event handlers**.
 
Node changes are the trigger; pods are what gets examined once awake. Adding
handlers to pods would enqueue a node key on every pod change — reconciling
thousands of times for changes that don't matter.
 
> Watch what should *trigger* reconciliation. Read everything else through a
> lister.
 
### Idempotent by construction
 
```go
if pod.Annotations[riskAnnotation] == nodeName {
    return nil
}
```
 
Three lines, and the most important ones in the file.
 
Reconcile runs several times a minute while a taint is present. Without this
check, every one of those would fire a PATCH writing the identical value —
forever. With it, the first reconcile writes and every subsequent one is a
no-op.
 
> Act because reality doesn't match what it should be, not because an event
> arrived. This is why a controller restart doesn't duplicate work — Karpenter
> replays every pod as an Add on startup and checks existing capacity before
> provisioning more.
 
### Patch, not Update
 
`Update` sends the whole object with the `resourceVersion` you read, so any
concurrent write returns 409 Conflict. The kubelet writes pod status constantly,
so conflicts would be frequent.
 
A merge patch sends only the field being changed and can't conflict over fields
it never touched. Removal uses the same call with `null` as the value — in a
merge patch (RFC 7386), null means delete the key.
 
### No finalizer
 
A finalizer is for when a controller owns something **outside** Kubernetes that
needs cleanup before the object disappears. This one owns nothing external, so
adding one would be ceremony — and a badly implemented finalizer wedges objects
in `Terminating` permanently.
 
Instead it checks `DeletionTimestamp` and skips objects on their way out.
 
> Karpenter genuinely needs them: deleting a Node object must not orphan a
> running EC2 instance. That's also why `terraform destroy` can hang on a
> Karpenter cluster — Terraform has no way to wait on a drain.
 
### Annotation, not label
 
Labels are indexed and selectable; annotations are arbitrary metadata. Nothing
selects on this, so a label would add index pressure for no benefit.
 
### A dry-run flag
 
Any controller that mutates cluster state should have one. It's how you validate
behaviour before granting write permissions.
 
It sits *after* the idempotence check, so it reports only what would actually
change rather than everything it looked at.
 
### RBAC derived from the code, not guessed
 
| Code | Verb |
|---|---|
| Informer LIST + WATCH | `list`, `watch` on nodes, pods |
| Annotation patch | `patch` on pods |
| Event recorder | `create` **and** `patch` on events — it aggregates |
| Policy read | `get` on `disruptionpolicies` |
 
No `delete`, no `create` on pods, no `update`. Tested by removing a verb and
watching the 403 — which names the ServiceAccount, the verb and the resource, so
the fix is mechanical.
 
### Distroless, non-root
 
Multi-stage build: ~900MB of Go toolchain in the build stage, ~20MB binary in
the runtime stage. `distroless/static:nonroot` has no shell, no package manager,
no utilities. The attack surface is the binary.
 
### Single replica
 
Two replicas would both reconcile the same nodes and race each other's patches.
Running more safely needs **leader election** — replicas compete for a Lease and
only the holder works. That's why Karpenter's Helm chart sets
`LeaderElection: true` alongside pod anti-affinity.
 
---
 
# Things that went wrong
 
The useful part.
 
### Static pods looked at-risk
 
The first working version flagged `etcd`, `kube-apiserver`, `kube-scheduler` and
`kube-controller-manager` as pods that wouldn't be recreated.
 
They're **static pods**. The kubelet runs them from manifests on disk and
creates read-only *mirror pods* in the API, owned by the Node. So nothing in the
API would recreate them — the ownership check was correct, and the conclusion
was wrong. The kubelet restarts them regardless.
 
Fixed by filtering on the `kubernetes.io/config.mirror` annotation, which the
kubelet sets on every mirror pod.
 
### The reconcile only converged one way
 
The controller annotated at-risk pods but never cleared the annotation when the
taint was removed. The pod went on claiming it was at risk indefinitely.
 
That's a one-way trigger, not reconciliation. `if taint == nil` had been treated
as *nothing to do*, when it actually means something: **this node is healthy, so
anything claiming otherwise needs correcting**.
 
The cleanup path keys off the annotation itself rather than re-deriving the
original condition — otherwise a later change to the selection criteria would
orphan state that could no longer be found.
 
### `defer` is function-scoped
 
`defer queue.Done(key)` belongs in the per-item function, not the worker loop.
The loop runs forever, so a defer there never fires — every key stays in flight
permanently and the controller silently stops reconciling. No error, no log.
 
Splitting into `runWorker` and `processNextItem` is what makes it fire per item.
 
---
 
# Configuration
 
The taint key is configurable through a CRD rather than hardcoded:
 
```yaml
apiVersion: jk.io/v1alpha1
kind: DisruptionPolicy
metadata:
  name: default
spec:
  taintKey: spot-interruption
  annotate: true
```
 
```bash
kubectl patch disruptionpolicy default --type=merge \
  -p '{"spec":{"taintKey":"maintenance"}}'
```
 
A CRD is a schema registered with the API server — storage, validation,
versioning and the whole kubectl surface, for free. The behaviour lives entirely
in the controller that reads it. `spec.taintKey` is marked required, so an
invalid policy is rejected by the API server and never reaches the code.
 
The typed clientset is **generated code** and has no idea this type exists, so
it's read with the **dynamic client**: a GroupVersionResource constructed by
hand, returning `unstructured.Unstructured` — a map rather than a struct, with
every field access a runtime lookup.
 
> Karpenter's `NodePool` and `EC2NodeClass` are CRDs. It's the same reason
> Terraform uses `kubectl_manifest` with raw YAML for them rather than a typed
> resource.
 
---
 
# Known limitations
 
Written down deliberately rather than hidden.
 
| | Why, and what the fix would be |
|---|---|
| **Policy is read at startup, not watched** | A mid-flight change leaves annotations written under the old taint key until the next restart. A proper fix watches the CRD and decides what happens to state written under the old config — a bigger design question than the configurability was worth here. |
| **No leader election** | Single replica only. ~20 lines with `tools/leaderelection` to fix. |
| **Pod lookup is a full scan** | `O(nodes × pods)`. At scale this wants an **indexer** on `spec.nodeName`, turning the scan into a map lookup. |
| **No metrics** | Reconcile count, error count and queue depth would be the first three. |
| **Minimal tests** | Logic is extracted to be testable (`findTaint`), but the suite isn't written. The fake clientset works because the controller holds `kubernetes.Interface` rather than the concrete type. |
 
---
 
# How it was built
 
Each stage was made to run before the next was started, so a failure was always
traceable to one change.
 
| Stage | Added | What running it revealed |
|---|---|---|
| 1 | Informer + event handlers, printing only | `ADD` fires for objects that already exist — the initial LIST arrives as a stream of Adds. Node heartbeats churn constantly on an idle cluster. |
| 2 | Workqueue, worker goroutines, retry | Bursts of events collapse into single reconciles. Exponential backoff visible by forcing a failure. |
| 3 | Lister read, taint detection | Silence is the normal state. Reconcile runs constantly and correctly decides there's nothing to do. |
| 4 | Pod informer, owner-reference logic | The static-pod false positive. |
| 5 | Annotation patch, `--dry-run` | Idempotence: "annotated" once, then nothing. |
| 6 | Bidirectional cleanup | The one-way convergence bug. |
| 7 | Events | Findings visible in `kubectl describe`, aggregated by the recorder. |
| 8 | In-cluster deployment, RBAC | A real 403, and the retry path firing against something that genuinely failed. |
| 9 | CRD + dynamic client | Behaviour changed with `kubectl patch` — no rebuild. |
 
---
 
# Reference
 
### Project layout
 
```
.
├── main.go                  controller
├── Dockerfile               multi-stage, distroless
└── deploy/
    ├── crd.yaml             DisruptionPolicy definition
    ├── policy.yaml          a DisruptionPolicy instance
    ├── rbac.yaml            ServiceAccount, ClusterRole, binding
    └── deployment.yaml
```
 
### Phrases worth remembering
 
- Controllers are level-triggered, not edge-triggered
- Dedup, decoupling, backoff — why a workqueue
- Observe current state, compare to desired, fix only the delta
- Read through the lister, write through the clientset
- In a controller, a 404 is a signal, not a failure
- An informer is LIST once, then WATCH forever
- A controller's memory footprint is its informer caches
### Further reading
 
- [sample-controller](https://github.com/kubernetes/sample-controller) — the official reference implementation
- [client-go controller design doc](https://github.com/kubernetes/sample-controller/blob/master/docs/controller-client-go.md) — Reflector, DeltaFIFO, Indexer
- [Karpenter](https://github.com/aws/karpenter-provider-aws) — the same patterns, built on controller-runtime