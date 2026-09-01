# Candidate-owned Filebase demonstration

This runbook is a readiness boundary, not a publication record.

Temporary candidate-owned Filebase demonstration of protocol compatibility.
The buckets and IPNS identities are candidate-controlled and are not represented
as Elephant-owned, owner-approved, authoritative-complete, or the final canonical
assessment publication.

## Current bounded execution state

The approved 25-property candidate plan has 34 provider-CID-verified object
checkpoints totaling 178,045 bytes. The open-data intent was already verified.
A bounded query-table observation found the approved target at the Filebase
control plane and official public gateway. It
also obtained the signed IPNS record from the compiled IPFS Delegated Routing
V1 endpoint, validated its signature and k51 identity binding with the pinned
IPNS library, and found the approved target. Only bounded metadata and the
signed-record SHA-256 were persisted; the raw record was not stored.

The controller-authorized `candidate_filebase_dweb_v1` policy is restricted to
this exact candidate plan and approval. It requires the Filebase control plane,
official Filebase public gateway and dweb.link to report the approved target.
ipfs.io is retained as immutable diagnostic evidence but does not block this
candidate-only profile. The policy cannot authorize `authoritative_complete`,
owner/canonical publication or any different plan, approval, identity or CID.

The stricter `candidate_filebase_delegated_v2` policy is a separate recovery
decision, not a modification of the approved plan or plan hash. It requires the
Filebase control plane, official Filebase public gateway and a cryptographically
valid delegated IPNS record to agree on the exact approved target. dweb.link and
ipfs.io are diagnostic only. Its immutable authorization binds the plan ID and
hash, original approval, query intent and k51 identity, prior and target CIDs,
and one exact converged signed-evidence record. No such authorization has been
reused outside that binding. The immutable human authorization
allowed the existing query intent to move from `update_ambiguous` to
`verified` and the candidate plan from `manual_intervention_required` to
`completed` without another upload or IPNS mutation. The completion record
states `remoteMutationPerformed=false`. Exact replay is idempotent.

The candidate executor is disabled. A credential-free local public read plane
has verified the official Filebase gateway, signed delegated records, immutable
root and query-table CIDs, graph and Parquet bindings, explorer routes and all
six MCP tools. Vercel deployment has not been performed. This policy is
candidate-demo-only and grants no owner/canonical authority.

The candidate flow has separate `oracle_candidate_demo_*` plan, approval,
object-effect, IPNS-intent and event tables. It never updates the accepted
`oracle_publication_*` owner/canonical state. Candidate plans accept only
`sample` or a coherent `partial` projection based on an authoritative base;
they reject `authoritative_complete`. The historical 25 and 25,000 datasets
remain samples. Processing all rows of a cached source object does not change
that coverage classification and cannot create an authoritative projection
head or absence-based inactivation.

Candidate bytes are revalidated and copied into the ignored, deterministic
`artifacts/candidate-demo/pasco/plans/<demo-plan-id>` namespace through a
uniquely owned private contender directory. Source paths must resolve beneath
`DATA_DIR`; symlink/path escape, invalid existing output or any byte/hash/CID
mismatch fails closed. Candidate publication never uploads directly from the
owner/canonical artifact directory.

The executor is disabled unless
`CANDIDATE_DEMO_REMOTE_EXECUTOR_ENABLED=true`. When enabled, configuration is
strict and requires two distinct candidate-prefixed buckets, labels and public
IPNS network keys. The S3 endpoint is explicitly allowlisted as either
`https://s3.filebase.com` or the legacy-compatible
`https://s3.filebase.io`; the names API is fixed to the documented
`https://api.filebase.io` origin. Redirects are rejected. Requests, retries, timeouts, concurrency,
object count, object bytes, total bytes and an estimated budget are all bounded
before execution.

Every artifact is rehashed and its UnixFS CID is recomputed before upload. The
Filebase `x-amz-meta-cid` receipt must equal that local CID. A missing or
mismatched CID is terminal. Upload admission and verified receipts are durable.
Both domain intents, bound to the exact candidate plan, target and prior CID,
must exist before the executor can invoke an IPNS update. Open-data and
query-table buckets and IPNS identities remain distinct. Provider request IDs
are stored only as SHA-256 values; response bodies, authorization values and
credentials are never persisted or logged.

## Required inputs

Create two candidate-only Filebase buckets and two existing, distinct IPNS
names in the Filebase dashboard. Record their public network keys. Create a
scoped candidate credential and API token. Set every `CANDIDATE_DEMO_*` value
listed in `.env.example`; prices must be set from the candidate account's
current plan rather than guessed. Generate and validate the 25-property
candidate plan, then record an exact hash-bound candidate approval. Only after
that pilot passes may a separate 25,000-sample plan be considered.

For Vercel, create or link a candidate preview project, set only the public
read-plane `MCP_*` values and `ORACLE_MCP_PROVIDER=public-ipns` in the Preview
environment, and redeploy. `MCP_PUBLIC_PLAN_SHA256` is the hash of the
immutable plan artifact bytes;
`MCP_PUBLIC_CANDIDATE_SOURCE_PLAN_SHA256` is the logical source plan's
internal hash; and `MCP_PUBLIC_CANDIDATE_DEMO_PLAN_SHA256` is the separately
approved candidate wrapper hash. Filebase write credentials do not belong in
the hosted read-plane project. The preview must pass `/`, `/health`, `/mcp`
and all explorer smoke checks before any production promotion is considered.

The bounded candidate execution used candidate-controlled Filebase resources;
it did not use or enable the real owner/canonical executor. Authenticated owner
approval remains separate, and Vercel deployment has not been performed.
