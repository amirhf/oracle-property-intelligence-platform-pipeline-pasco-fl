# Candidate-owned Filebase demonstration

This runbook is a readiness boundary, not a publication record.

Temporary candidate-owned Filebase demonstration of protocol compatibility.
The buckets and IPNS identities are candidate-controlled and are not represented
as Elephant-owned, owner-approved, authoritative-complete, or the final canonical
assessment publication.

## Current bounded execution state

The approved 25-property candidate plan has 34 provider-CID-verified object
checkpoints totaling 178,045 bytes. Its open-data Names API control plane reports
the approved target. The latest bounded observation also found the official
Filebase gateway and dweb.link at the target while ipfs.io remained at the
immutable prior. The durable plan therefore remains
`manual_intervention_required`, and the existing open-data intent remains
`update_ambiguous`, until a fresh policy-bound cycle is recorded. The query-table
intent remains `prior_confirmed`; its IPNS identity has not been updated. The
candidate executor is disabled. No completed IPNS publication, public read-plane
verification, owner/canonical publication, or Vercel deployment is claimed.

The controller-authorized `candidate_filebase_dweb_v1` policy is restricted to
this exact candidate plan and approval. It requires the Filebase control plane,
official Filebase public gateway and dweb.link to report the approved target.
ipfs.io is retained as immutable diagnostic evidence but does not block this
candidate-only profile. The policy cannot authorize `authoritative_complete`,
owner/canonical publication or any different plan, approval, identity or CID.

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
environment, and redeploy. Filebase write credentials do not belong in the
hosted read-plane project. The preview must pass `/`, `/health`, `/mcp` and all
explorer smoke checks before any production promotion is considered.

The bounded candidate execution used candidate-controlled Filebase resources;
it did not use or enable the real owner/canonical executor. Authenticated owner
approval remains separate, and Vercel deployment has not been performed.
