# Soofi baseline and approved Restate exceptions

The Soofi engineering guidance remains the baseline for strict TypeScript APIs, Vercel AI SDK usage, validation, testing, security and structured observability. The Oracle assignment explicitly authorizes the following architecture exceptions.

## Retained guidance

- Strict TypeScript and explicit public types.
- Zod or equivalent validation at external boundaries.
- Typecheck, formatting, lint and targeted tests.
- Vercel AI SDK for model-facing features; no direct provider-specific logic in domain code.
- Structured logs with correlation IDs.
- No credentials, auth headers, session IDs, unnecessary owner/contact data, or raw source bodies in logs.
- Explicit errors, bounded retries, deterministic calculations and dependency-health reporting.

## Approved exceptions

- No AWS CDK or AWS-hosted compute is required.
- No Lambda, CloudWatch, X-Ray, Powertools, SQS DLQ or PagerDuty dependency is introduced for the one-day assignment.
- Local mutation infrastructure is Restate 1.7.2 plus PostgreSQL 16 in a two-service Compose project.
- The Node Restate endpoint runs directly on the host at port 9080.
- Restate journals, retry/pause state and persisted pipeline run records replace AWS workflow and DLQ defaults.
- Pino-style JSON logs, run metrics, `/health`, Restate UI state and evidence reports replace CloudWatch/X-Ray dashboards for this scope.
- AWS SDK usage is limited to Filebase's external S3-compatible publication API.

These are recorded architecture choices, not permission to weaken input validation, test coverage, logging hygiene or secret handling.
