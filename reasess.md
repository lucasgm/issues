`proposal.html` (drafted without access to `RECORE.third-party-catalog` — it says so itself) recommends a "catalog-first hybrid": exclude generated `metrics/*` files from Splunk, and instead build an async exporter that diffs commits on the catalog repo's `main` branch into normalized change events, later completed with IssueOps request events and scan events.

The user's instinct — that `metrics-catalog.yml` / `additions.ndjson` is a local batch-reporting leaf, not fit to forward to Splunk — is correct and now verified. But with both repos actually available locally, the proposal's *replacement* mechanism (diffing catalog `main`) doesn't hold up either:

- The catalog repo has **zero GitHub Actions workflows of its own** and **no GitHub issue number anywhere in its git history** — commits/branches/PRs key only on `run_id`. A diff exporter reading just the catalog repo cannot populate request correlation without a second, stateful `run_id → issue` lookup against `RECORE.third-party`'s Actions API.
- `main`'s history isn't flat: force-push happens only on ephemeral `catalog-${RUN_ID}` branches pre-merge, but `main` itself gets real (non-squash) merge commits plus periodic `chore: refresh catalog metrics` noise commits from `metrics-catalog.yml` itself (confirmed by direct repo inspection) — a diff exporter has to walk merges and filter its own tooling's output.
- Every catalog-mutating workflow **already has full context** — issue number, artifact identity, before/after status, security scan results — at the exact moment it writes. `image-request-ops.yml:973-1024` already re-clones the catalog and derives `.status`/`.artif_ng`/`.security.security_report`/`.tag` per requested artifact to build its human-facing result comment (verified by direct read) — that derivation loop *is* the event payload; emitting from it is not new extraction logic, it's one `emit-telemetry` call added inside a loop that already exists.

Confirmed with the user: recommend **synchronous emission at existing write-time checkpoints**, not the async diff exporter, with a periodic full-catalog snapshot as the safety net for rare out-of-band edits, and the diff logic in `extract_from_catalog_commits.py` repurposed only as a **one-time historical backfill**, not a live mechanism.

This also builds directly on correlation-design decisions already settled earlier in this engagement: `artifact_id` (`{source}:{name}:{version}`, always present, durable end-to-end) as the primary join key; `request_issue` as a secondary attribute present only on request-phase events (since one issue can request multiple artifacts — `request-image.yml` explicitly supports listing multiple image URLs per issue, looped in `image-request-ops.yml`); `request_initiated` fired per-artifact, not per-issue; a shared `emit-telemetry` composite action as the low-level HEC sender; telemetry that never blocks publication; and no changes required to the catalog's JSON schema.

## Recommended architecture

**Reject the async git-commit-diff exporter as the live mechanism.** Instrument each catalog-mutating workflow to emit telemetry at its own existing authoritative checkpoint, via one shared `emit-telemetry` composite action. Use a periodic full-catalog snapshot/reconciliation job (not a diff) to catch rare out-of-band edits that bypass every instrumented workflow. Repurpose `extract_from_catalog_commits.py`'s existing history-walk as a one-time backfill only.

### Canonical event schema

```json
{
  "event_id": "sha256(artifact_id|event_type|source_run_id|source_step)",
  "event_type": "request_initiated|catalog_publish|catalog_scan_gated|catalog_eol_block|recurring_scan|reconciliation_snapshot",
  "ts": "ISO8601",
  "artifact_id": "amazon:amazoncorretto:21",
  "artifact_kind": "container|helm-chart|library-pypi|library-npm|package-amazon|package-oracle|package-microsoft",
  "request_issue": 4821,
  "source_run_id": "18452...",
  "source_workflow": "image-request-ops.yml",
  "catalog_ref": { "status": "active", "artif_ng": "...", "security_report": "...", "tag_or_release": "..." },
  "extra": {}
}
```

`artifact_kind` + `extra` absorb the confirmed non-uniformity across the catalog's three JSON shapes (containers/library nested under `software{}`, packages flat `releases[]`, helm-chart top-level array with nested `images[]`; `digest` absent for library, `sunset_date` container-only, `malware` library-only). The top-level fields are the intersection that always exists — no catalog schema changes needed anywhere.

### Instrumentation points (reuse existing checkpoints, don't build new extraction logic)

| File | Existing checkpoint | Event(s) to add |
|---|---|---|
| `image-request-ops.yml` | `parse-issue` step (id at [line 32](RECORE.third-party/.github/workflows/image-request-ops.yml#L32)) resolving the per-image config list (`configurations` output, [line 391](RECORE.third-party/.github/workflows/image-request-ops.yml#L391)) | `request_initiated`, one per artifact |
| `image-request-ops.yml` | the per-artifact catalog verification loop, [lines 974-1024](RECORE.third-party/.github/workflows/image-request-ops.yml#L974-L1024) — already computes `$sw`/`$tag`/`$st`/`$art`/`$rep` per image | `catalog_publish` (active/suppressed) / `catalog_scan_gated` (inactive) / `catalog_eol_block`, one per artifact, using `$ISSUE` ([line 802](RECORE.third-party/.github/workflows/image-request-ops.yml#L802)) as `request_issue` |
| `library-request-ops.yml` | analogous parse-issue + post-run catalog-read steps | same triad |
| `helm-charts-request-ops.yml` | chart config commit to `RECORE.third-party-configs` (separate from the catalog write) + its deferred "not yet whitelisted" close step | needs its own two checkpoints — do not copy-paste the image pattern |
| `windows-package-request-ops.yml` | confirmed to also commit to `CONFIG_REPO` ([lines 184, 267](RECORE.third-party/.github/workflows/windows-package-request-ops.yml#L184)) before dispatch — same configs-repo divergence as helm, needs the same bespoke treatment |
| `image-recurring-scan.yml` / `library-recurring-scan.yml` | their per-artifact status/tag extraction loop | `recurring_scan` — `artifact_id` only, no `request_issue` (none exists at this point, confirmed — not a gap to fix, a structural fact) |
| `catalog-merge.yml` | "Commit and push changes" step ([lines 149-176](RECORE.third-party/.github/workflows/catalog-merge.yml#L149-L176)), which already has `packageName`/`packageVersion` | `catalog_publish`, no `request_issue` (reusable workflow, called with no issue input today) |

### `emit-telemetry` composite action

Model on `vendor-request-ops.yml`'s existing ServiceNow REST POST (basic-auth header, preflight check, non-200 handling around [lines 81-86](RECORE.third-party/.github/workflows/vendor-request-ops.yml#L81-L86)) — the only existing external-HTTP-POST pattern in the repo. New composite action at `.github/actions/emit-telemetry/action.yml`: takes an event-JSON input, POSTs to Splunk HEC with token auth, batches per-loop bursts, retries with backoff, redacts unexpected keys, and **always exits 0** (`|| true`, matching the fail-soft pattern already used for the ServiceNow call) so a Splunk outage never blocks issue tracking, catalog writes, or publication.

### Backfill

Run `scripts/metrics/extract_from_catalog_commits.py` once, retargeting its sink from git-committed NDJSON to a batched HEC POST (or POST straight from its existing NDJSON output), tagged `source: backfill`. `request_issue` will be absent on all backfilled events — that's an accepted, documented, one-time gap; do not attempt to reconstruct it via Actions-API history lookups. Retire the script from any live/scheduled role afterward — it stays purely a historical bootstrap tool, and `metrics-catalog.yml`'s existing daily local-report job is untouched and left exactly as-is (it's not part of this pipeline).

### Reconciliation snapshot (safety net, not a live pipeline)

New scheduled job, on the same cadence as `metrics-catalog.yml`: clone the catalog (reusing the same clone pattern already in `image-request-ops.yml:977-978`), compute a content hash per `artifact_id` across all four `vendor-catalog/` categories, diff against the last snapshot's hashes, and emit `reconciliation_snapshot` only for changed `artifact_id`s. This is what actually catches `catalog-merge.yml` gaps or any genuinely out-of-band push — a full snapshot compare, not a per-commit diff pipeline.

### Phased rollout

1. **Phase 0** — build `emit-telemetry`, land it in log-only/no-op mode.
2. **Phase 1** — instrument `image-request-ops.yml` end-to-end (highest volume, cleanest checkpoint); validate `request_initiated` → `catalog_publish`/`catalog_scan_gated` pairing and per-artifact time-to-approval in Splunk.
3. **Phase 2** — `library-request-ops.yml`, `catalog-merge.yml`, `image-recurring-scan.yml`, `library-recurring-scan.yml`.
4. **Phase 3** — `helm-charts-request-ops.yml`, `windows-package-request-ops.yml` (bespoke configs-repo checkpoints).
5. **Phase 4** — one-time backfill via modified `extract_from_catalog_commits.py`; stand up the reconciliation snapshot job.

### Risks / controls

| Risk | Control |
|---|---|
| Force-push / consolidation noise on catalog `main` | N/A — this design never reads catalog git history live |
| Non-uniform JSON shapes across artifact types | `extra` bag + shape-tolerant intersection fields in the canonical schema |
| Helm / windows-package configs-repo divergence | Dedicated checkpoints per workflow (confirmed both diverge identically), not a copy-pasted pattern |
| Splunk outage | `emit-telemetry` always exits 0; publication/issue-close never blocked |
| Duplicate/out-of-order delivery | `event_id = hash(artifact_id, event_type, run_id, step)` — idempotent on the Splunk side |
| Out-of-band catalog edits bypassing all instrumented workflows | Reconciliation snapshot job (not a diff pipeline) |

## Critical files

- `RECORE.third-party/.github/workflows/image-request-ops.yml` — primary instrumentation target, lines 32/391/802/974-1024
- `RECORE.third-party/.github/workflows/vendor-request-ops.yml` — pattern reference for `emit-telemetry`'s HTTP-POST design, lines 81-86
- `RECORE.third-party/.github/workflows/library-request-ops.yml`, `helm-charts-request-ops.yml`, `windows-package-request-ops.yml`, `catalog-merge.yml`
- `RECORE.third-party/.github/workflows/image-recurring-scan.yml`, `library-recurring-scan.yml`
- `RECORE.third-party/scripts/metrics/extract_from_catalog_commits.py` — backfill source
- New: `RECORE.third-party/.github/actions/emit-telemetry/action.yml`

## Verification

- Phase 0/1: trigger a real image request issue against a test vendor, confirm `emit-telemetry` fires without affecting the existing issue-comment/close behavior (diff the issue's comment thread before/after — should be identical plus telemetry firing silently alongside).
- Confirm in Splunk (or a stand-in HEC receiver during dev) that `request_initiated` and the matching `catalog_publish`/`catalog_scan_gated` events share the same `artifact_id`, and that `request_issue` matches the real issue number.
- For a multi-image issue (2+ images in one request), confirm each image gets its own `request_initiated`/`catalog_publish` pair with distinct `artifact_id`s and correct individual outcomes — this is the scenario the proposal's original design would have gotten wrong.
- Force a Splunk-unreachable condition (bad HEC URL) in a test run and confirm the workflow still closes the issue / completes publication normally.
- After Phase 4: spot-check that backfilled events and live events for the same artifact don't produce duplicate `event_id`s in Splunk.
