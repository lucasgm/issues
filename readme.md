# Architecture — RECORE.third-party

How ICE's third-party onboarding platform turns a GitHub issue into a scanned, signed, cataloged
artifact. This document is the technical companion to the [README](../README.md).

---

## 1. Three-repo model

| Repo | Variable name in workflows | Contents |
|------|----------------------------|----------|
| `icesdlc/RECORE.third-party` | `WORKFLOW_REPO` / `TARGET_REPO` | Workflows, composite actions, scripts (this repo). |
| `icesdlc/RECORE.third-party-configs` | `CONFIG_REPO` (branch `main`) | Request desires: `vendor-configs/*.json`, `library-configs/{npm,pypi}-configs.json`, `chart-configs/helm-charts-config.json`. |
| `icesdlc/RECORE.third-party-catalog` | `CATALOG_REPO` | Published results, per artifact category. Powers ICEDevX. |

Cross-repo pushes use the `GHE_RE_TOKEN` PAT. Everything runs on self-hosted GHES runner groups
(`ice3p`, etc.). Registry/scan endpoints: Artifactory `artifbuild.intcx.net` (registries
`*.icr.intcx.net`), Aqua `aquasec.intcx.net`.

---

## 2. Request-ops workflows (issue → configs → dispatch → verify)

All `*-request-ops.yml` workflows trigger on `issues: [opened, edited]` (plus `workflow_dispatch`
by issue number for re-runs) and follow the same single-job pattern:

1. **Guard** on the issue's label/title (each `ISSUE_TEMPLATE` applies a distinct label:
   `image-request`, `library-request`, `helm-request`, `windows-package-request`, `blackout-request`,
   `delay-config-request`).
2. **Parse** the issue body (regex/awk) into structured fields. Image parsing handles per-registry
   URL shapes (dockerhub/dhi.io/quay/icr/…) — `image-request-ops.yml:31-396`; IBM digests are
   validated and dropped if stale — `:396-457`.
3. **Validate**; on bad input, comment the errors on the issue and stop.
4. **Write the configs repo** — checkout `CONFIG_REPO@main`, merge new entries into the vendor/
   registry config JSON, commit and push directly:
   - Images → `vendor-configs/<vendor>-catalog-container-config.json` (`image-request-ops.yml:466-624`)
   - Libraries → `library-configs/{npm,pypi}-configs.json` (`library-request-ops.yml:71-449`)
   - Helm → `chart-configs/helm-charts-config.json` (`helm-charts-request-ops.yml:354-689`)
   - Windows → derived vendor config (`windows-package-request-ops.yml:89-267`)
5. **Comment** "request received" with a run link.
6. **Dispatch** the matching lifecycle workflow via `gh workflow run` / `createWorkflowDispatch`,
   capturing the run id.
7. **Verify the real outcome** — the request-ops does *not* trust the lifecycle's green check.
   It clones `CATALOG_REPO`, looks up each requested artifact in the catalog JSON, and classifies
   it as **published / scan-gated (inactive) / past-EOL / not cataloged**, posts a per-item result
   comment, and closes the issue only on clean success (`image-request-ops.yml:788-1100`).

**Special cases:**
- `vendor-request-ops.yml` — no build. Job `create-snow-ticket` opens a ServiceNow ticket
  (idempotent) and comments the SNOW number.
- `library-blackout-request-ops.yml` — deletes the package from Artifactory and flags it inactive
  in configs + catalog (audit `reason` required).
- `library-delay-request-ops.yml` — edits only the `.delay` window in configs (controls how long a
  newly published library is held before mirroring).

---

## 3. Lifecycle workflows (build → scan → gate → publish → catalog)

### Container images — `<vendor>-workflow-image-lifecycle.yml`

One per vendor (amazon, aquasec, dockerhub, ghcr, ibm, kubernetes, microsoft, netboxlabs, quayio,
redhat). Triggered by `push` / `workflow_dispatch` (from request-ops) / daily `schedule`
(cron `0 10 * * *`, one hour before the recurring scan). Job chain (dockerhub example):

```
retrieve-container-configs   # read config JSON from CONFIG_REPO
  → construct-container-image # skopeo tag discovery / pull
  → aggregate-Images
  → scan-validate-publish-images  # Aqua scannercli → policy gate → docker push to prod → cosign sign+verify
  → consolidate-catalog       # merge per-run catalog branch
  → generate-or-update-catalog
  → cleanup-orphan-catalog-branch
```

Env flags seen across lifecycles: `ALLOW_PARTIAL_CATALOG`, `USE_SEMVER_PREFIX_SUFFIX_SELECTION`,
`ALLOW_DUPLICATE_SOFTWARE`, `SIGNATURE_CONTEXT` (`ci`/`cd`). Catalog status per artifact is written
as `active` / `inactive` (gate fail) / `suppressed` (gate fail but active AppSec suppression).

**Catalog write mechanism:** lifecycle jobs write the provider JSON onto a **per-run branch** of the
catalog repo (force-push), then a consolidate job merges into main, then the generate job refreshes
derived files, then an orphan-branch cleanup runs.

### Libraries — `npm-library-workflow.yml` / `pypi-library-workflow.yml`

`retrieve-*-configs → mirror-scan-publish → update-catalog → mirror-summary`. Mirrors into
`base-{npm,pypi}-thirdparty-dev`, respects the `delay` window, scans, and catalogs to
`vendor-catalog/library/{npm,pypi}.json`. Daily crons (10:00 / 09:30 UTC). Library scanning also
runs **malware** detection (npm/pypi-specific). External publish to npmjs.org is a separate workflow
(`publish-to-npmjs.yml`) dispatched from **Jenkins DC4**, because DC4 runners cannot reach npmjs.

### Helm charts — `helm-charts-workflow.yml`

`pull-charts → prepare-images (extract sub-images) → scan-each-chart → update-vendor-JSON-from-helm`.
A chart bundles multiple container images, each scanned individually; the chart gets an aggregate
`gate_status`. Pushes to the `helm-oci` Artifactory repo, signs with cosign.

### Windows packages — `windows-pkg-{microsoft,oracle}-workflow-lifecycle.yml`

Download installer → build NuGet package → scan on a Windows base image (Windows scannercli) →
publish to `base-nuget-thirdparty-{dev,prod}` → catalog to `vendor-catalog/packages/<vendor>.json`.
Weekly cron. Uses `.github/scripts/windows-package-utils.sh` for download/hash/extract/verify.

### RPM / Chocolatey (Corretto etc.)

The `catalog-workflow.yml` orchestrates `catalog-base-package-build.yml` / `base-package-build.yml`
(download → build → upload dev → scan → promote). Catalog writes go through the reusable
`catalog-merge.yml`, which filters a vendor JSON to the target version and merges into `amazon.json`.

---

## 4. Reusable workflows (`workflow_call`)

| Workflow | Purpose |
|----------|---------|
| `catalog-merge.yml` | Filter vendor JSON to a version, merge into `amazon.json`, commit to catalog (uses `scripts/merge_vendor_into_amazon.py`). |
| `artifact-promote.yml` | JFrog dev→prod promotion (`*-dev` → `*-prod`) + inject artifact metadata into vendor JSON. |
| `artifactory.yml` | Upload to Artifactory. |
| `scan-image.yml` | Set up scannercli + run Aqua scan. |
| `container.yml` / `rpm.yml` / `choco.yml` | Per-type build. |
| `base-package-build.yml` / `catalog-base-package-build.yml` | Full RPM/choco/nuget/image build pipeline. |
| `image-deployment.yml` / `package-deployment.yml` / `thirdparty.yml` | Deployment orchestration. |

**Recurring scanners:** `image-recurring-scan.yml` (~11:00 UTC daily, rescans every
`SCAN_INTERVAL_DAYS=3`, can open ServiceNow tickets), `library-recurring-scan.yml`
(`RESCAN_INTERVAL_DAYS=7`, drives `.github/scripts/scan-library-chunk.sh`).

> Files with `-testing`, `_old_backup`, or `custom-wf.yml` are dev/scratch variants (may pin
> feature branches like `RECORE-2901`) — not production paths.

---

## 5. Composite actions (`.github/actions/`)

| Action | Purpose | Key inputs → outputs |
|--------|---------|----------------------|
| `setup-jfrog-cli` | Configure JFrog CLI, auto-select auth token from artifactory path (`int-*`/default → `ICE_ARTIF_WRITER_TOKEN`, `base-*` → `BASE_TOKEN`). | `artifactoryPath`, `jfUrl` → `tokenType`, `jfConfigured` |
| `setup-gpg` | Install GPG + import a key. Sub-step of sign/verify. | `gpgKey` |
| `create-sbom` | Generate SBOM via GitHub dependency-graph API (`api`) or `anchore/sbom-action` (`marketplace`); emits CycloneDX + PURLs. | `sbomMethod`, `appId`/`privateKey` → `sbomFilePath` |
| `scan-sbom` | Scan an SBOM for vulns/licenses with **Snyk**. | `sbomFilePath`, `failureSeverityThreshold` → `scanStatus`, `buildShouldFail`, counts |
| `semantic-versioning` | Validate & pass through a dev-provided version. | `version` → `finalVersion` |
| `prepare-package-spec/rpm` | Build an RPM `.spec` from template (ICE conventions: adapters, service accounts, hooks, systemd). | many → spec file |
| `prepare-package-spec/choco` | Build a Chocolatey `.nuspec` from template. | `packageName`, `version`, `sources` → nuspec |
| `detect-file-types` | Classify files in a source dir. | `sourceDir` → `hasTarFiles`/`hasZipFiles`/… |
| `extract-tar-files` / `extract-zip-files` | Extract archives (optional strip of top-level folder). | `sourceDir`, `destinationDir` |
| `download-sources` | Download a URL (optional basic auth), validate. | `sourceUrl` → `downloadedFile` |
| `download-jfrog-artifacts` | Context-aware download (`ci` = GH artifact, `cd` = `jf rt download`). | `context`, `preserveSignatures` |
| `sign-artifacts` | GPG detached-sign artifacts (`.ci.asc`/`.cd.asc`). | `gpgPrivateKey`, `signatureSuffix` |
| `verify-artifacts` | GPG-verify signatures (CI local / CD from Artifactory). | `context` → `verificationStatus`, counts |
| `set-artifactory-properties` | Set provenance properties (version, sbom_url, author, scan_results, signature URLs) on Artifactory items. | context-keyed props |
| `tag-git-version` | Create git tag + GitHub release via app token. | `tagName`, `createRelease` |

> **Signing split:** images use **cosign** (via `icesdlc/recore.ReusableWorkflows/actions/*cosign*`);
> RPM/choco/jar/tgz/nupkg use **GPG** (the `sign-artifacts`/`verify-artifacts` actions above).

---

## 6. Scripts (`scripts/`)

**Discovery** (config-driven, emit JSON of resolved tags to stdout):
- `discover_amazon_images_tags.py` — ECR Public; `--config`; excludes prerelease tags, infers arch.
- `discover_ibm_images_tags.py` — IBM ICR (`cp.icr.io`); handles entitlement/`iamapikey`, digest refs.
- `discover_microsoft_images.py` — reads `filtered-images.json`, classifies Linux/Windows, writes `all-images.json`.
- `discover_netboxlabs_tags.py` — NetBox; selects stable semver via skopeo; parses `min_date` windows.

**Vendor-JSON mutation:**
- `add_artifact_to_vendor_json.py` — inject one artifact (packaging/repo/path/checksum) into a release; derives `platform`.
- `add_security_to_vendor_json.py` — attach an Aqua scan `security` block to a release.
- `merge_vendor_into_amazon.py` — merge missing versions into the `amazon` baseline (`--dry-run`, `--sort`, `--normalize-five-part`, …).
- `compare_vendor_amazon.py` — list versions in vendor JSON but not in amazon (`--json`, `--keys`).

**Corretto:**
- `fetch_corretto_release_and_images.py` — fetch last-6-months releases from all `corretto-*` repos, scrape MSI/RPM URLs, merge RPM+MSI+container by version, group by vendor.
- `fetch_and_inject_corretto_checksums.py` — download each RPM, compute SHA256, inject into `release.source.verification.checksum.sha256`.

**Metrics** (`scripts/metrics/`) — a git-history-driven pipeline (no GitHub API), run by the
`Catalog Metrics` workflow against the catalog repo:
```
extract_from_catalog_commits.py   # walk catalog git history, diff commits → additions.ndjson / scans.ndjson / all-events.ndjson
  → generate_report.py            # NDJSON → metrics/REPORT.md (+ README.md data dictionary)
  → generate_dashboard.py         # NDJSON → metrics/DASHBOARD.html (Chart.js, weekly)
```

**Shell helpers** (`.github/scripts/`):
- `scan-library-chunk.sh` — scan one matrix chunk of libraries: download from Artifactory → import as
  Docker image → Aqua scannercli (with retries) → suppression check → set final gate/status →
  **delete artifacts that fail the gate without an active suppression** → write per-package
  `chunk-results/<name>-<version>.props.json`.
- `windows-package-utils.sh` — CLI lib: `download`, `hash`, `extract-msi`, `extract-zip`, `metadata`,
  `verify`, `list-versions`.

---

## 7. Security model

- **Scanner:** Aqua `scannercli` (Linux v2022.4.812, Windows 1.0.1); SBOMs also Snyk-scanned in some paths.
- **Policy gate:** `pass` / `fail` / `blackout`. `fail` blocks publishing (status → `inactive`,
  artifact deleted from Artifactory unless suppressed).
- **Exceptions:** critical CVEs require an **AppSec exception** (SLA 3–5 business days). On approval,
  the artifact publishes as `suppressed` with per-CVE records in the catalog's `suppression_details[]`
  (issue name, ticket, approver, expiry). Exceptions are time-bound and must be renewed.
- **Signing:** cosign (images) / GPG (packages), CI and CD contexts with separate keys.

---

## <a name="secrets"></a>8. Secrets reference

| Category | Secrets |
|----------|---------|
| GitHub / cross-repo | `GHE_RE_TOKEN`, `GITHUB_TOKEN`, `PROMOTE_TOKEN`, `PRIVATE_KEY_1776554`, `DEPLY_RUNNER_KEY` |
| Artifactory | `ARTIF_USER`/`ARTIF_PASSWORD`/`ARTIF_TOKEN`, `BASE_USER`/`BASE_TOKEN`, `BASE_WRITER_*`, `BASE_THIRDPARTY_*`, `ICE_ARTIF_WRITER_TOKEN` |
| Aqua | `AQUA_TOKEN`, `AQUA_WF_USER`/`AQUA_WF_PASS`, `recore_aquasec_api_user`/`recore_aquasec_api_pass` |
| cosign | `COSIGN_PRIVATE_KEY_{CI,CD}`, `COSIGN_PUBLIC_KEY_{CI,CD}`, `COSIGN_PASSWORD_{CI,CD}` |
| Source registries | `DOCKER_HUB`/`DOCKER_HUB_PASS`, `RHEL_USER`/`RHEL_PASSWORD`, `QUAY_USER`/`QUAY_PASS`, `NETBOX_USER`/`NETBOX_PASSWORD`, `IBM_API_KEY`/`IBM_ENTITLEMENT_KEY` |
| npmjs | `NPM_PUBLISH_TOKEN` |
| ServiceNow | `SNOW_USER`/`SNOW_PASS`/`SNOW_PWD` |
