# qtmesh-runner

Runner agent for asynchronous [QtMesh Cloud](https://qtmesh.dev) jobs — GitLab-runner style. Registered machines claim jobs (asset analysis, thumbnail/turntable renders, …) from the QtMesh Cloud API over **outbound HTTPS only**, execute them with the [QtMeshEditor](https://github.com/fernandotonon/QtMeshEditor) CLI, upload artifacts, and report completion.

Epic: [#1](https://github.com/fernandotonon/qtmesh-runner/issues/1) · Backend: [qtmesh-cloud#106](https://github.com/fernandotonon/qtmesh-cloud/issues/106)

## Quick start

Requirements: Node ≥ 20, plus **either** Docker (uses `ghcr.io/fernandotonon/qtmesh:latest`) **or** a native `qtmesh` binary.

### 1. Mint a runner token (admin)

Runner tokens are minted by a QtMesh Cloud admin. First get a **personal API token** for yourself: sign in at [qtmesh.dev](https://qtmesh.dev) → user menu → **API & tokens** → *Personal API tokens* → Create (copy the `qtm_pat_…` value — it is shown once).

Then mint the runner token with it:

```bash
curl -s -X POST https://api.qtmesh.dev/v1/admin/runner-tokens \
  -H "authorization: Bearer qtm_pat_..." \
  -H "content-type: application/json" \
  -d '{"name":"my-linux-pc"}'
# → { "ok": true, "token": "qtm_run_...", ... }   (also shown once — store it)
```

For CI / shared environments, mint a **restricted** token instead — it can only claim jobs on public assets and only the listed operations:

```bash
curl -s -X POST https://api.qtmesh.dev/v1/admin/runner-tokens \
  -H "authorization: Bearer qtm_pat_..." \
  -H "content-type: application/json" \
  -d '{"name":"github-actions","restrictions":{"allowPrivateAssets":false,"allowedOperations":["analyze-asset","generate-thumbnail"],"maxClaimBatch":10}}'
```

Revoke a token anytime: `DELETE /v1/admin/runner-tokens/:id`.

### 2. Instantiate a runner

Clone this repo on the machine, then:

**Persistent worker** (home PC / server — keeps polling until stopped):

```bash
git clone https://github.com/fernandotonon/qtmesh-runner && cd qtmesh-runner

QTMESH_API_URL=https://api.qtmesh.dev \
QTMESH_RUNNER_TOKEN=qtm_run_... \
QTMESH_RUNNER_NAME=my-linux-pc \
node agent.mjs run --continuous
```

By default jobs execute in the locked-down Docker image. On a machine with a native `qtmesh` build (e.g. a Mac), skip Docker entirely:

```bash
QTMESH_API_URL=https://api.qtmesh.dev \
QTMESH_RUNNER_TOKEN=qtm_run_... \
QTMESH_NATIVE_QTMESH=$HOME/QtMeshEditor/build_local/bin/qtmesh \
node agent.mjs run --continuous
```

**Batch worker** (CI / cron — drains eligible jobs, then exits):

```bash
node agent.mjs run --max-jobs 10 --exit-when-empty
```

Sanity checks: `node agent.mjs capabilities` (what this machine will advertise) · `node agent.mjs health` (config + executor).

The runner shows up under `GET /v1/admin/runners` after its first registration; stopping it is always safe — any in-flight job's lease expires server-side and the job is retried elsewhere.

## Configuration (env)

| Var | Default | |
|---|---|---|
| `QTMESH_API_URL` | — | required |
| `QTMESH_RUNNER_TOKEN` | — | required, `qtm_run_...` |
| `QTMESH_RUNNER_NAME` | hostname | stable machine name |
| `QTMESH_CAPABILITIES` | auto | csv, e.g. `cpu,quality-analysis,ogre-render` |
| `QTMESH_NATIVE_QTMESH` | — | path to native `qtmesh` (skips Docker) |
| `QTMESH_DOCKER_IMAGE` | `ghcr.io/fernandotonon/qtmesh:latest` | processor image |
| `QTMESH_POLL_INTERVAL` | 30 | seconds between empty claims |
| `QTMESH_WORK_DIR` | os tmp | per-job scratch (wiped after each job) |
| `QTMESH_JOB_TIMEOUT` | 600 | seconds per job |
| `QTMESH_EPHEMERAL` | — | `1` for CI workers |

## How it works

1. Registers itself (name, platform, capabilities) with its token.
2. Polls `POST /v1/runner/claim`; the server only hands out jobs whose required capabilities the runner satisfies (and that the token is allowed to touch).
3. Every claim carries a **lease token**; progress pings renew it. If the machine dies, the lease expires server-side and the job is retried elsewhere — kill -9 is safe.
4. Inputs stream from the API; artifacts stream back to it. No storage credentials on the machine.
5. Untrusted assets execute in a locked-down container (`--network=none --cap-drop=ALL --memory=4g …`) or the native CLI; each job gets a fresh workdir, deleted afterwards.

## Supported operations (v1)

| operation | tool | output |
|---|---|---|
| `analyze-asset` | `qtmesh info/validate/anim --json` | `report.json` |
| `generate-thumbnail` | `qtmesh turntable --frames 1` | `thumbnail.png` (auto-attaches to marketplace listings) |

## GitHub Actions

`.github/workflows/batch-worker.yml` runs the same agent in batch mode every 4h (and on demand). Set the `QTMESH_RUNNER_TOKEN` repo secret using a token minted with restrictions:

```json
{ "allowPrivateAssets": false, "maxClaimBatch": 10 }
```
