# qtmesh-runner

Runner agent for asynchronous [QtMesh Cloud](https://qtmesh.dev) jobs — GitLab-runner style. Registered machines claim jobs (asset analysis, thumbnail/turntable renders, …) from the QtMesh Cloud API over **outbound HTTPS only**, execute them with the [QtMeshEditor](https://github.com/fernandotonon/QtMeshEditor) CLI, upload artifacts, and report completion.

Epic: [#1](https://github.com/fernandotonon/qtmesh-runner/issues/1) · Backend: [qtmesh-cloud#106](https://github.com/fernandotonon/qtmesh-cloud/issues/106)

## Quick start

Requirements: Node ≥ 20, plus **either** Docker (uses `ghcr.io/fernandotonon/qtmesh:latest`) **or** a native `qtmesh` binary.

```bash
# 1. An admin mints a runner token on QtMesh Cloud:
#    POST /v1/admin/runner-tokens { "name": "my-linux-pc" }  → qtm_run_...

# 2. Run the agent:
QTMESH_API_URL=https://api.qtmesh.dev \
QTMESH_RUNNER_TOKEN=qtm_run_... \
node agent.mjs run --continuous
```

Batch mode (CI / cron):

```bash
node agent.mjs run --max-jobs 10 --exit-when-empty
```

Other commands: `node agent.mjs capabilities` · `node agent.mjs health`

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
