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

### 3. Run as a service (auto-start + auto-restart)

Templates live in [`examples/`](examples/). Both restart the agent on crash and start it on boot/login; killing the machine mid-job is safe (lease recovery).

**Linux (systemd)** — [`examples/qtmesh-runner.service`](examples/qtmesh-runner.service):

```bash
sudo useradd -r -m -s /usr/sbin/nologin qtmesh
sudo git clone https://github.com/fernandotonon/qtmesh-runner /opt/qtmesh-runner
sudo cp /opt/qtmesh-runner/examples/qtmesh-runner.env.example /etc/qtmesh-runner.env
sudo chmod 600 /etc/qtmesh-runner.env && sudo $EDITOR /etc/qtmesh-runner.env   # token goes here
sudo cp /opt/qtmesh-runner/examples/qtmesh-runner.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now qtmesh-runner
journalctl -u qtmesh-runner -f          # logs
```

Using the Docker executor? Add the service user to the docker group (`sudo usermod -aG docker qtmesh`), or set `QTMESH_NATIVE_QTMESH` in the env file instead.

**macOS (launchd)** — [`examples/com.qtmesh.runner.plist`](examples/com.qtmesh.runner.plist), runs at login and restarts on crash:

```bash
cp examples/com.qtmesh.runner.plist ~/Library/LaunchAgents/
$EDITOR ~/Library/LaunchAgents/com.qtmesh.runner.plist   # set repo path, node path (`which node`), token
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.qtmesh.runner.plist
tail -f ~/Library/Logs/qtmesh-runner.log                 # logs
```

Stop/remove: `launchctl bootout gui/$(id -u)/com.qtmesh.runner`.

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
| `QTMESH_EPHEMERAL` | — | `1` for CI workers (also disables eligibility checks) |
| `QTMESH_ELIGIBILITY` | on | `off` disables all eligibility gating |
| `QTMESH_REQUIRE_AC` | `1` on macOS | pause claiming on battery power |
| `QTMESH_MAX_LOAD` | cpu cores | pause claiming above this 1-min load average |
| `QTMESH_MIN_FREE_GB` | 5 | pause claiming below this free disk space |
| `QTMESH_PAUSE_FILE` | `~/.qtmesh-runner/paused` | `touch` it to pause, delete to resume |
| `QTMESH_MODEL_CACHE_DIR` | `~/.qtmesh-runner/ai-models` | persistent ONNX cache for the ML backends; empty disables the mount |


### ML model weights (auto-rig / skinning)

`auto-rig --algo unirig` and `skin-model --algo skintokens` are ONNX models whose
weights are **not** baked into the processor image; upstream downloads them on
first use. Jobs run with `--network=none`, so they cannot be fetched at job time.

When the weights are missing these backends **do not fail** — they silently fall
back to the deterministic template rig (`pinocchio` / `geodesic-voxel`) and still
exit 0. Warm the cache once, out of band:

```bash
node agent.mjs warm-models                    # both sets (~3.7 GB total)
node agent.mjs warm-models --set unirig       # auto-rig only  (~1.4 GB)
node agent.mjs warm-models --set skintokens   # skinning only  (~2.3 GB)
```

Downloads verify `Content-Length`, write via a `.part` rename, and resume from
the partial offset if interrupted, so a truncated file is never mistaken for a
complete model. Re-running is idempotent — already-cached files are skipped.

With the Docker executor the cache is bind-mounted into each container. With
`QTMESH_NATIVE_QTMESH` there is nothing to mount, so `warm-models` also wires the
native binary to the same cache: on Linux the agent points `XDG_DATA_HOME` at it
when spawning `qtmesh`, and on macOS it symlinks the cache into
`~/Library/Application Support/QtMeshEditor/QtMeshEditor/ai_models`. Run
`warm-models` with the same `QTMESH_NATIVE_QTMESH`/`QTMESH_MODEL_CACHE_DIR` values
the runner itself uses. If that path already exists as a real directory it is
left untouched and a warning is printed — point `QTMESH_MODEL_CACHE_DIR` at it
instead.

The cache is bind-mounted read-write into each container, so it persists across
`--rm` runs. The runner reports the algorithm that *actually* ran in
`result.algo` (with `fallback: true` and `fallbackReason` when it degraded), so a
template rig is never reported as an ML rig.

## How it works

1. Registers itself (name, platform, capabilities) with its token.
2. Polls `POST /v1/runner/claim`; the server only hands out jobs whose required capabilities the runner satisfies (and that the token is allowed to touch).
3. Every claim carries a **lease token**; progress pings renew it. If the machine dies, the lease expires server-side and the job is retried elsewhere — kill -9 is safe.
4. Inputs stream from the API; artifacts stream back to it. No storage credentials on the machine.
5. Untrusted assets execute in a locked-down container (`--network=none --cap-drop=ALL --memory=4g …`) or the native CLI; each job gets a fresh workdir, deleted afterwards.

## Supported operations

| operation | tool | output |
|---|---|---|
| `analyze-asset` | `qtmesh info/validate/anim --json` | `report.json` + datasheet (auto-enqueued on publish & web uploads) |
| `generate-thumbnail` | `qtmesh turntable --frames 1` | `thumbnail.png` (auto-attaches to marketplace listings) |
| `optimize-mesh` | `qtmesh fix` | `fixed-model` |
| `convert-format` | `qtmesh convert` | `converted-model` (params: `targetFormat`) |
| `generate-lods` | `qtmesh lod` | `lod1..lod3` (params: `count`, `algo`) |
| `render-turntable` / `render-sprite-sheet` | `qtmesh turntable` | sprite sheet png (+ metadata json) |

## Laptop etiquette (eligibility)

Non-ephemeral runners pause claiming while on battery (macOS), under high
load, low on disk, or when the pause file exists — checked before every
claim, logged once per state change, all thresholds configurable (table
above). In-flight jobs are never killed by eligibility; it only stops new
claims.

## GitHub Actions

`.github/workflows/batch-worker.yml` runs the same agent in batch mode every 4h (and on demand). Set the `QTMESH_RUNNER_TOKEN` repo secret using a token minted with restrictions:

```json
{ "allowPrivateAssets": false, "maxClaimBatch": 10 }
```
