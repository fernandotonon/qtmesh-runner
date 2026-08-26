#!/usr/bin/env node
// QtMesh Runner agent (qtmesh-runner#1; backend: qtmesh-cloud#106).
//
// Claims async jobs from the QtMesh Cloud API over outbound HTTPS, executes
// them with the QtMeshEditor CLI (native binary or Docker image), uploads
// artifacts, and reports completion. Zero dependencies; Node >= 20.
//
// Usage:
//   qtmesh-runner run --continuous
//   qtmesh-runner run --max-jobs 10 --exit-when-empty     # CI batch mode
//   qtmesh-runner capabilities | health
//
// Env:
//   QTMESH_API_URL          e.g. https://api.qtmesh.dev (required)
//   QTMESH_RUNNER_TOKEN     qtm_run_... (required)
//   QTMESH_RUNNER_NAME      stable machine name (default: hostname)
//   QTMESH_CAPABILITIES     csv (default: cpu,quality-analysis[,ogre-render if executor found])
//   QTMESH_POLL_INTERVAL    seconds between empty claims (default 30)
//   QTMESH_WORK_DIR         scratch dir (default: os tmp)
//   QTMESH_NATIVE_QTMESH    path to a native qtmesh binary (preferred if set)
//   QTMESH_DOCKER_IMAGE     processor image (default ghcr.io/fernandotonon/qtmesh:latest)
//   QTMESH_JOB_TIMEOUT      per-job seconds (default 600)
//   QTMESH_EPHEMERAL        "1" for CI workers

import { spawn } from 'node:child_process';
import { mkdirSync, rmSync, writeFileSync, readFileSync, createWriteStream, existsSync, statSync, readdirSync, statfsSync, renameSync, lstatSync, readlinkSync, symlinkSync } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import os from 'node:os';
import path from 'node:path';

const VERSION = '0.1.0';

// ---- config -----------------------------------------------------------------

const argv = process.argv.slice(2);
const command = argv[0] || 'run';
const flag = (name) => argv.includes(name);
const flagValue = (name, dflt) => {
  const i = argv.indexOf(name);
  return i >= 0 && argv[i + 1] !== undefined ? argv[i + 1] : dflt;
};

const cfg = {
  apiUrl: (process.env.QTMESH_API_URL || flagValue('--api', '')).replace(/\/$/, ''),
  token: process.env.QTMESH_RUNNER_TOKEN || '',
  name: process.env.QTMESH_RUNNER_NAME || flagValue('--name', os.hostname()),
  pollInterval: Math.max(2, Number(process.env.QTMESH_POLL_INTERVAL || flagValue('--poll-interval', 30))),
  workDir: process.env.QTMESH_WORK_DIR || path.join(os.tmpdir(), 'qtmesh-runner'),
  nativeQtmesh: process.env.QTMESH_NATIVE_QTMESH || '',
  dockerImage: process.env.QTMESH_DOCKER_IMAGE || 'ghcr.io/fernandotonon/qtmesh:latest',
  jobTimeoutMs: Math.max(10, Number(process.env.QTMESH_JOB_TIMEOUT || 600)) * 1000,
  // Memory cap for the processor container. The ML backends load multi-GB ONNX
  // graphs (UniRig decoder alone is 1.2 GB), so the old fixed 4g cap OOM-killed
  // them on real assets. Resolved at preflight from what the Docker VM actually
  // has (see resolveJobMemory); this is only the explicit override.
  jobMemory: process.env.QTMESH_JOB_MEMORY || '',
  // Persistent host dir for the ~1.4 GB ONNX model cache (UniRig, SkinTokens).
  // Containers are --rm, so without this every ML job re-downloads the models --
  // and under --network=none it cannot download them at all and silently falls
  // back to the deterministic template backend. Set to '' to disable the mount.
  modelCacheDir: process.env.QTMESH_MODEL_CACHE_DIR !== undefined
    ? process.env.QTMESH_MODEL_CACHE_DIR
    : path.join(os.homedir(), '.qtmesh-runner', 'ai-models'),
  ephemeral: process.env.QTMESH_EPHEMERAL === '1' || flag('--ephemeral'),
  continuous: flag('--continuous'),
  exitWhenEmpty: flag('--exit-when-empty'),
  maxJobs: Number(flagValue('--max-jobs', 0)) || 0,
  heartbeatSec: 60,
};

function log(msg, extra) {
  const line = { t: new Date().toISOString(), runner: cfg.name, msg, ...extra };
  console.log(JSON.stringify(line));
}

function detectCapabilities() {
  const envCaps = (process.env.QTMESH_CAPABILITIES || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (envCaps.length) return envCaps;
  const caps = ['cpu', 'quality-analysis', 'format-conversion'];
  // The turntable renderer needs a working qtmesh executor; both the native
  // binary and the docker image ship it.
  caps.push('ogre-render');
  if (process.platform === 'darwin') caps.push('apple-silicon');
  if (process.platform === 'linux') caps.push('linux-container');
  return caps;
}

// ---- API client ---------------------------------------------------------------

let runnerId = '';

async function api(pathname, { method = 'POST', body, lease, raw } = {}) {
  const headers = { authorization: `Bearer ${cfg.token}` };
  if (runnerId) headers['x-qtmesh-runner'] = runnerId;
  if (lease) headers['x-qtmesh-lease'] = lease;
  if (body !== undefined && !raw) headers['content-type'] = 'application/json';
  const res = await fetch(cfg.apiUrl + pathname, {
    method,
    headers,
    body: body === undefined ? undefined : raw ? body : JSON.stringify(body),
  });
  if (raw === 'response') return res;
  const text = await res.text();
  let data = {};
  try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!res.ok) {
    const err = new Error(data.error || `HTTP ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return data;
}

async function register() {
  const caps = detectCapabilities();
  const out = await api('/v1/runner/register', {
    body: {
      name: cfg.name,
      platform: process.platform,
      arch: process.arch,
      capabilities: caps,
      maxConcurrent: 1,
      version: VERSION,
      ephemeral: cfg.ephemeral,
    },
  });
  runnerId = out.runnerId;
  log('registered', { runnerId, capabilities: caps });
}

// ---- executor -----------------------------------------------------------------

function runProcess(cmd, args, { timeoutMs, onSpawn, env } = {}) {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], ...(env ? { env } : {}) });
    if (onSpawn) onSpawn(child);
    let stdout = '';
    let stderr = '';
    let timedOut = false;
    const timer = timeoutMs
      ? setTimeout(() => { timedOut = true; child.kill('SIGKILL'); }, timeoutMs)
      : null;
    child.stdout.on('data', (d) => { stdout += d; if (stdout.length > 8e6) stdout = stdout.slice(-4e6); });
    child.stderr.on('data', (d) => { stderr += d; if (stderr.length > 1e6) stderr = stderr.slice(-5e5); });
    child.on('close', (code) => {
      if (timer) clearTimeout(timer);
      resolve({ code, stdout, stderr, timedOut });
    });
    child.on('error', (err) => {
      if (timer) clearTimeout(timer);
      resolve({ code: -1, stdout, stderr: String(err), timedOut });
    });
  });
}

// The image runs as uid 10001 with HOME=/home/qtmesh; qtmesh resolves its model
// cache via Qt's writable AppDataLocation, i.e. $HOME/.local/share/<org>/<app>.
const CONTAINER_HOME = '/home/qtmesh';
const MODEL_CACHE_SUBPATH = '.local/share/QtMeshEditor/QtMeshEditor/ai_models';

// The native executor cannot be bind-mounted, so point Qt's AppDataLocation at
// the warmed cache instead. Qt honours XDG_DATA_HOME on Linux; macOS resolves
// AppDataLocation under ~/Library/Application Support and ignores it, so there
// we symlink the app's ai_models dir at warm time (see linkNativeCache).
function nativeModelEnv() {
  if (!cfg.modelCacheDir || process.platform === 'darwin') return undefined;
  // Qt appends <org>/<app>/ under XDG_DATA_HOME, so hand it a private root whose
  // QtMeshEditor/QtMeshEditor/ai_models path is the warmed cache. Built by
  // warm-models via linkNativeCache(); see nativeXdgRoot().
  return { ...process.env, XDG_DATA_HOME: nativeXdgRoot() };
}

// Private XDG root whose <org>/<app>/ai_models resolves to cfg.modelCacheDir.
function nativeXdgRoot() {
  return path.join(cfg.modelCacheDir, '.xdg');
}

// Where the native binary looks for its models by default (Qt AppDataLocation).
function nativeCacheDir() {
  const root = process.platform === 'darwin'
    ? path.join(os.homedir(), 'Library', 'Application Support')
    : path.join(os.homedir(), '.local', 'share');
  return path.join(root, 'QtMeshEditor', 'QtMeshEditor', 'ai_models');
}

// Container memory cap. ML inference needs several GB, but the cap must also
// fit inside the Docker VM or the daemon refuses the run. Preflight replaces
// this with ~2/3 of the VM's memory (clamped to 4-12g); the 4g floor matches
// the pre-ML default so non-ML operations behave as before on tiny machines.
let DEFAULT_JOB_MEMORY = '4g';

async function resolveJobMemory() {
  if (cfg.jobMemory) return cfg.jobMemory;
  const probe = await runProcess('docker', ['info', '--format', '{{.MemTotal}}'], { timeoutMs: 20000 });
  const total = Number(probe.stdout.trim());
  if (!Number.isFinite(total) || total <= 0) return DEFAULT_JOB_MEMORY;
  const gb = Math.floor((total / 1073741824) * 2 / 3);
  DEFAULT_JOB_MEMORY = `${Math.max(4, Math.min(12, gb))}g`;
  return DEFAULT_JOB_MEMORY;
}

// Bind the host model cache over the container's ai_models dir so ONNX weights
// persist across --rm containers and stay usable under --network=none.
function modelCacheArgs() {
  if (!cfg.modelCacheDir) return [];
  try {
    mkdirSync(cfg.modelCacheDir, { recursive: true });
  } catch {
    return [];
  }
  return ['--mount', `type=bind,src=${cfg.modelCacheDir},dst=${CONTAINER_HOME}/${MODEL_CACHE_SUBPATH}`];
}

// Execute `qtmesh <args>` natively or in a locked-down container. Paths inside
// `args` must use {IN}/{OUT} placeholders so the docker path can remap them.
async function qtmesh(args, { inDir, outDir, timeoutMs, onSpawn }) {
  if (cfg.nativeQtmesh) {
    const mapped = args.map((a) => a.replaceAll('{IN}', inDir).replaceAll('{OUT}', outDir));
    return runProcess(cfg.nativeQtmesh, mapped, { timeoutMs, onSpawn, env: nativeModelEnv() });
  }
  const mapped = args.map((a) => a.replaceAll('{IN}', '/input').replaceAll('{OUT}', '/output'));
  return runProcess('docker', [
    'run', '--rm',
    '--network=none', '--cap-drop=ALL', '--security-opt=no-new-privileges',
    `--memory=${cfg.jobMemory || DEFAULT_JOB_MEMORY}`, '--cpus=2', '--pids-limit=256',
    '--mount', `type=bind,src=${inDir},dst=/input,readonly`,
    '--mount', `type=bind,src=${outDir},dst=/output`,
    ...modelCacheArgs(),
    cfg.dockerImage, ...mapped,
  ], { timeoutMs, onSpawn });
}

// ---- operations -----------------------------------------------------------------
// Each adapter runs the CLI and returns { artifacts: {type: filePath}, result }.
// Throwing JobError with retryable=false marks the job permanently failed.

// Environment/executor failures (docker daemon down, binary missing) must
// NEVER permafail a job — they are retryable infrastructure errors, and the
// agent stops claiming instead of draining the queue into failures.
function isExecutorFailure(out) {
  const hay = `${out.stderr || ''}\n${out.stdout || ''}`;
  return out.code === -1 ||
    /docker API|Docker daemon|docker\.sock|Cannot connect to the Docker|command not found|No such file or directory: .*docker|ENOENT/i.test(hay) ||
    // A stale executor image that predates an operation reports the subcommand
    // as unknown. The asset is fine — the runner is out of date — so this is a
    // retryable environment fault, never a permanent invalid_asset.
    /Unknown command|unrecognized (?:command|subcommand)|no such command/i.test(hay);
}

function ensureExecutorOk(out, what) {
  if (isExecutorFailure(out)) {
    throw new JobError('executor_unavailable', `${what}: executor failure: ${(out.stderr || out.stdout).slice(0, 300)}`, true);
  }
  // 128+SIGKILL(9): the kernel OOM-killer (or an operator) killed the container.
  // It dies mid-write, so stdout/stderr are empty and the failure is otherwise
  // indistinguishable from a bad asset -- which would permafail a valid job.
  // The asset is fine; the box was too small. Retryable, and say so plainly.
  if (out.code === 137 && !(out.stderr || out.stdout).trim()) {
    throw new JobError('out_of_memory',
      `${what}: killed (exit 137, no output) — likely OOM at --memory=${cfg.jobMemory || DEFAULT_JOB_MEMORY}. ` +
      'Raise QTMESH_JOB_MEMORY or route this asset to a larger runner.', true);
  }
}

// qtmesh --json prints a single JSON object, sometimes preceded by human-readable
// "Note:" lines on stdout. Take the outermost {...} span.
function parseJsonReport(stdout) {
  const text = String(stdout || '');
  const a = text.indexOf('{');
  const b = text.lastIndexOf('}');
  if (a < 0 || b <= a) return null;
  try { return JSON.parse(text.slice(a, b + 1)); } catch { return null; }
}

class JobError extends Error {
  constructor(code, message, retryable) {
    super(message);
    this.code = code;
    this.retryable = retryable;
  }
}

// qtmesh subcommands the operation adapters rely on, checked against the
// executor's --help at startup.
const OPERATION_COMMANDS = ['info', 'validate', 'anim', 'fix', 'convert', 'lod', 'turntable', 'isometric', 'rig', 'skin'];

const OPERATIONS = {
  async 'analyze-asset'(job, ctx) {
    const input = `{IN}/${ctx.inputName}`;
    const report = { analyzedAt: new Date().toISOString(), agentVersion: VERSION, operation: 'analyze-asset' };

    const info = await qtmesh(['info', input, '--json'], ctx);
    if (info.timedOut) throw new JobError('processor_timeout', 'qtmesh info timed out', true);
    ensureExecutorOk(info, 'qtmesh info');
    if (info.code !== 0) throw new JobError('invalid_asset', `qtmesh info failed: ${info.stderr.slice(0, 300)}`, false);
    try { report.info = JSON.parse(info.stdout); } catch { report.info = null; }

    // validate exits 1 when it FINDS errors — that is a successful analysis.
    const val = await qtmesh(['validate', input, '--json'], ctx);
    if (val.timedOut) throw new JobError('processor_timeout', 'qtmesh validate timed out', true);
    try { report.validation = JSON.parse(val.stdout); } catch { report.validation = null; }
    report.validationExitCode = val.code;

    const anim = await qtmesh(['anim', input, '--list', '--json'], ctx);
    try { report.animations = JSON.parse(anim.stdout); } catch { report.animations = null; }

    const reportPath = path.join(ctx.outDir, 'report.json');
    writeFileSync(reportPath, JSON.stringify(report, null, 2));

    // Distill a datasheet for the site (full detail stays in the report
    // artifact). Only REAL values from the CLI — absent data stays absent.
    const meshInfo = report.info || {};
    const anims = Array.isArray(report.animations) ? report.animations
      : Array.isArray(meshInfo.animations) ? meshInfo.animations : [];
    const matName = (m) => typeof m === 'string' ? m : (m && (m.name || m.material)) ? String(m.name || m.material) : null;
    const datasheet = {
      vertices: typeof meshInfo.vertices === 'number' ? meshInfo.vertices : null,
      triangles: typeof meshInfo.triangles === 'number' ? meshInfo.triangles : null,
      submeshes: typeof meshInfo.submeshes === 'number' ? meshInfo.submeshes : null,
      upAxis: typeof meshInfo.upAxis === 'string' ? meshInfo.upAxis : null,
      boundingBox: meshInfo.boundingBox && Array.isArray(meshInfo.boundingBox.min) ? meshInfo.boundingBox : null,
      materials: Array.isArray(meshInfo.materials) ? meshInfo.materials.map(matName).filter(Boolean).slice(0, 50) : [],
      textures: Array.isArray(meshInfo.textures) ? meshInfo.textures.map(String).slice(0, 50) : [],
      boneCount: meshInfo.skeleton && typeof meshInfo.skeleton.boneCount === 'number' ? meshInfo.skeleton.boneCount : null,
      skeletonName: meshInfo.skeleton && meshInfo.skeleton.name ? String(meshInfo.skeleton.name) : null,
      animations: anims
        .filter((a) => a && a.name)
        .slice(0, 100)
        .map((a) => ({ name: String(a.name).slice(0, 120), duration: typeof a.duration === 'number' ? Math.round(a.duration * 100) / 100 : null })),
      // Full validation check list (error/warning/info/ok) so the site can
      // show WHAT the issues are, not just that they exist.
      validation: Array.isArray(report.validation)
        ? report.validation.slice(0, 30).map((chk) => ({
            type: String(chk && chk.type || 'info').slice(0, 12),
            description: String(chk && chk.description || '').slice(0, 200),
            count: typeof (chk && chk.count) === 'number' ? chk.count : null,
            fixable: Boolean(chk && chk.fixable),
          }))
        : [],
      hasErrors: val.code !== 0,
    };
    return {
      artifacts: { report: reportPath },
      result: {
        analyzed: true,
        hasErrors: val.code !== 0,
        datasheet,
      },
    };
  },

  async 'generate-thumbnail'(job, ctx) {
    const input = `{IN}/${ctx.inputName}`;
    const size = typeof job.params?.size === 'string' && /^\d{2,4}x\d{2,4}$/.test(job.params.size)
      ? job.params.size : '512x512';
    const out = await qtmesh(['turntable', input, '-o', '{OUT}/thumbnail.png', '--frames', '1', '--size', size], ctx);
    if (out.timedOut) throw new JobError('processor_timeout', 'turntable timed out', true);
    ensureExecutorOk(out, 'turntable');
    if (out.code !== 0) throw new JobError('render_failed', `turntable failed: ${(out.stderr || out.stdout).slice(0, 300)}`, false);
    const thumbPath = path.join(ctx.outDir, 'thumbnail.png');
    if (!existsSync(thumbPath)) throw new JobError('render_failed', 'turntable produced no output', false);
    return { artifacts: { thumbnail: thumbPath }, result: { rendered: true, size } };
  },

  async 'optimize-mesh'(job, ctx) {
    const input = `{IN}/${ctx.inputName}`;
    const ext = (path.extname(ctx.inputName).slice(1) || 'fbx').toLowerCase();
    const out = await qtmesh(['fix', input, '-o', `{OUT}/fixed.${ext}`], ctx);
    if (out.timedOut) throw new JobError('processor_timeout', 'qtmesh fix timed out', true);
    ensureExecutorOk(out, 'qtmesh fix');
    if (out.code !== 0) throw new JobError('invalid_asset', `qtmesh fix failed: ${(out.stderr || out.stdout).slice(0, 300)}`, false);
    const fixedPath = path.join(ctx.outDir, `fixed.${ext}`);
    if (!existsSync(fixedPath)) throw new JobError('invalid_asset', 'fix produced no output', false);
    return { artifacts: { 'fixed-model': fixedPath }, result: { fixed: true, summary: out.stdout.slice(0, 500) } };
  },

  async 'convert-format'(job, ctx) {
    const input = `{IN}/${ctx.inputName}`;
    const target = String(job.params?.targetFormat || 'glb').toLowerCase().replace(/[^a-z]/g, '') || 'glb';
    const out = await qtmesh(['convert', input, '-o', `{OUT}/converted.${target}`], ctx);
    if (out.timedOut) throw new JobError('processor_timeout', 'qtmesh convert timed out', true);
    ensureExecutorOk(out, 'qtmesh convert');
    if (out.code !== 0) throw new JobError('unsupported_format', `convert failed: ${(out.stderr || out.stdout).slice(0, 300)}`, false);
    const convertedPath = path.join(ctx.outDir, `converted.${target}`);
    if (!existsSync(convertedPath)) throw new JobError('unsupported_format', 'convert produced no output', false);
    return { artifacts: { 'converted-model': convertedPath }, result: { targetFormat: target } };
  },

  async 'generate-lods'(job, ctx) {
    const input = `{IN}/${ctx.inputName}`;
    const ext = (path.extname(ctx.inputName).slice(1) || 'fbx').toLowerCase();
    const count = Math.min(3, Math.max(1, parseInt(job.params?.count, 10) || 2));
    const algo = ['ogre', 'meshopt'].includes(String(job.params?.algo)) ? String(job.params.algo) : 'meshopt';
    const out = await qtmesh(['lod', input, '--count', String(count), '--algo', algo, '-o', `{OUT}/model.${ext}`], ctx);
    if (out.timedOut) throw new JobError('processor_timeout', 'qtmesh lod timed out', true);
    ensureExecutorOk(out, 'qtmesh lod');
    if (out.code !== 0) throw new JobError('invalid_asset', `lod failed: ${(out.stderr || out.stdout).slice(0, 300)}`, false);
    const artifacts = {};
    for (let i = 1; i <= count; i++) {
      const lodPath = path.join(ctx.outDir, `model_lod${i}.${ext}`);
      if (existsSync(lodPath)) artifacts[`lod${i}`] = lodPath;
    }
    if (!artifacts.lod1) throw new JobError('invalid_asset', 'lod produced no output', false);
    return { artifacts, result: { count: Object.keys(artifacts).length, algo } };
  },

  async 'render-turntable'(job, ctx) {
    return renderSheet(job, ctx, 'turntable', { frames: 12, columns: 12 });
  },

  // Sprite sheets use `qtmesh isometric` (rows = directions, cols = frames)
  // — a different renderer from turntable. `animation` passes through when
  // provided (currently gated in the UI by QtMeshEditor#936).
  async 'render-sprite-sheet'(job, ctx) {
    const input = `{IN}/${ctx.inputName}`;
    const p = job.params || {};
    const directions = Math.min(8, Math.max(1, parseInt(p.directions, 10) || 8));
    const frames = Math.min(36, Math.max(1, parseInt(p.frames, 10) || 1));
    const size = typeof p.size === 'string' && /^\d{2,4}x\d{2,4}$/.test(p.size) ? p.size : '256x256';
    const args = ['isometric', input, '-o', '{OUT}/sheet.png', '--directions', String(directions), '--frames', String(frames), '--size', size];
    if (typeof p.animation === 'string' && p.animation) args.push('--animation', p.animation);
    if (p.elevation !== undefined && !isNaN(Number(p.elevation))) args.push('--elevation', String(Number(p.elevation)));
    const out = await qtmesh(args, ctx);
    if (out.timedOut) throw new JobError('processor_timeout', 'isometric timed out', true);
    ensureExecutorOk(out, 'isometric');
    if (out.code !== 0) throw new JobError('render_failed', `isometric failed: ${(out.stderr || out.stdout).slice(0, 300)}`, false);
    const sheetPath = path.join(ctx.outDir, 'sheet.png');
    if (!existsSync(sheetPath)) throw new JobError('render_failed', 'isometric produced no output', false);
    const metaPath = path.join(ctx.outDir, 'sprite-metadata.json');
    writeFileSync(metaPath, JSON.stringify({ directions, frames, animation: p.animation || null, frameSize: size, layout: 'rows=directions, cols=frames' }, null, 2));
    return { artifacts: { 'sprite-sheet': sheetPath, 'sprite-metadata': metaPath }, result: { directions, frames, animation: p.animation || null, size } };
  },

  // Auto-rig a static mesh (qtmesh rig, UniRig backend). Optional --skin to
  // also compute weights in the same pass. ONNX inference is slow (~tens of s).
  async 'auto-rig'(job, ctx) {
    const input = `{IN}/${ctx.inputName}`;
    const p = job.params || {};
    // Always output GLB: it carries the skeleton (and skin weights) — OBJ/STL
    // cannot, so a rig written back to the input format would silently drop it.
    const skeleton = ['humanoid', 'biped', 'quadruped', 'generic'].includes(String(p.skeleton)) ? String(p.skeleton) : 'humanoid';
    const algo = ['unirig', 'pinocchio'].includes(String(p.algo)) ? String(p.algo) : 'unirig';
    const args = ['rig', input, '--skeleton', skeleton, '--algo', algo, '--json', '-o', '{OUT}/rigged.glb'];
    if (p.skin === true) args.push('--skin');
    const out = await qtmesh(args, ctx);
    if (out.timedOut) throw new JobError('processor_timeout', 'qtmesh rig timed out', true);
    ensureExecutorOk(out, 'qtmesh rig');
    if (out.code !== 0) throw new JobError('invalid_asset', `rig failed: ${(out.stderr || out.stdout).slice(0, 300)}`, false);
    const riggedPath = path.join(ctx.outDir, 'rigged.glb');
    if (!existsSync(riggedPath)) throw new JobError('invalid_asset', 'rig produced no output', false);
    // `--algo unirig` silently degrades to the deterministic template backend
    // when the ONNX weights are missing, still exiting 0. Report the algorithm
    // that actually ran (never the requested one) so a template rig is never
    // passed off as an ML rig, and surface the reason.
    const report = parseJsonReport(out.stdout);
    const algoUsed = typeof report?.algorithm === 'string' ? report.algorithm : algo;
    const fallbackReason = typeof report?.fallbackReason === 'string' ? report.fallbackReason : null;
    if (fallbackReason) log('rig backend fell back', { jobId: job.id, requested: algo, used: algoUsed, reason: fallbackReason });
    return {
      artifacts: { 'rigged-model': riggedPath },
      result: {
        skeleton, algo: algoUsed, algoRequested: algo, skinned: p.skin === true,
        ...(fallbackReason ? { fallback: true, fallbackReason } : {}),
        ...(Number.isFinite(report?.boneCount) ? { boneCount: report.boneCount } : {}),
        summary: out.stdout.slice(0, 500),
      },
    };
  },

  // Compute skin weights on a rigged mesh (qtmesh skin, SkinTokens ML backend;
  // falls back to geodesic-voxel if the ~2.3 GB models aren't available).
  // Outputs GLB so the skeleton + computed weights survive.
  async 'skin-model'(job, ctx) {
    const input = `{IN}/${ctx.inputName}`;
    const p = job.params || {};
    const algo = ['skintokens', 'geodesic-voxel', 'inverse-distance'].includes(String(p.algo)) ? String(p.algo) : 'skintokens';
    const args = ['skin', input, '--algo', algo, '--json', '-o', '{OUT}/skinned.glb'];
    if (p.maxInfluences !== undefined && Number.isInteger(Number(p.maxInfluences))) args.push('--max-influences', String(Number(p.maxInfluences)));
    const out = await qtmesh(args, ctx);
    if (out.timedOut) throw new JobError('processor_timeout', 'qtmesh skin timed out', true);
    ensureExecutorOk(out, 'qtmesh skin');
    if (out.code !== 0) throw new JobError('invalid_asset', `skin failed: ${(out.stderr || out.stdout).slice(0, 300)}`, false);
    const skinnedPath = path.join(ctx.outDir, 'skinned.glb');
    if (!existsSync(skinnedPath)) throw new JobError('invalid_asset', 'skin produced no output', false);
    // Same silent-degradation contract as auto-rig: 'skintokens' falls back to
    // geodesic-voxel when the ~2.3 GB models are unavailable, still exiting 0.
    // NB: `skin --json` names the field `algorithmUsed`, while `rig` uses
    // `algorithm`; accept either so a fallback is never missed.
    const report = parseJsonReport(out.stdout);
    const algoUsed = typeof report?.algorithmUsed === 'string' ? report.algorithmUsed
      : typeof report?.algorithm === 'string' ? report.algorithm : algo;
    const fallbackReason = typeof report?.fallbackReason === 'string' ? report.fallbackReason : null;
    if (fallbackReason) log('skin backend fell back', { jobId: job.id, requested: algo, used: algoUsed, reason: fallbackReason });
    return {
      artifacts: { 'skinned-model': skinnedPath },
      result: {
        algo: algoUsed, algoRequested: algo,
        ...(fallbackReason ? { fallback: true, fallbackReason } : {}),
        summary: out.stdout.slice(0, 500),
      },
    };
  },
};

// Shared turntable-based sheet renderer for render-turntable / render-sprite-sheet.
async function renderSheet(job, ctx, artifactType, defaults) {
  const input = `{IN}/${ctx.inputName}`;
  const p = job.params || {};
  const frames = Math.min(36, Math.max(1, parseInt(p.frames, 10) || defaults.frames));
  const columns = Math.min(12, Math.max(1, parseInt(p.columns, 10) || defaults.columns));
  const size = typeof p.size === 'string' && /^\d{2,4}x\d{2,4}$/.test(p.size) ? p.size : '256x256';
  const args = ['turntable', input, '-o', '{OUT}/sheet.png', '--frames', String(frames), '--columns', String(columns), '--size', size];
  if (p.elevation !== undefined && !isNaN(Number(p.elevation))) args.push('--elevation', String(Number(p.elevation)));
  const out = await qtmesh(args, ctx);
  if (out.timedOut) throw new JobError('processor_timeout', 'turntable timed out', true);
  ensureExecutorOk(out, 'turntable');
  if (out.code !== 0) throw new JobError('render_failed', `turntable failed: ${(out.stderr || out.stdout).slice(0, 300)}`, false);
  const sheetPath = path.join(ctx.outDir, 'sheet.png');
  if (!existsSync(sheetPath)) throw new JobError('render_failed', 'turntable produced no output', false);
  const artifacts = { [artifactType]: sheetPath };
  if (artifactType === 'sprite-sheet') {
    const metaPath = path.join(ctx.outDir, 'sprite-metadata.json');
    writeFileSync(metaPath, JSON.stringify({ frames, columns, rows: Math.ceil(frames / columns), frameSize: size }, null, 2));
    artifacts['sprite-metadata'] = metaPath;
  }
  return { artifacts, result: { frames, columns, size } };
}

// ---- job processing -----------------------------------------------------------

async function processJob(claim) {
  const { job, leaseToken } = claim;
  const jobDir = path.join(cfg.workDir, `job-${job.id}`);
  const inDir = path.join(jobDir, 'in');
  const outDir = path.join(jobDir, 'out');
  rmSync(jobDir, { recursive: true, force: true });
  mkdirSync(inDir, { recursive: true });
  mkdirSync(outDir, { recursive: true });

  let child = null;
  let cancelled = false;
  // Progress ping doubles as lease renewal; response tells us to abort.
  const renew = setInterval(async () => {
    try {
      const r = await api(`/v1/runner/jobs/${job.id}/progress`, { lease: leaseToken, body: { stage: 'processing' } });
      if (r.cancelRequested) {
        cancelled = true;
        if (child) child.kill('SIGKILL');
      }
    } catch (e) {
      log('lease renewal failed', { jobId: job.id, error: String(e.message) });
    }
  }, Math.min(60, cfg.pollInterval * 2) * 1000);

  try {
    log('job start', { jobId: job.id, operation: job.operation, attempt: job.attempt });

    // Download input.
    const inputName = job.input.filename.replace(/[^A-Za-z0-9._-]/g, '_') || `input.${job.input.extension || 'bin'}`;
    const res = await api(job.input.downloadPath, { method: 'GET', lease: leaseToken, raw: 'response' });
    if (res.status === 410) throw new JobError('input_missing', 'Job input no longer exists', false);
    if (!res.ok) throw new JobError('temporary_storage_error', `input download HTTP ${res.status}`, true);
    await pipeline(Readable.fromWeb(res.body), createWriteStream(path.join(inDir, inputName)));

    const adapter = OPERATIONS[job.operation];
    if (!adapter) throw new JobError('not_implemented', `Operation ${job.operation} not supported by this agent`, false);

    const ctx = { inDir, outDir, inputName, timeoutMs: cfg.jobTimeoutMs, onSpawn: (ch) => { child = ch; } };
    const outcome = await adapter(job, ctx);
    if (cancelled) throw new JobError('cancelled', 'Cancelled by user', false);

    // Upload artifacts against the declared slots.
    for (const [type, filePath] of Object.entries(outcome.artifacts)) {
      const ext = path.extname(filePath).slice(1).toLowerCase();
      const bytes = readFileSync(filePath);
      const up = await fetch(`${cfg.apiUrl}/v1/runner/jobs/${job.id}/artifacts/${type}?ext=${ext}`, {
        method: 'PUT',
        headers: {
          authorization: `Bearer ${cfg.token}`,
          'x-qtmesh-runner': runnerId,
          'x-qtmesh-lease': leaseToken,
          'content-type': ext === 'json' ? 'application/json' : ext === 'txt' ? 'text/plain' : `image/${ext === 'jpg' ? 'jpeg' : ext}`,
          'content-length': String(bytes.length),
        },
        body: bytes,
      });
      if (!up.ok) throw new JobError('temporary_upload_error', `artifact ${type} upload HTTP ${up.status}`, true);
    }

    await api(`/v1/runner/jobs/${job.id}/complete`, { lease: leaseToken, body: { result: outcome.result } });
    log('job completed', { jobId: job.id, operation: job.operation });
    return true;
  } catch (err) {
    const code = err instanceof JobError ? err.code : 'processor_error';
    const retryable = err instanceof JobError ? err.retryable : true;
    log('job failed', { jobId: job.id, errorCode: code, retryable, error: String(err.message).slice(0, 300) });
    try {
      await api(`/v1/runner/jobs/${job.id}/fail`, { lease: leaseToken, body: { errorCode: code, message: String(err.message).slice(0, 400), retryable } });
    } catch (e) {
      log('fail report rejected (lease lost?)', { jobId: job.id, error: String(e.message) });
    }
    if (code === 'executor_unavailable') executorBroken = true;
    return false;
  } finally {
    clearInterval(renew);
    rmSync(jobDir, { recursive: true, force: true });
  }
}

// ---- main loop ------------------------------------------------------------------

let executorBroken = false;

// ---- eligibility (personal machines) ----------------------------------------
// A laptop should not silently chew battery/CPU: unless the runner is
// ephemeral (CI) or checks are disabled, claiming pauses while on battery,
// under load, low on disk, or manually paused. All thresholds configurable.
const elig = {
  disabled: process.env.QTMESH_ELIGIBILITY === 'off',
  requireAC: process.env.QTMESH_REQUIRE_AC !== '0' && process.platform === 'darwin',
  maxLoad: Number(process.env.QTMESH_MAX_LOAD || os.cpus().length),
  minFreeGb: Number(process.env.QTMESH_MIN_FREE_GB || 5),
  pauseFile: process.env.QTMESH_PAUSE_FILE || path.join(os.homedir(), '.qtmesh-runner', 'paused'),
};

async function eligibilityBlockReason() {
  if (elig.disabled || cfg.ephemeral) return null;
  if (existsSync(elig.pauseFile)) return `paused (${elig.pauseFile} exists)`;
  const load = os.loadavg()[0];
  if (load > elig.maxLoad) return `system load ${load.toFixed(1)} > ${elig.maxLoad}`;
  try {
    const st = statfsSync(cfg.workDir);
    const freeGb = (st.bavail * st.bsize) / 1e9;
    if (freeGb < elig.minFreeGb) return `free disk ${freeGb.toFixed(1)}GB < ${elig.minFreeGb}GB`;
  } catch { /* statfs unsupported — skip the disk check */ }
  if (elig.requireAC) {
    const batt = await runProcess('pmset', ['-g', 'batt'], { timeoutMs: 5000 });
    if (batt.code === 0 && /Battery Power/i.test(batt.stdout)) return 'on battery power';
  }
  return null;
}

// Refuse to claim anything if the executor can't run — a broken executor must
// never convert queued jobs into failures.
async function preflightExecutor() {
  if (cfg.nativeQtmesh) {
    if (!existsSync(cfg.nativeQtmesh)) {
      console.error(`QTMESH_NATIVE_QTMESH points to a missing binary: ${cfg.nativeQtmesh}`);
      process.exit(1);
    }
    const probe = await runProcess(cfg.nativeQtmesh, ['--help'], { timeoutMs: 15000 });
    if (probe.code === -1) {
      console.error(`Native qtmesh is not executable: ${probe.stderr}`);
      process.exit(1);
    }
    log('executor ok', { executor: `native:${cfg.nativeQtmesh}`, ...(await executorVersion()) });
    return;
  }
  const probe = await runProcess('docker', ['info', '--format', '{{.ServerVersion}}'], { timeoutMs: 20000 });
  if (probe.code !== 0) {
    console.error('Docker executor unavailable (is the daemon running?). ' +
      'Start Docker, or set QTMESH_NATIVE_QTMESH to a native qtmesh binary.\n' + (probe.stderr || probe.stdout));
    process.exit(1);
  }
  log('executor ok', {
    executor: `docker:${cfg.dockerImage}`,
    dockerServer: probe.stdout.trim(),
    jobMemory: await resolveJobMemory(),
    ...(await executorVersion()),
  });
}

// Report the qtmesh build the executor actually runs, plus the operations it
// supports. A stale image silently lacks newer subcommands (e.g. `rig`), so
// surfacing this at startup makes "runner needs a docker pull" obvious instead
// of only showing up as a mid-job failure.
async function executorVersion() {
  const run = (args) => cfg.nativeQtmesh
    ? runProcess(cfg.nativeQtmesh, args, { timeoutMs: 30000 })
    : runProcess('docker', ['run', '--rm', '--network=none', cfg.dockerImage, ...args], { timeoutMs: 60000 });
  try {
    const [ver, help] = await Promise.all([run(['--version']), run(['--help'])]);
    const qtmeshVersion = (ver.stdout || '').trim().split('\n')[0] || null;
    const hay = `${help.stdout || ''}\n${help.stderr || ''}`;
    const missing = OPERATION_COMMANDS.filter((c) => !new RegExp(`^\\s+${c}\\s`, 'm').test(hay));
    if (missing.length) {
      console.error(`WARNING: executor is missing subcommands: ${missing.join(', ')}. ` +
        (cfg.nativeQtmesh ? 'Update the native qtmesh binary.' : `Run: docker pull ${cfg.dockerImage}`));
    }
    return { qtmeshVersion, ...(missing.length ? { missingCommands: missing } : {}) };
  } catch {
    return {};
  }
}

async function mainLoop() {
  if (!cfg.apiUrl || !cfg.token) {
    console.error('QTMESH_API_URL and QTMESH_RUNNER_TOKEN are required');
    process.exit(1);
  }
  mkdirSync(cfg.workDir, { recursive: true });
  await preflightExecutor();
  await register();

  const heartbeat = setInterval(async () => {
    try { await api('/v1/runner/heartbeat', { body: { runnerId } }); } catch { /* transient */ }
  }, cfg.heartbeatSec * 1000);

  let processed = 0;
  const sleep = (s) => new Promise((r) => setTimeout(r, s * 1000));

  let lastBlockReason = '';
  for (;;) {
    const blockReason = await eligibilityBlockReason();
    if (blockReason) {
      if (blockReason !== lastBlockReason) log('claiming paused', { reason: blockReason });
      lastBlockReason = blockReason;
      if (cfg.exitWhenEmpty) break;
      await sleep(cfg.pollInterval);
      continue;
    }
    if (lastBlockReason) { log('claiming resumed', {}); lastBlockReason = ''; }
    let claim = null;
    try {
      claim = await api('/v1/runner/claim', { body: { runnerId } });
    } catch (err) {
      log('claim failed', { error: String(err.message) });
      await sleep(cfg.pollInterval);
      continue;
    }
    if (!claim.job) {
      if (cfg.exitWhenEmpty) break;
      await sleep(cfg.pollInterval);
      continue;
    }
    await processJob(claim);
    processed++;
    if (executorBroken) {
      log('executor became unavailable — stopping so jobs are not drained into retries', {});
      clearInterval(heartbeat);
      process.exit(2); // service supervisors restart us; jobs requeue via lease/retry
    }
    if (cfg.maxJobs && processed >= cfg.maxJobs) break;
  }

  clearInterval(heartbeat);
  log('exiting', { processed });
}

// ---- entry ----------------------------------------------------------------------

// Pre-download the ONNX weights into the persistent model cache. Jobs run with
// --network=none, so the models can only be fetched here, out of band. Verifies
// Content-Length and writes via a .part rename so a truncated download can never
// masquerade as a complete model.
// Multi-GB transfers get dropped; resume rather than restart.
const MODEL_FETCH_RETRIES = Math.max(1, Number(process.env.QTMESH_MODEL_FETCH_RETRIES || 5));

const MODEL_SETS = {
  unirig: {
    baseUrl: process.env.QTMESH_UNIRIG_MODEL_BASE_URL ||
      'https://huggingface.co/fernandotonon/QtMeshEditor-models/resolve/main/unirig/',
    files: ['embed.onnx', 'encoder.onnx', 'decoder.onnx'],
  },
  // ~2.3 GB. decoder.onnx is a 1.4 MB graph whose weights live in the external
  // decoder.onnx.data sidecar — both are required or the backend won't load.
  skintokens: {
    baseUrl: process.env.QTMESH_SKINTOKENS_MODEL_BASE_URL ||
      'https://huggingface.co/fernandotonon/QtMeshEditor-models/resolve/main/skintokens/',
    files: [
      'skintokens.json', 'embed.onnx', 'mesh_cond.onnx', 'vae_cond.onnx',
      'skin_decode.onnx', 'decoder.onnx', 'decoder.onnx.data',
    ],
  },
};

// The native executor reads Qt's AppDataLocation directly. On Linux we can
// redirect it with XDG_DATA_HOME at spawn time; macOS ignores that, so link the
// warmed cache into place instead. Never clobber a real directory that already
// holds models -- only create the link, or replace a link we own.
function linkNativeCache() {
  const target = process.platform === 'darwin'
    ? nativeCacheDir()
    : path.join(nativeXdgRoot(), 'QtMeshEditor', 'QtMeshEditor', 'ai_models');
  try {
    const existing = lstatSync(target, { throwIfNoEntry: false });
    if (existing?.isSymbolicLink()) {
      if (readlinkSync(target) === cfg.modelCacheDir) return;
      rmSync(target, { force: true });
    } else if (existing) {
      console.error(`Native model cache ${target} already exists and is not a symlink. ` +
        `Point QTMESH_MODEL_CACHE_DIR at it, or move it aside, so warmed models are actually used.`);
      return;
    }
    mkdirSync(path.dirname(target), { recursive: true });
    symlinkSync(cfg.modelCacheDir, target, 'dir');
    log('native model cache linked', { from: target, to: cfg.modelCacheDir });
  } catch (err) {
    console.error(`Could not link the native model cache (${target}): ${err.message}`);
  }
}

async function warmModels() {
  if (!cfg.modelCacheDir) {
    console.error('QTMESH_MODEL_CACHE_DIR is empty — nothing to warm.');
    process.exit(1);
  }
  const only = flagValue('--set', '');
  const sets = only ? [only] : Object.keys(MODEL_SETS);
  let failed = 0;
  for (const name of sets) {
    const set = MODEL_SETS[name];
    if (!set) { console.error(`Unknown model set: ${name}`); failed++; continue; }
    const dir = path.join(cfg.modelCacheDir, name);
    mkdirSync(dir, { recursive: true });
    for (const file of set.files) {
      const dest = path.join(dir, file);
      const url = new URL(file, set.baseUrl).toString();
      try {
        const head = await fetch(url, { method: 'HEAD', redirect: 'follow' });
        const expected = Number(head.headers.get('content-length')) || 0;
        if (!head.ok) throw new Error(`HTTP ${head.status}`);
        // Some assets are served without a Content-Length (chunked); for those
        // a non-empty cached file is the best signal we have.
        if (existsSync(dest) && (expected ? statSync(dest).size === expected : statSync(dest).size > 0)) {
          log('model cached', { set: name, file, bytes: statSync(dest).size });
          continue;
        }
        log('model downloading', { set: name, file, bytes: expected || null });
        const tmp = `${dest}.part`;
        // These are multi-GB transfers; a dropped connection is normal, not
        // exceptional. Resume from the .part offset with a Range request
        // instead of restarting from zero.
        //
        // A leftover .part that is at least as large as the upstream file
        // cannot be a valid prefix of it (upstream was replaced by a smaller
        // build). Resuming from that offset would request past EOF and get 416
        // forever, so the cache could never self-heal -- discard it and restart.
        if (expected && existsSync(tmp) && statSync(tmp).size >= expected) {
          log('model discarding stale partial', { set: name, file, bytes: statSync(tmp).size, expected });
          rmSync(tmp, { force: true });
        }
        try {
          let attempt = 0;
          for (;;) {
            attempt++;
            const have = existsSync(tmp) ? statSync(tmp).size : 0;
            if (expected && have === expected) break;
            try {
              const headers = have > 0 ? { Range: `bytes=${have}-` } : {};
              const res = await fetch(url, { redirect: 'follow', headers });
              if (!res.ok) throw new Error(`HTTP ${res.status}`);
              // A server that ignores Range replies 200 and restarts the stream.
              const append = have > 0 && res.status === 206;
              await pipeline(res.body, createWriteStream(tmp, append ? { flags: 'a' } : {}));
            } catch (err) {
              if (attempt >= MODEL_FETCH_RETRIES) throw err;
              log('model download interrupted, resuming', { set: name, file, attempt, bytes: existsSync(tmp) ? statSync(tmp).size : 0 });
              continue;
            }
            const size = statSync(tmp).size;
            if (!expected || size === expected) break;
            if (attempt >= MODEL_FETCH_RETRIES) throw new Error(`size mismatch: got ${size}, expected ${expected}`);
          }
          const got = statSync(tmp).size;
          if (expected && got !== expected) throw new Error(`size mismatch: got ${got}, expected ${expected}`);
          renameSync(tmp, dest);
          log('model ready', { set: name, file, bytes: got });
        } catch (err) {
          // Never leave a partial that later runs would resume from blindly.
          if (existsSync(tmp) && (!expected || statSync(tmp).size >= expected)) rmSync(tmp, { force: true });
          throw err;
        }
      } catch (err) {
        console.error(`FAILED ${name}/${file}: ${err.message}`);
        failed++;
      }
    }
  }
  if (failed) {
    console.error(`${failed} model file(s) failed. ML backends will fall back to the deterministic template.`);
    process.exit(1);
  }
  if (cfg.nativeQtmesh) linkNativeCache();
  log('models warm', { cacheDir: cfg.modelCacheDir, sets });
}

if (command === 'capabilities') {
  console.log(JSON.stringify({ name: cfg.name, platform: process.platform, arch: process.arch, capabilities: detectCapabilities() }, null, 2));
} else if (command === 'health') {
  const executor = cfg.nativeQtmesh ? `native:${cfg.nativeQtmesh}` : `docker:${cfg.dockerImage}`;
  console.log(JSON.stringify({ ok: Boolean(cfg.apiUrl && cfg.token), apiUrl: cfg.apiUrl, executor }, null, 2));
} else if (command === 'warm-models') {
  warmModels().catch((err) => { console.error(err); process.exit(1); });
} else if (command === 'run') {
  mainLoop().catch((err) => { console.error(err); process.exit(1); });
} else {
  console.error(`Unknown command: ${command}. Use: run | capabilities | health | warm-models`);
  process.exit(1);
}
