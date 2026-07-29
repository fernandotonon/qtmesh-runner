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
import { mkdirSync, rmSync, writeFileSync, readFileSync, createWriteStream, existsSync, statSync, readdirSync } from 'node:fs';
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

function runProcess(cmd, args, { timeoutMs, onSpawn } = {}) {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] });
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

// Execute `qtmesh <args>` natively or in a locked-down container. Paths inside
// `args` must use {IN}/{OUT} placeholders so the docker path can remap them.
async function qtmesh(args, { inDir, outDir, timeoutMs, onSpawn }) {
  if (cfg.nativeQtmesh) {
    const mapped = args.map((a) => a.replaceAll('{IN}', inDir).replaceAll('{OUT}', outDir));
    return runProcess(cfg.nativeQtmesh, mapped, { timeoutMs, onSpawn });
  }
  const mapped = args.map((a) => a.replaceAll('{IN}', '/input').replaceAll('{OUT}', '/output'));
  return runProcess('docker', [
    'run', '--rm',
    '--network=none', '--cap-drop=ALL', '--security-opt=no-new-privileges',
    '--memory=4g', '--cpus=2', '--pids-limit=256',
    '--mount', `type=bind,src=${inDir},dst=/input,readonly`,
    '--mount', `type=bind,src=${outDir},dst=/output`,
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
    /docker API|Docker daemon|docker\.sock|Cannot connect to the Docker|command not found|No such file or directory: .*docker|ENOENT/i.test(hay);
}

function ensureExecutorOk(out, what) {
  if (isExecutorFailure(out)) {
    throw new JobError('executor_unavailable', `${what}: executor failure: ${(out.stderr || out.stdout).slice(0, 300)}`, true);
  }
}

class JobError extends Error {
  constructor(code, message, retryable) {
    super(message);
    this.code = code;
    this.retryable = retryable;
  }
}

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
};

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
    log('executor ok', { executor: `native:${cfg.nativeQtmesh}` });
    return;
  }
  const probe = await runProcess('docker', ['info', '--format', '{{.ServerVersion}}'], { timeoutMs: 20000 });
  if (probe.code !== 0) {
    console.error('Docker executor unavailable (is the daemon running?). ' +
      'Start Docker, or set QTMESH_NATIVE_QTMESH to a native qtmesh binary.\n' + (probe.stderr || probe.stdout));
    process.exit(1);
  }
  log('executor ok', { executor: `docker:${cfg.dockerImage}`, dockerServer: probe.stdout.trim() });
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

  for (;;) {
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

if (command === 'capabilities') {
  console.log(JSON.stringify({ name: cfg.name, platform: process.platform, arch: process.arch, capabilities: detectCapabilities() }, null, 2));
} else if (command === 'health') {
  const executor = cfg.nativeQtmesh ? `native:${cfg.nativeQtmesh}` : `docker:${cfg.dockerImage}`;
  console.log(JSON.stringify({ ok: Boolean(cfg.apiUrl && cfg.token), apiUrl: cfg.apiUrl, executor }, null, 2));
} else if (command === 'run') {
  mainLoop().catch((err) => { console.error(err); process.exit(1); });
} else {
  console.error(`Unknown command: ${command}. Use: run | capabilities | health`);
  process.exit(1);
}
