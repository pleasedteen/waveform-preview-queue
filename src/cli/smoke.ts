#!/usr/bin/env node
/**
 * Live demonstration / observability tool.
 *
 * Runs a queue, submits the bundled fixtures and two real worker processes
 * (in-process Worker loops over HTTP), then prints a timeline that makes the
 * two things the team cares about directly measurable:
 *
 *   - takeover latency: time from "a recording became runnable again"
 *     (reader ack freeing space, or lease expiry) to "a worker claimed it";
 *   - idle operations: how many claim scans + renewal checks the service
 *     performed while every unfinished recording was paused and no new input
 *     arrived (must be <= 2 per 4 s window).
 *
 *   npm run smoke            # default: 30 s scenario
 *   npm run smoke -- --fast  # shorter, for CI
 */

import path from 'node:path';
import fs from 'node:fs';
import { startHarness, probesBetween, lastTakeover, type Harness } from '../harness';
import { loadWav } from '../decoder/wav';
import { decodeWhole } from '../decoder/mock';

const fixtureDir = path.resolve(__dirname, '../../../fixtures');

function banner(text: string): void {
  process.stdout.write(`\n=== ${text} ===\n`);
}

async function scenarioTakeover(h: Harness): Promise<void> {
  banner('1. takeover after a stalled producer (lease expiry)');
  h.submit({ id: 'stalled-demo', path: path.join(fixtureDir, 'long-30s.wav'), segmentMs: 500, ackWindow: 100 });
  const stuck = await h.client.post<{ grant: { recording: { id: string } } }>('/claim', {
    workerId: 'stalled',
    waitMs: 0,
  });
  process.stdout.write(`stalled worker holds ${stuck.grant.recording.id} and stops renewing\n`);
  const w = h.worker('backup', { decodeMs: 4 });
  w.start();
  const started = Date.now();
  await h.waitFor(() => h.get('stalled-demo').progress >= 2, 8000, 'takeover');
  const takeover = lastTakeover(h, 'stalled-demo');
  process.stdout.write(
    `backup worker took over after ${Date.now() - started} ms ` +
      `(recorded latency ${takeover ? takeover.latencyMs + ' ms, reason ' + takeover.reason : 'n/a'})\n`,
  );
  await h.waitFor(() => h.get('stalled-demo').state === 'completed', 15_000, 'completion');
  const wav = loadWav(path.join(fixtureDir, 'long-30s.wav'));
  const expected = await decodeWhole(wav, 500, h.get('stalled-demo').totalSegments);
  const same = h
    .segments('stalled-demo')
    .every((s, i) => s.digest === expected[i]!.digest);
  process.stdout.write(`all ${expected.length} segments match a single-pass decode: ${same}\n`);
  await w.stop();
}

async function scenarioPause(h: Harness): Promise<void> {
  banner('2. reader ack frees space; takeover must be <= 500 ms');
  h.submit({ id: 'paused-demo', path: path.join(fixtureDir, 'long-60s.wav'), segmentMs: 500, ackWindow: 2 });
  h.submit({ id: 'free-demo', path: path.join(fixtureDir, 'long-30s.wav'), segmentMs: 500, ackWindow: 200 });
  const worker = h.worker('solo', { decodeMs: 10 });
  worker.start();
  await h.waitFor(() => h.get('paused-demo').paused, 8000, 'paused-demo to pause');
  process.stdout.write(
    `paused-demo paused at progress=${h.get('paused-demo').progress} ` +
      `(cap ${h.get('paused-demo').ackWindow}); the same worker keeps doing free-demo\n`,
  );
  await h.waitFor(() => h.get('free-demo').state === 'completed', 20_000, 'free-demo completion');
  const ackCursor = h.get('paused-demo').progress;
  const at = Date.now();
  await h.ack('paused-demo', ackCursor);
  await h.waitFor(() => h.get('paused-demo').progress > ackCursor, 500, 'takeover after ack');
  const takeover = lastTakeover(h, 'paused-demo');
  process.stdout.write(
    `after ack at ${ackCursor}: progress resumed after ${Date.now() - at} ms ` +
      `(recorded latency ${takeover?.latencyMs ?? 'n/a'} ms)\n`,
  );
  await worker.stop();
}

async function scenarioIdle(h: Harness): Promise<void> {
  banner('3. idle budget with every incomplete recording paused');
  h.submit({ id: 'idle-a', path: path.join(fixtureDir, 'long-30s.wav'), segmentMs: 500, ackWindow: 1 });
  h.submit({ id: 'idle-b', path: path.join(fixtureDir, 'long-30s.wav'), segmentMs: 500, ackWindow: 1 });
  const w1 = h.worker('idle-1', { decodeMs: 20 });
  const w2 = h.worker('idle-2', { decodeMs: 20 });
  w1.start();
  w2.start();
  await h.waitFor(
    () => h.list().every((r) => r.id.startsWith('idle') ? r.paused : true) && h.stats().idleWorkers.length === 2,
    15_000,
    'both paused and both workers parked',
  );
  // Wait for the transition into steady state (workers parking after the last
  // claim, expiry timers drained) before measuring the idle budget.
  for (;;) {
    const events = h.stats().probes.events;
    const last = events.length ? events[events.length - 1]!.at : 0;
    if (Date.now() - last >= 1200) break;
    await h.sleep(100);
  }
  const stats = h.stats();
  const resumed = stats.renewals.paused;
  process.stdout.write(
    `both recordings paused; parked workers=${stats.idleWorkers.length}; fenced-lease renewals refused so far=${resumed}\n`,
  );
  for (let i = 0; i < 2; i++) {
    const from = Date.now();
    await h.sleep(4000);
    const to = Date.now();
    const counts = probesBetween(h, from, to);
    process.stdout.write(
      `window ${i + 1}: service checks = ${counts.claim + counts.renew} ` +
        `(claim scans ${counts.claim}, renewal checks ${counts.renew}) — limit 2\n`,
    );
  }
  await w1.stop();
  await w2.stop();
}

async function main(): Promise<void> {
  const fast = process.argv.includes('--fast');
  const h = await startHarness({
    leaseMs: fast ? 500 : 1200,
    defaultAckWindow: 4,
    idleProbeIntervalMs: 5000,
    synchronous: 'NORMAL',
  });
  const originalClose = h.close.bind(h);
  h.close = async () => {
    await originalClose();
    fs.rmSync(h.dir, { recursive: true, force: true });
  };
  process.stdout.write(`queue at ${h.url}, db ${h.dbPath}\n`);
  try {
    await scenarioTakeover(h);
    await scenarioPause(h);
    if (!fast) await scenarioIdle(h);
    banner('done');
  } finally {
    await h.close();
  }
}

if (require.main === module) {
  main().catch((err) => {
    process.stderr.write(`smoke failed: ${(err as Error)?.stack ?? err}\n`);
    process.exit(1);
  });
}
