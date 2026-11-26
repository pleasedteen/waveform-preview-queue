/** End-to-end integration tests over real HTTP, real SQLite and the fixtures. */

import assert from 'node:assert/strict';
import test from 'node:test';
import path from 'node:path';
import { startHarness, probesBetween, lastTakeover } from '../src/harness';
import { loadWav } from '../src/decoder/wav';
import { decodeWhole } from '../src/decoder/mock';
import { countSegments } from '../src/segments';
import { QueueService } from '../src/queue/service';
import { createServer } from '../src/queue/server';
import { Worker } from '../src/worker/worker';
import { Reader } from '../src/reader/reader';

const long60 = 'long-60s.wav';
const long30 = 'long-30s.wav';

async function wholeDigests(file: string, segmentMs: number): Promise<string[]> {
  const wav = loadWav(path.resolve(__dirname, '../../fixtures/', file));
  const total = countSegments(wav.info.frames, wav.info.sampleRate, segmentMs);
  return (await decodeWhole(wav, segmentMs, total)).map((s) => s.digest);
}

test('handover after lease expiry resumes from persisted progress with identical results', async (t) => {
  const h = await startHarness({ leaseMs: 350, defaultAckWindow: 4 });
  t.after(() => h.close());
  h.submit({ id: 'rec', path: long30, segmentMs: 500, ackWindow: 100 });

  // A "stuck" worker claims the recording and then never renews or commits.
  const stuck = await h.client.post<{ grant: { token: string; attempt: number } }>('/claim', {
    workerId: 'stuck',
    waitMs: 0,
  });
  assert.ok(stuck.grant, 'stuck worker must get the lease');
  await h.sleep(120);

  const worker = h.worker('takover', { decodeMs: 2 });
  worker.start();
  t.after(() => worker.stop());

  const elapsed = await h.waitFor(() => h.get('rec').progress >= 3, 5000, 'takeover to advance progress');
  const takeover = lastTakeover(h, 'rec');
  assert.ok(takeover, 'a takeover must be recorded');
  assert.equal(takeover!.reason, 'lease_expiry');
  assert.notEqual(h.get('rec').lease?.workerId, 'stuck');
  assert.ok(elapsed < 3000, `takeover took ${elapsed} ms`);

  await h.waitFor(() => h.get('rec').state === 'completed', 10_000, 'recording to complete');
  const produced = h.segments('rec').map((s) => s.digest);
  const expected = await wholeDigests(long30, 500);
  assert.deepEqual(produced, expected, 'takeover must reproduce the single-pass decode');
  // The stalled claim consumed attempt 1; everything produced afterwards must
  // be stamped with the takeover lease's attempt.
  const firstAttempt = h.segments('rec')[0]!.attempt;
  assert.ok(firstAttempt >= 2, `takeover segments must use a later attempt, got ${firstAttempt}`);
});

test('paused worker yields and another recording keeps generating with a single worker', async (t) => {
  const h = await startHarness({ leaseMs: 800, defaultAckWindow: 2 });
  t.after(() => h.close());
  h.submit({ id: 'a', path: long60, segmentMs: 500, ackWindow: 2 });
  h.submit({ id: 'b', path: long30, segmentMs: 500, ackWindow: 100 });

  const worker = h.worker('solo', { decodeMs: 2 });
  worker.start();
  t.after(() => worker.stop());

  await h.waitFor(() => h.get('a').paused && h.get('a').progress === 2, 4000, 'a to fill its window');
  const aLease = h.get('a').lease;
  assert.ok(aLease?.yielded, 'a’s lease must be fenced after reaching the cap');

  // b must complete while a is paused — one worker works on what has space.
  await h.waitFor(() => h.get('b').state === 'completed', 10_000, 'b to complete while a is paused');

  // Renewals against a's paused lease are refused, not looped.
  const before = h.stats().renewals.total;
  const renew = await h.client
    .post(`/recordings/a/leases/renew`, { token: aLease!.attempt ? (aLease as unknown as { token?: string }).token ?? '' : '' })
    .catch((e) => e);
  assert.ok(renew instanceof Error, 'renew with a bad/absent token must fail');
  assert.equal(h.get('a').progress, 2, 'paused recording must not advance');
  assert.ok(h.stats().renewals.total <= before + 1);
});

test('reader ack frees space and a worker takes over within 500 ms even with a stalled producer', async (t) => {
  const h = await startHarness({ leaseMs: 3000, defaultAckWindow: 1 });
  t.after(() => h.close());
  h.submit({ id: 'r', path: long60, segmentMs: 500, ackWindow: 1 });

  // A producer claims, produces exactly one segment, then freezes while its
  // lease is still valid for seconds (it never renews and never commits again).
  const wav = loadWav(path.resolve(__dirname, '../../fixtures/', long60));
  const whole = await decodeWhole(wav, 500, 2);
  const grant = await h.client.post<{ grant: { token: string; recording: { continuation: unknown } } }>('/claim', {
    workerId: 'stalled',
    waitMs: 0,
  });
  assert.ok(grant.grant);
  const commit = (await h.client.post('/recordings/r/segments', {
    token: grant.grant!.token,
    segment: whole[0],
  })) as { shouldYield: boolean; progress: number };
  assert.equal(commit.progress, 1);
  assert.equal(commit.shouldYield, true, 'producer must be told to yield at the cap');

  const paused = h.get('r');
  assert.ok(paused.paused);
  assert.ok(paused.lease, 'the fenced lease is still visible');
  assert.ok(!paused.lease!.expired, 'the old lease must still be unexpired');
  assert.equal(paused.lease!.yielded, true);

  // A different worker is up and idle.
  const fresh = h.worker('fresh', { decodeMs: 1 });
  fresh.start();
  t.after(() => fresh.stop());
  await h.waitFor(() => h.stats().idleWorkers.includes('fresh'), 2000, 'fresh worker to park');

  const ackAt = Date.now();
  const ackRes = (await h.ack('r', 1)) as { freedSpace: boolean };
  assert.equal(ackRes.freedSpace, true);
  await h.waitFor(() => h.get('r').progress >= 2, 500, 'fresh worker to take over after ack');
  const takeover = lastTakeover(h, 'r');
  assert.ok(takeover, 'takeover must be recorded');
  assert.equal(takeover!.reason, 'ack_free');
  assert.equal(takeover!.workerId, 'fresh');
  assert.ok(Date.now() - ackAt <= 500, 'ack-to-progress must stay under 500 ms');
  assert.deepEqual(h.segments('r').slice(0, 2).map((s) => s.digest), whole.slice(0, 2).map((s) => s.digest));

  // The stalled holder's token is now stale: it can no longer advance anything.
  const staleCommit = await h.client
    .post(`/recordings/r/segments`, { token: grant.grant!.token, segment: whole[1] })
    .then(() => null)
    .catch((e) => e);
  assert.ok(staleCommit, 'stale token commit must be refused');
  assert.match(String(staleCommit.message), /lease_lost|stale_lease/);
  assert.equal(h.get('r').progress, 2, 'stale commit must not duplicate segment 1');
});

test('idle budget: paused recordings with no input cost at most 2 checks per 4 s', async (t) => {
  const h = await startHarness({ leaseMs: 700, defaultAckWindow: 1, idleProbeIntervalMs: 5000 });
  t.after(() => h.close());
  h.submit({ id: 'x', path: long30, segmentMs: 500, ackWindow: 1 });
  h.submit({ id: 'y', path: long30, segmentMs: 500, ackWindow: 1 });

  const workerA = h.worker('w1', { decodeMs: 60 });
  const workerB = h.worker('w2', { decodeMs: 60 });
  workerA.start();
  workerB.start();
  t.after(() => Promise.all([workerA.stop(), workerB.stop()]));

  await h.waitFor(
    () => h.list().every((r) => r.paused) && h.stats().idleWorkers.length === 2,
    8000,
    'both recordings paused and both workers parked',
  );

  // Let everything settle (workers parking, expiry timers draining), then
  // observe four consecutive 4 s windows with no new submissions and no pokes
  // from the test.
  for (;;) {
    const events = h.stats().probes.events;
    const last = events.length ? events[events.length - 1]!.at : 0;
    if (Date.now() - last >= 1200) break;
    await h.sleep(100);
  }
  const start = Date.now();
  const windows: Array<{ from: number; to: number; claim: number; renew: number }> = [];
  while (Date.now() - start < 16_000) {
    const from = Date.now();
    await h.sleep(4000);
    const to = Date.now();
    const counts = probesBetween(h, from, to);
    windows.push({ from, to, ...counts });
  }
  for (const [i, w] of windows.entries()) {
    const total = w.claim + w.renew;
    assert.ok(total <= 2, `window ${i} saw ${total} service checks (claim=${w.claim} renew=${w.renew})`);
  }
  // The recordings really are still paused and did not advance meanwhile.
  for (const rec of h.list()) {
    assert.ok(rec.paused, `${rec.id} must still be paused`);
    assert.equal(rec.progress, 1);
  }
  // No paused lease was renewed in a loop.
  assert.equal(h.stats().renewals.paused, 0);
});

test('queue restart preserves progress, completion and reader acks; work resumes automatically', async (t) => {
  const h = await startHarness({ leaseMs: 500, defaultAckWindow: 3, keepDir: true });
  t.after(() => h.close());
  h.submit({ id: 'keep', path: long60, segmentMs: 500, ackWindow: 4 });
  h.submit({ id: 'done', path: long30, segmentMs: 500, ackWindow: 100 });

  const reader = h.reader('keep', { processMs: 0 });
  reader.start();
  const worker = h.worker('orig', { decodeMs: 3 });
  worker.start();

  await h.waitFor(() => h.get('done').state === 'completed', 15_000, 'done to complete');
  await h.waitFor(() => h.get('keep').progress >= 6 && h.get('keep').ackCursor >= 4, 8000, 'keep to make progress');
  // Stop the producer and the reader before snapshotting: afterwards neither
  // progress nor the ack cursor can still move, so the restart comparison is
  // deterministic.
  await worker.stop();
  await reader.stop();
  const before = h.get('keep');
  const doneBefore = h.get('done');

  // Stop the first queue (server + service) but keep the DB file.
  await h.closeServerOnly();

  const service2 = new QueueService({
    dbPath: h.dbPath,
    leaseMs: 500,
    defaultAckWindow: 3,
    idleProbeIntervalMs: 5000,
    synchronous: 'NORMAL',
  });
  const server2 = createServer(service2);
  await new Promise<void>((resolve) => server2.listen(0, '127.0.0.1', resolve));
  const port2 = (server2.address() as import('node:net').AddressInfo).port;
  const url2 = `http://127.0.0.1:${port2}`;

  try {
    const resumed = service2.getRecording('keep');
    assert.equal(resumed.progress, before.progress, 'progress must survive restart');
    assert.equal(resumed.ackCursor, before.ackCursor, 'reader ack must survive restart');
    const doneAfter = service2.getRecording('done');
    assert.equal(doneAfter.state, 'completed');
    assert.equal(doneAfter.progress, doneBefore.progress);
    assert.ok(doneAfter.completedAt !== null, 'completion time must survive restart');

    // A pre-restart lease token cannot advance anything.
    const stale = await fetch(`${url2}/recordings/keep/segments`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ token: 'lease_from_previous_run', segment: {} }),
    });
    assert.equal(stale.status, 409, 'stale lease must be rejected');

    const worker2 = new Worker({ queueUrl: url2, workerId: 'after-restart', decodeMs: 1 });
    const reader2 = new Reader({ queueUrl: url2, recordingId: 'keep', processMs: 0 });
    reader2.start();
    worker2.start();
    try {
      const started = Date.now();
      for (;;) {
        const rec = service2.getRecording('keep');
        if (rec.state === 'completed' && rec.ackCursor >= rec.totalSegments) break;
        if (Date.now() - started > 20_000) throw new Error('restarted queue did not finish the recording');
        await new Promise((r) => setTimeout(r, 10));
      }
      const final = service2.getRecording('keep');
      assert.equal(final.progress, final.totalSegments);
      assert.equal(final.ackCursor, final.totalSegments);
      assert.deepEqual(
        service2.getSegments('keep', 0, 500).segments.map((s) => s.digest),
        await wholeDigests(long60, 500),
        'results across the restart must match a single uninterrupted decode',
      );
    } finally {
      await worker2.stop();
      await reader2.stop();
    }
  } finally {
    await new Promise<void>((resolve) => server2.close(() => resolve()));
    await service2.close();
  }
});

test('only the current lease holder can advance; duplicates and gaps are refused', async (t) => {
  const h = await startHarness({ leaseMs: 800, defaultAckWindow: 100 });
  t.after(() => h.close());
  h.submit({ id: 'one', path: long30, segmentMs: 500, ackWindow: 100 });
  const wav = loadWav(path.resolve(__dirname, '../../fixtures/', long30));
  const total = countSegments(wav.info.frames, wav.info.sampleRate, 500);
  const whole = await decodeWhole(wav, 500, total);

  const grant = await h.client.post<{ grant: { token: string; recording: { continuation: unknown } } }>('/claim', {
    workerId: 'first',
    waitMs: 0,
  });
  assert.ok(grant.grant);

  const commit = (token: string, segment: unknown) =>
    h.client.post('/recordings/one/segments', { token, segment });

  // Wrong token: refused.
  await assert.rejects(() => commit('nope', whole[0]), /lease_lost|stale_lease/);
  // Out of order: refused.
  await assert.rejects(() => commit(grant.grant!.token, whole[1]), /out_of_order/);
  // Right one takes effect.
  const ok = (await commit(grant.grant!.token, whole[0])) as { progress: number };
  assert.equal(ok.progress, 1);
  // Identical retry is idempotent, does not double count.
  const again = (await commit(grant.grant!.token, whole[0])) as { idempotent: boolean; progress: number };
  assert.equal(again.idempotent, true);
  assert.equal(again.progress, 1);
  assert.equal(h.segments('one').length, 1, 'segment must exist exactly once');
  // Conflicting retry is refused.
  const tampered = { ...whole[0]!, checksum: (whole[0]!.checksum + 1) >>> 0, digest: 'deadbeef' };
  await assert.rejects(() => commit(grant.grant!.token, tampered), /bad_segment|segment_conflict/);
  // A takeover that tries to continue from a stale continuation is refused.
  await h.client.post('/recordings/one/leases/renew', { token: grant.grant!.token }).catch(() => {});
  const grant2 = await h.client.post<{ grant: { token: string; recording: { progress: number } } }>('/claim', {
    workerId: 'second',
    waitMs: 0,
  });
  assert.equal(grant2.grant, null, 'a live lease must not be claimable');
});

test('preview reader pauses, the recording fills up, then resumes from its confirmed prefix', async (t) => {
  const h = await startHarness({ leaseMs: 700, defaultAckWindow: 3 });
  t.after(() => h.close());
  h.submit({ id: 'view', path: long60, segmentMs: 500, ackWindow: 3 });

  const seen: Array<{ idx: number; attempt: number }> = [];
  const reader = h.reader('view', {
    processMs: 2,
    onSegment: (info) => seen.push({ idx: info.idx, attempt: info.attempt }),
  });
  reader.start();
  const worker = h.worker('w', { decodeMs: 2 });
  worker.start();
  t.after(() => Promise.all([worker.stop(), reader.stop()]));

  await h.waitFor(() => h.get('view').progress >= 6, 6000, 'initial progress');

  // Pause the reader: it stops confirming, so the unconfirmed window fills.
  reader.pause();
  const ackAtPause = h.get('view').ackCursor;
  await h.waitFor(() => h.get('view').paused, 6000, 'recording to pause when reader stops acking');
  const pausedProgress = h.get('view').progress;
  assert.equal(pausedProgress - ackAtPause, 3, 'exactly the configured cap is outstanding');
  await h.sleep(300);
  assert.equal(h.get('view').progress, pausedProgress, 'nothing advances past the cap');

  // Segments that already took effect are still readable while paused.
  const readable = h.segments('view');
  assert.equal(readable.length, pausedProgress);
  assert.equal(readable[readable.length - 1]!.idx, pausedProgress - 1);

  // Resume: the reader continues from the persisted ack position, the worker
  // then takes over and the recording finishes.
  reader.resume();
  await h.waitFor(() => h.get('view').progress > pausedProgress, 4000, 'progress to resume after reader resumes');
  await h.waitFor(() => h.get('view').state === 'completed', 20_000, 'recording to complete');
  await h.waitFor(
    () => h.get('view').ackCursor === h.get('view').totalSegments,
    5000,
    'reader to confirm the tail',
  );

  // Every segment was presented exactly once, in order.
  assert.equal(seen.length, h.get('view').totalSegments);
  assert.deepEqual(
    seen.map((s) => s.idx),
    Array.from({ length: h.get('view').totalSegments }, (_, i) => i),
  );
  // And the results match a single-pass decode across the pause.
  const expected = await wholeDigests(long60, 500);
  assert.deepEqual(h.segments('view').map((s) => s.digest), expected);
});

test('preview reader reconnects from the durable ack position without duplicates', async (t) => {
  const h = await startHarness({ leaseMs: 700, defaultAckWindow: 50 });
  t.after(() => h.close());
  h.submit({ id: 're', path: long30, segmentMs: 500, ackWindow: 50 });

  const firstSeen: number[] = [];
  const r1 = h.reader('re', { processMs: 1, onSegment: (i) => firstSeen.push(i.idx) });
  r1.start();
  const worker = h.worker('w', { decodeMs: 3 });
  worker.start();
  t.after(() => worker.stop());

  await h.waitFor(() => h.get('re').ackCursor >= 8, 6000, 'first reader to confirm a prefix');
  await r1.stop();
  const cursorAtStop = h.get('re').ackCursor;

  const secondSeen: Array<{ idx: number; reconnected: boolean }> = [];
  const r2 = h.reader('re', {
    processMs: 0,
    onSegment: (i) => secondSeen.push({ idx: i.idx, reconnected: i.reconnected }),
  });
  r2.start();
  t.after(() => r2.stop());
  await h.waitFor(() => h.get('re').state === 'completed' && h.get('re').ackCursor === h.get('re').totalSegments, 15_000, 'completion');

  // The reconnected reader never re-presents a confirmed segment.
  assert.ok(secondSeen.length > 0);
  assert.equal(secondSeen[0]!.idx, cursorAtStop);
  assert.ok(secondSeen.every((s) => s.idx >= cursorAtStop));
  const all = [...firstSeen, ...secondSeen.map((s) => s.idx)];
  assert.equal(new Set(all).size, all.length, 'no segment presented twice across the reconnect');
  assert.equal(all.length, h.get('re').totalSegments);
});

test('a worker that keeps running across a queue restart re-claims from persisted progress', async (t) => {
  const h = await startHarness({ leaseMs: 4000, defaultAckWindow: 100, keepDir: true });
  t.after(() => h.close());
  h.submit({ id: 'live', path: long60, segmentMs: 500, ackWindow: 100 });

  const reader = h.reader('live', { processMs: 0 });
  reader.start();
  const worker = h.worker('survivor', { decodeMs: 25 });
  worker.start();
  await h.waitFor(() => h.get('live').progress >= 3, 6000, 'progress before restart');
  const port = h.port;
  const progressBefore = h.get('live').progress;

  // Kill only the queue process; the worker keeps its loop and its old token.
  await h.closeServerOnly();
  const service2 = new QueueService({
    dbPath: h.dbPath,
    leaseMs: 4000,
    defaultAckWindow: 100,
    idleProbeIntervalMs: 5000,
    synchronous: 'NORMAL',
  });
  const server2 = createServer(service2);
  await new Promise<void>((resolve) => server2.listen(port, '127.0.0.1', resolve));
  try {
    // The old token belongs to the previous run and must be rejected.
    const stale = await fetch(`http://127.0.0.1:${port}/recordings/live/leases/renew`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ token: 'from-before-restart' }),
    });
    assert.equal(stale.status, 409);

    await h.waitFor(
      () => service2.getRecording('live').state === 'completed',
      25_000,
      'surviving worker to finish after restart',
    );
    const final = service2.getRecording('live');
    assert.equal(final.progress, final.totalSegments);
    assert.ok(final.progress >= progressBefore);
    assert.deepEqual(
      service2.getSegments('live', 0, 500).segments.map((s) => s.digest),
      await wholeDigests(long60, 500),
      'results after a live restart must match a clean decode',
    );
    assert.ok(worker.getStatus().leaseLosses >= 1, 'the worker must have observed its lease being lost');
  } finally {
    await worker.stop();
    await reader.stop();
    await new Promise<void>((resolve) => {
      server2.close(() => resolve());
      server2.closeAllConnections?.();
    });
    await service2.close();
  }
});
