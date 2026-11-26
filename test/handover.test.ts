/**
 * Ack-free handover tracing: /stats keeps the most recent 100 reader
 * confirmations that actually freed a slot for an unfinished recording, with
 * the four stage times (free, lease issued, worker takeover report, first
 * resumed segment landed) — including the ones that never happened. Covers
 * disconnects (lease issued, no report), stalls after the report, queue
 * restarts and re-dispatch correlation.
 */

import assert from 'node:assert/strict';
import test from 'node:test';
import path from 'node:path';
import type { AddressInfo } from 'node:net';
import { startHarness, rawClaim, rawCommit, handoversFor, latestHandover, type Harness } from '../src/harness';
import { loadWav } from '../src/decoder/wav';
import { decodeWhole } from '../src/decoder/mock';
import { QueueService } from '../src/queue/service';
import { createServer } from '../src/queue/server';
import { Worker } from '../src/worker/worker';
import type { AckHandoverView } from '../src/types';

const long30 = 'long-30s.wav';
const long60 = 'long-60s.wav';

function wholeSegments(file: string, count: number) {
  const wav = loadWav(path.resolve(__dirname, '../../fixtures/', file));
  return decodeWhole(wav, 500, count);
}

/** Fill the ack window with exactly one segment so the recording pauses. */
async function produceOneAndPause(h: Harness, id: string, workerId: string): Promise<void> {
  const [first] = await wholeSegments(long30, 1);
  const grant = await rawClaim(h, workerId);
  assert.equal(grant.recording.id, id);
  const commit = await rawCommit(h, grant, first);
  assert.equal(commit.shouldYield, true, 'the window must be full after one segment');
  assert.ok(h.get(id).paused, `${id} must be paused at its cap`);
}

function onlyHandover(h: Harness, id: string): AckHandoverView {
  const all = handoversFor(h, id);
  assert.equal(all.length, 1, `expected exactly one handover record for ${id}`);
  return all[0]!;
}

test('disconnect: lease issued but no takeover report; re-dispatch stays correlated to the same ack', async (t) => {
  const h = await startHarness({ leaseMs: 400, defaultAckWindow: 1 });
  t.after(() => h.close());
  h.submit({ id: 'dc', path: long30, segmentMs: 500, ackWindow: 1 });
  await produceOneAndPause(h, 'dc', 'producer');

  const ack = (await h.ack('dc', 1)) as { freedSpace: boolean };
  assert.equal(ack.freedSpace, true);

  // The free is visible immediately, with every later stage still pending.
  let ho = onlyHandover(h, 'dc');
  assert.equal(ho.state, 'awaiting_lease');
  assert.equal(ho.ackCursor, 1);
  assert.equal(ho.progressAtFree, 1);
  assert.equal(ho.leaseIssuedAt, null);
  assert.equal(ho.reportedAt, null);
  assert.equal(ho.firstSegmentAt, null);
  assert.equal(ho.dispatches.length, 0);

  // A worker claims the freed recording and then disconnects: it never
  // reports the takeover and never commits. Simulated with a raw claim.
  const gone = await rawClaim(h, 'gone');
  assert.equal(gone.recording.id, 'dc');
  ho = onlyHandover(h, 'dc');
  assert.equal(ho.state, 'awaiting_report', 'lease issued, no report received yet');
  assert.ok(ho.leaseIssuedAt !== null);
  assert.equal(ho.reportedAt, null, 'no takeover report from the disconnected worker');
  assert.equal(ho.firstSegmentAt, null);
  assert.equal(ho.dispatches.length, 1);
  assert.equal(ho.dispatches[0]!.workerId, 'gone');
  assert.equal(ho.dispatches[0]!.phase, 'awaiting_report');
  assert.equal(ho.dispatches[0]!.reportedAt, null);
  assert.equal(ho.dispatches[0]!.firstSegmentAt, null);

  // The disconnected worker's lease expires; a healthy worker is parked and
  // takes over. The re-dispatch must land on the SAME handover record.
  const healthy = h.worker('healthy', { decodeMs: 1 });
  healthy.start();
  t.after(() => healthy.stop());
  await h.waitFor(() => onlyHandover(h, 'dc').state === 'landed', 5000, 'handover to land after re-dispatch');

  ho = onlyHandover(h, 'dc');
  assert.equal(ho.dispatches.length, 2, 'the re-dispatch is a second dispatch of the same confirmation');
  const [first, second] = ho.dispatches;
  assert.equal(first!.phase, 'superseded');
  assert.equal(first!.reportedAt, null, 'first dispatch: never reported (disconnect)');
  assert.equal(first!.firstSegmentAt, null, 'first dispatch: nothing landed');
  assert.equal(second!.workerId, 'healthy');
  assert.equal(second!.phase, 'landed');
  assert.ok(second!.reportedAt !== null, 'second dispatch: takeover reported');
  assert.ok(second!.firstSegmentAt !== null, 'second dispatch: first resumed segment landed');
  assert.equal(second!.firstSegmentIdx, 1, 'the resumed run starts where the ack freed space');
  // The handover's effective stage times come from the dispatch that got furthest.
  assert.equal(ho.leaseIssuedAt, first!.leaseIssuedAt);
  assert.equal(ho.reportedAt, second!.reportedAt);
  assert.equal(ho.firstSegmentAt, second!.firstSegmentAt);
  assert.equal(ho.firstSegmentIdx, 1);
  assert.equal(ho.leaseLatencyMs, ho.leaseIssuedAt! - ho.freedAt);
  assert.equal(ho.reportLatencyMs, ho.reportedAt! - ho.freedAt);
  assert.equal(ho.firstSegmentLatencyMs, ho.firstSegmentAt! - ho.freedAt);
});

test('stuck after reporting is distinguishable from never reporting', async (t) => {
  const h = await startHarness({ leaseMs: 600, defaultAckWindow: 1 });
  t.after(() => h.close());
  h.submit({ id: 'st', path: long30, segmentMs: 500, ackWindow: 1 });
  await produceOneAndPause(h, 'st', 'producer');
  await h.ack('st', 1);

  // A worker that reports the takeover and then gets stuck before its first
  // segment lands (decode takes "forever"; its heartbeat keeps the lease).
  const stuck = h.worker('stuck', { decodeMs: 60_000 });
  stuck.start();
  await h.waitFor(() => onlyHandover(h, 'st').reportedAt !== null, 3000, 'stuck worker to report the takeover');

  let ho = onlyHandover(h, 'st');
  assert.equal(ho.state, 'awaiting_first_segment', 'reported, but no resumed segment landed');
  assert.ok(ho.leaseIssuedAt !== null);
  assert.ok(ho.reportedAt !== null, 'the report DID arrive — this is not a disconnect');
  assert.equal(ho.firstSegmentAt, null, 'stuck after the report: nothing landed');
  assert.equal(ho.dispatches.length, 1);
  assert.equal(ho.dispatches[0]!.phase, 'awaiting_first_segment');
  assert.ok(ho.dispatches[0]!.reportedAt !== null);
  assert.equal(ho.dispatches[0]!.firstSegmentAt, null);

  // The stuck worker dies; its lease expires and a fast worker re-dispatches.
  await stuck.stop();
  const fast = h.worker('fast', { decodeMs: 1 });
  fast.start();
  t.after(() => fast.stop());
  await h.waitFor(() => onlyHandover(h, 'st').state === 'landed', 5000, 'handover to land after the stall');

  ho = onlyHandover(h, 'st');
  assert.equal(ho.dispatches.length, 2);
  const [stalled, landed] = ho.dispatches;
  assert.equal(stalled!.phase, 'superseded');
  assert.ok(stalled!.reportedAt !== null, 'stuck-after-report stays visible on the superseded dispatch');
  assert.equal(stalled!.firstSegmentAt, null);
  assert.equal(landed!.workerId, 'fast');
  assert.equal(landed!.phase, 'landed');
  assert.equal(landed!.firstSegmentIdx, 1);
  assert.equal(ho.firstSegmentAt, landed!.firstSegmentAt);
});

test('queue restart keeps the same confirmation traceable; a post-restart claim continues the same handover', async (t) => {
  const h = await startHarness({ leaseMs: 500, defaultAckWindow: 1, keepDir: true });
  t.after(() => h.close());
  h.submit({ id: 'rs', path: long30, segmentMs: 500, ackWindow: 1 });
  await produceOneAndPause(h, 'rs', 'producer');
  await h.ack('rs', 1);

  // A worker claims but the queue restarts before it ever reports.
  const doomed = await rawClaim(h, 'doomed');
  assert.equal(doomed.recording.id, 'rs');
  const before = onlyHandover(h, 'rs');
  assert.equal(before.state, 'awaiting_report');
  assert.equal(before.dispatches.length, 1);

  await h.closeServerOnly();

  const service2 = new QueueService({
    dbPath: h.dbPath,
    leaseMs: 500,
    defaultAckWindow: 1,
    idleProbeIntervalMs: 5000,
    synchronous: 'NORMAL',
  });
  const server2 = createServer(service2);
  await new Promise<void>((resolve) => server2.listen(0, '127.0.0.1', resolve));
  const port2 = (server2.address() as AddressInfo).port;
  const url2 = `http://127.0.0.1:${port2}`;

  try {
    // The same confirmation is still traceable under the same handover id,
    // including the dispatch whose worker never reported.
    const stats2 = service2.stats();
    const reopened = stats2.ackHandovers.find((x) => x.recordingId === 'rs');
    assert.ok(reopened, 'handover record must survive the queue restart');
    assert.equal(reopened!.handoverId, before.handoverId);
    assert.equal(reopened!.freedAt, before.freedAt);
    assert.equal(reopened!.ackCursor, 1);
    assert.equal(reopened!.state, 'awaiting_report');
    assert.equal(reopened!.dispatches.length, 1);
    assert.equal(reopened!.dispatches[0]!.workerId, 'doomed');
    assert.equal(reopened!.dispatches[0]!.reportedAt, null);

    // A worker that connects after the restart re-dispatches the SAME
    // handover: the old lease is gone, so the recording is claimable again.
    const worker = new Worker({ queueUrl: url2, workerId: 'post-restart', decodeMs: 1 });
    worker.start();
    try {
      const started = Date.now();
      for (;;) {
        const cur = service2.stats().ackHandovers.find((x) => x.handoverId === before.handoverId);
        if (cur?.state === 'landed') break;
        if (Date.now() - started > 5000) throw new Error('handover did not land after the restart');
        await new Promise((r) => setTimeout(r, 10));
      }
      const after = service2.stats().ackHandovers.find((x) => x.handoverId === before.handoverId)!;
      assert.equal(after.dispatches.length, 2, 'the post-restart claim is a new dispatch of the same handover');
      assert.equal(after.dispatches[0]!.workerId, 'doomed');
      assert.equal(after.dispatches[0]!.phase, 'superseded');
      assert.equal(after.dispatches[1]!.workerId, 'post-restart');
      assert.equal(after.dispatches[1]!.phase, 'landed');
      assert.ok(after.dispatches[1]!.reportedAt !== null);
      assert.equal(after.dispatches[1]!.firstSegmentIdx, 1);
      assert.equal(after.firstSegmentAt, after.dispatches[1]!.firstSegmentAt);
      assert.equal(after.reportedAt, after.dispatches[1]!.reportedAt);
      // The takeover latency stat also attributes the post-restart claim to
      // the original pre-restart confirmation.
      const takeover = service2.stats().takeovers.find((x) => x.recordingId === 'rs');
      assert.ok(takeover, 'takeover stat must survive the restart for the same ack');
      assert.equal(takeover!.reason, 'ack_free');
      assert.equal(takeover!.freedAt, before.freedAt);
    } finally {
      await worker.stop();
    }
  } finally {
    await new Promise<void>((resolve) => {
      server2.close(() => resolve());
      server2.closeAllConnections?.();
    });
    await service2.close();
  }
});

test('with an idle worker parked, every handover stage lands within 500 ms of the ack', async (t) => {
  const h = await startHarness({ leaseMs: 3000, defaultAckWindow: 1 });
  t.after(() => h.close());
  h.submit({ id: 'quick', path: long60, segmentMs: 500, ackWindow: 1 });

  // A stalled producer fills the window and keeps its (long, valid) lease.
  const [first] = await wholeSegments(long60, 1);
  const grant = await rawClaim(h, 'stalled');
  const commit = await rawCommit(h, grant, first);
  assert.equal(commit.shouldYield, true);

  const idle = h.worker('idle', { decodeMs: 1 });
  idle.start();
  t.after(() => idle.stop());
  await h.waitFor(() => h.stats().idleWorkers.includes('idle'), 2000, 'idle worker to park');

  const ackAt = Date.now();
  const ack = (await h.ack('quick', 1)) as { freedSpace: boolean };
  assert.equal(ack.freedSpace, true);
  await h.waitFor(() => latestHandover(h, 'quick')?.state === 'landed', 500, 'handover to land within 500 ms');
  const elapsed = Date.now() - ackAt;
  assert.ok(elapsed <= 500, `ack-to-landing took ${elapsed} ms`);

  const ho = latestHandover(h, 'quick')!;
  assert.equal(ho.dispatches.length, 1);
  assert.equal(ho.dispatches[0]!.workerId, 'idle');
  assert.ok(ho.leaseLatencyMs !== null && ho.leaseLatencyMs <= 500, `lease latency ${ho.leaseLatencyMs} ms`);
  assert.ok(ho.reportLatencyMs !== null && ho.reportLatencyMs <= 500, `report latency ${ho.reportLatencyMs} ms`);
  assert.ok(
    ho.firstSegmentLatencyMs !== null && ho.firstSegmentLatencyMs <= 500,
    `first-segment latency ${ho.firstSegmentLatencyMs} ms`,
  );
  // The four stages happened in order.
  assert.ok(ho.freedAt <= ho.leaseIssuedAt!);
  assert.ok(ho.leaseIssuedAt! <= ho.reportedAt!);
  assert.ok(ho.reportedAt! <= ho.firstSegmentAt!);
  assert.equal(ho.firstSegmentIdx, 1);
});

test('only the most recent 100 ack-free handovers are kept; pending stages stay visible', async (t) => {
  const h = await startHarness({ leaseMs: 800, defaultAckWindow: 1 });
  t.after(() => h.close());

  const [first] = await wholeSegments(long30, 1);
  const total = 105;
  for (let i = 0; i < total; i++) {
    const id = `cap-${i}`;
    h.submit({ id, path: long30, segmentMs: 500, ackWindow: 1 });
    const grant = await rawClaim(h, 'capper');
    const commit = await rawCommit(h, grant, first);
    assert.equal(commit.shouldYield, true);
  }
  // No worker is running: every ack frees a slot that nobody claims.
  for (let i = 0; i < total; i++) {
    const ack = (await h.ack(`cap-${i}`, 1)) as { freedSpace: boolean };
    assert.equal(ack.freedSpace, true);
  }

  const all = h.stats().ackHandovers;
  assert.equal(all.length, 100, 'only the most recent 100 confirmations are kept');
  assert.equal(all[0]!.recordingId, 'cap-5', 'the five oldest records were pruned');
  assert.equal(all[all.length - 1]!.recordingId, 'cap-104');
  assert.equal(handoversFor(h, 'cap-0').length, 0, 'pruned records are gone');
  for (const ho of all) {
    assert.equal(ho.state, 'awaiting_lease');
    assert.equal(ho.ackCursor, 1);
    assert.equal(ho.progressAtFree, 1);
    assert.equal(ho.leaseIssuedAt, null, 'lease stage has not happened and is visible as such');
    assert.equal(ho.reportedAt, null);
    assert.equal(ho.firstSegmentAt, null);
    assert.equal(ho.leaseLatencyMs, null);
    assert.equal(ho.dispatches.length, 0);
  }
});
