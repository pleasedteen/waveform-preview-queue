/** Semantics of completion, readable-but-unconfirmed results and ack rules. */

import assert from 'node:assert/strict';
import test from 'node:test';
import path from 'node:path';
import { startHarness } from '../src/harness';
import { loadWav } from '../src/decoder/wav';
import { decodeWhole } from '../src/decoder/mock';
import { countSegments } from '../src/segments';

const long30 = path.resolve(__dirname, '../../fixtures/long-30s.wav');

test('completion shows as soon as the last segment takes effect; unconfirmed results stay readable', async (t) => {
  const h = await startHarness({ leaseMs: 800, defaultAckWindow: 100 });
  t.after(() => h.close());
  h.submit({ id: 'c', path: long30, segmentMs: 500, ackWindow: 100 });

  // No reader at all: nothing is ever confirmed.
  const worker = h.worker('w', { decodeMs: 1 });
  worker.start();
  t.after(() => worker.stop());
  await h.waitFor(() => h.get('c').state === 'completed', 15_000, 'completion without any ack');

  const rec = h.get('c');
  assert.equal(rec.progress, rec.totalSegments);
  assert.equal(rec.ackCursor, 0, 'nothing was confirmed');
  assert.equal(rec.unconfirmed, rec.totalSegments);
  assert.ok(rec.completedAt !== null);
  assert.equal(rec.lease, null, 'the lease is released on completion');

  // Every result is readable even though none is confirmed.
  const segments = h.segments('c');
  assert.equal(segments.length, rec.totalSegments);
  const wav = loadWav(long30);
  const expected = await decodeWhole(wav, 500, countSegments(wav.info.frames, wav.info.sampleRate, 500));
  assert.deepEqual(segments.map((s) => s.digest), expected.map((s) => s.digest));

  // A late ack is still accepted and does not move a completed recording.
  const ack = (await h.ack('c', rec.totalSegments)) as { ackCursor: number; freedSpace: boolean };
  assert.equal(ack.ackCursor, rec.totalSegments);
  assert.equal(ack.freedSpace, false);
  assert.equal(h.get('c').state, 'completed');
});

test('acks are monotonic, cannot run ahead, and per-recording caps are honoured', async (t) => {
  const h = await startHarness({ leaseMs: 800, defaultAckWindow: 3 });
  t.after(() => h.close());
  const created = h.submit({ id: 'cfg', path: long30, segmentMs: 500, ackWindow: 7 });
  assert.equal(created.ackWindow, 7, 'explicit per-recording cap is stored');
  const defaulted = h.submit({ id: 'dflt', path: long30, segmentMs: 500 });
  assert.equal(defaulted.ackWindow, 3, 'default cap applies when omitted');

  const worker = h.worker('w', { decodeMs: 1 });
  worker.start();
  t.after(() => worker.stop());
  await h.waitFor(() => h.get('cfg').progress >= 7 && h.get('cfg').paused, 5000, 'cfg to fill its window');
  assert.equal(h.get('cfg').progress, 7);

  // Ack ahead of produced progress is refused.
  await assert.rejects(() => h.ack('cfg', 20), /ack_ahead/);
  // Ack to 3 then back to 1 stays at 3.
  const a1 = (await h.ack('cfg', 3)) as { ackCursor: number };
  assert.equal(a1.ackCursor, 3);
  const a2 = (await h.ack('cfg', 1)) as { ackCursor: number };
  assert.equal(a2.ackCursor, 3, 'ack cursor never moves backwards');
  // Negative cursor is a bad request.
  await assert.rejects(() => h.ack('cfg', -1), /non-negative/);
});
