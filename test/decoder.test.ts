/** Unit tests for the cross-segment decoder, geometry and payload validation. */

import assert from 'node:assert/strict';
import test from 'node:test';
import path from 'node:path';
import { loadWav } from '../src/decoder/wav';
import { continuationAfter, decodeSegment, decodeWhole } from '../src/decoder/mock';
import { countSegments, segmentBounds } from '../src/segments';
import { initialContinuation, validateSegmentPayload } from '../src/protocol';

const fixture = path.resolve(__dirname, '../../fixtures/long-30s.wav');

test('segment geometry covers the whole recording exactly once', () => {
  const frames = 30_000 * 16; // 30 s at 16 kHz
  const total = countSegments(frames, 16_000, 500);
  assert.equal(total, 60);
  let cursor = 0;
  for (let idx = 0; idx < total; idx++) {
    const b = segmentBounds(frames, 16_000, 500, idx);
    assert.equal(b.startSample, cursor);
    assert.ok(b.endSample >= b.startSample);
    cursor = b.endSample;
  }
  assert.equal(cursor, frames);
});

test('decoder resumes from persisted continuation and matches a single pass', async () => {
  const wav = loadWav(fixture);
  const total = countSegments(wav.info.frames, wav.info.sampleRate, 500);
  const whole = await decodeWhole(wav, 500, total);

  // Decode 0..2, persist continuation, then "restart" from it.
  let state = initialContinuation();
  for (let idx = 0; idx < 2; idx++) {
    const seg = await decodeSegment(wav, 500, idx, state);
    assert.deepEqual(seg, whole[idx]);
    state = continuationAfter(seg);
  }
  for (let idx = 2; idx < 10; idx++) {
    const seg = await decodeSegment(wav, 500, idx, state);
    assert.deepEqual(seg, whole[idx]);
    state = continuationAfter(seg);
  }
});

test('decoder rejects a continuation that does not match the segment start', async () => {
  const wav = loadWav(fixture);
  await assert.rejects(
    () => decodeSegment(wav, 500, 3, initialContinuation()),
    /continuation cursor 0 does not match segment 3 start/,
  );
});

test('cross-segment state actually matters (carry and checksum chain)', async () => {
  const wav = loadWav(fixture);
  const total = countSegments(wav.info.frames, wav.info.sampleRate, 500);
  const whole = await decodeWhole(wav, 500, total);
  const state = continuationAfter(whole[0]!);

  // The high-pass carry from the previous segment changes the peaks...
  const withCarry = await decodeSegment(wav, 500, 1, state);
  const withoutCarry = await decodeSegment(wav, 500, 1, { ...state, carry: 0 });
  assert.notDeepEqual(withCarry.peaks, withoutCarry.peaks);
  assert.equal(withCarry.digest, withoutCarry.digest, 'raw-sample checksum does not depend on peaks');

  // ...and the chained checksum makes the digest depend on earlier segments.
  const wrongChain = await decodeSegment(wav, 500, 1, { ...state, checksum: 0x811c9dc5 });
  assert.notEqual(wrongChain.digest, whole[1]!.digest);
  assert.equal(withCarry.digest, whole[1]!.digest);
});

test('payload validation rejects malformed segments', async () => {
  const wav = loadWav(fixture);
  const seg = await decodeSegment(wav, 500, 0, initialContinuation());
  assert.equal(validateSegmentPayload(seg).digest, seg.digest);
  assert.throws(() => validateSegmentPayload({ ...seg, peaks: [1, 2] }), /32 values/);
  assert.throws(() => validateSegmentPayload({ ...seg, digest: 'deadbeef' }), /hex form/);
  assert.throws(() => validateSegmentPayload({ ...seg, sampleCount: -1 }), /sampleCount/);
  assert.throws(() => validateSegmentPayload(null), /must be an object/);
});
