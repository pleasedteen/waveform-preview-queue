/** Mock segmented waveform decoder with state that crosses segment boundaries. */

import { fnv1a32, sleep, AppError } from '../util/common';
import { PEAK_BUCKETS, type ContinuationState, type SegmentPayload, checksumHex } from '../protocol';
import { segmentBounds } from '../segments';
import type { LoadedWav } from './wav';

export const HIGH_PASS_ALPHA = 0.85;

export interface DecodeOptions {
  /** Simulated decode duration, ms (spread across slices so aborts land mid-decode). */
  delayMs?: number;
  signal?: AbortSignal;
  /** Called once per simulated slice, useful for logs. */
  onSlice?: (fraction: number) => void;
}

/**
 * Decode one segment. Deterministic: given the same wav, segment and input
 * state the output payload is byte-for-byte identical, so a takeover that
 * restarts from the persisted continuation reproduces the same segments a
 * single uninterrupted worker would have produced.
 *
 * The simulated delay is split into 4 slices; if `signal` aborts the decode
 * rejects and the partial result is discarded (nothing is persisted by the
 * decoder itself).
 */
export async function decodeSegment(
  wav: LoadedWav,
  segmentMs: number,
  idx: number,
  state: ContinuationState,
  opts: DecodeOptions = {},
): Promise<SegmentPayload> {
  const { sampleRate, frames } = wav.info;
  const bounds = segmentBounds(frames, sampleRate, segmentMs, idx);
  if (state.cursor !== bounds.startSample) {
    throw new AppError(
      409,
      'continuation_mismatch',
      `continuation cursor ${state.cursor} does not match segment ${idx} start ${bounds.startSample}`,
    );
  }
  const delayMs = Math.max(0, opts.delayMs ?? 0);
  const slices = 4;
  for (let slice = 0; slice < slices; slice++) {
    if (opts.signal?.aborted) throw new AppError(499, 'aborted', 'decode aborted');
    if (delayMs > 0) await sleep(delayMs / slices, opts.signal);
    opts.onSlice?.((slice + 1) / slices);
  }
  if (opts.signal?.aborted) throw new AppError(499, 'aborted', 'decode aborted');

  const count = bounds.endSample - bounds.startSample;
  const peaks = new Array<number>(PEAK_BUCKETS).fill(0);
  let checksum = state.checksum >>> 0;
  let carry = state.carry;
  const two = new Uint8Array(2);
  const view = new DataView(two.buffer);
  for (let i = 0; i < count; i++) {
    const raw = wav.samples[bounds.startSample + i]!;
    const filtered = raw - HIGH_PASS_ALPHA * carry;
    carry = raw;
    const abs = Math.min(1, Math.abs(filtered));
    const bucket = Math.min(PEAK_BUCKETS - 1, Math.floor((i / Math.max(1, count)) * PEAK_BUCKETS));
    if (abs > peaks[bucket]!) peaks[bucket] = abs;
    const q = Math.max(-32768, Math.min(32767, Math.round(raw * 32767)));
    view.setInt16(0, q, true);
    checksum = fnv1a32(two, checksum);
  }
  const endChecksum = checksum >>> 0;
  return {
    version: 1,
    idx,
    startMs: bounds.startMs,
    endMs: bounds.endMs,
    startSample: bounds.startSample,
    sampleCount: count,
    sampleRate,
    peaks: peaks.map((v) => Math.round(v * 1e6) / 1e6),
    startChecksum: state.checksum >>> 0,
    checksum: endChecksum,
    inputCarry: state.carry,
    outputCarry: carry,
    digest: checksumHex(endChecksum),
  };
}

export function continuationAfter(payload: SegmentPayload): ContinuationState {
  return {
    version: 1,
    cursor: payload.startSample + payload.sampleCount,
    checksum: payload.checksum,
    carry: payload.outputCarry,
  };
}

/** Decode every segment in one pass; used by tests to cross-check handovers. */
export async function decodeWhole(
  wav: LoadedWav,
  segmentMs: number,
  totalSegments: number,
  initial?: ContinuationState,
): Promise<SegmentPayload[]> {
  let state = initial ?? { version: 1, cursor: 0, checksum: 0x811c9dc5, carry: 0 };
  const out: SegmentPayload[] = [];
  for (let idx = 0; idx < totalSegments; idx++) {
    const payload = await decodeSegment(wav, segmentMs, idx, state);
    out.push(payload);
    state = continuationAfter(payload);
  }
  return out;
}
