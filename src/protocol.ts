/**
 * Wire protocol shared by the queue, the workers and the preview reader.
 *
 * A segment payload is self-describing: besides the preview peaks it carries
 * the decoder state it started from (`startChecksum`, `inputCarry`,
 * `startSample`) and the state it leaves behind (`checksum`, `outputCarry`).
 * The queue refuses a segment whose input state does not match the
 * continuation state it persisted with the previous segment, which is what
 * makes takeover-after-lease-expiry safe.
 */

import { AppError } from './util/common';

export const PEAK_BUCKETS = 32;

export interface ContinuationState {
  version: 1;
  /** Absolute sample index at which the next segment starts. */
  cursor: number;
  /** FNV-1a checksum of every sample decoded so far. */
  checksum: number;
  /** Last raw sample of the previous segment (high-pass filter carry). */
  carry: number;
}

export interface SegmentPayload {
  version: 1;
  idx: number;
  startMs: number;
  endMs: number;
  startSample: number;
  sampleCount: number;
  sampleRate: number;
  /** True peak amplitudes per bucket (0..1). */
  peaks: number[];
  startChecksum: number;
  checksum: number;
  inputCarry: number;
  outputCarry: number;
  /** Hex form of `checksum`; convenient for assertions and logs. */
  digest: string;
}

export function checksumHex(checksum: number): string {
  return (checksum >>> 0).toString(16).padStart(8, '0');
}

function isUint32(v: unknown): v is number {
  return typeof v === 'number' && Number.isInteger(v) && v >= 0 && v <= 0xffffffff;
}

function isUnit(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v) && v >= -1.0000001 && v <= 1.0000001;
}

/** Structural validation performed by the queue before a segment is stored. */
export function validateSegmentPayload(input: unknown): SegmentPayload {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw new AppError(400, 'bad_segment', 'segment payload must be an object');
  }
  const p = input as Record<string, unknown>;
  if (p.version !== 1) throw new AppError(400, 'bad_segment', 'segment.version must be 1');
  if (typeof p.idx !== 'number' || !Number.isInteger(p.idx) || p.idx < 0) {
    throw new AppError(400, 'bad_segment', 'segment.idx must be a non-negative integer');
  }
  if (!isUint32(p.startChecksum) || !isUint32(p.checksum)) {
    throw new AppError(400, 'bad_segment', 'segment checksums must be uint32 values');
  }
  if (!isUnit(p.inputCarry) || !isUnit(p.outputCarry)) {
    throw new AppError(400, 'bad_segment', 'segment carries must be finite samples within [-1, 1]');
  }
  if (typeof p.startSample !== 'number' || !Number.isInteger(p.startSample) || p.startSample < 0) {
    throw new AppError(400, 'bad_segment', 'segment.startSample must be a non-negative integer');
  }
  if (typeof p.sampleCount !== 'number' || !Number.isInteger(p.sampleCount) || p.sampleCount < 0) {
    throw new AppError(400, 'bad_segment', 'segment.sampleCount must be a non-negative integer');
  }
  if (typeof p.sampleRate !== 'number' || !Number.isInteger(p.sampleRate) || p.sampleRate <= 0) {
    throw new AppError(400, 'bad_segment', 'segment.sampleRate must be a positive integer');
  }
  if (typeof p.startMs !== 'number' || !Number.isFinite(p.startMs) || p.startMs < 0) {
    throw new AppError(400, 'bad_segment', 'segment.startMs must be a non-negative number');
  }
  if (typeof p.endMs !== 'number' || !Number.isFinite(p.endMs) || p.endMs < p.startMs) {
    throw new AppError(400, 'bad_segment', 'segment.endMs must be >= startMs');
  }
  if (!Array.isArray(p.peaks) || p.peaks.length !== PEAK_BUCKETS) {
    throw new AppError(400, 'bad_segment', `segment.peaks must contain ${PEAK_BUCKETS} values`);
  }
  for (const peak of p.peaks) {
    if (typeof peak !== 'number' || !Number.isFinite(peak) || peak < 0 || peak > 1) {
      throw new AppError(400, 'bad_segment', 'segment.peaks values must be within [0, 1]');
    }
  }
  if (typeof p.digest !== 'string' || !/^[0-9a-f]{8}$/.test(p.digest) || p.digest !== checksumHex(p.checksum)) {
    throw new AppError(400, 'bad_segment', 'segment.digest must be the hex form of segment.checksum');
  }
  return p as unknown as SegmentPayload;
}

export function initialContinuation(): ContinuationState {
  return { version: 1, cursor: 0, checksum: 0x811c9dc5, carry: 0 };
}
