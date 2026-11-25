/** Segment geometry shared by the queue and the mock decoder. */

export interface SampleBounds {
  startSample: number;
  endSample: number;
  startMs: number;
  endMs: number;
}

/**
 * Wav segments are defined in sample space: segment i spans samples
 * [round(i * segmentMs * rate / 1000), round((i+1) * segmentMs * rate / 1000)).
 */
export function segmentBounds(
  frames: number,
  sampleRate: number,
  segmentMs: number,
  idx: number,
): SampleBounds {
  const startSample = Math.min(frames, Math.round((idx * segmentMs * sampleRate) / 1000));
  const endSample = Math.min(frames, Math.round(((idx + 1) * segmentMs * sampleRate) / 1000));
  return {
    startSample,
    endSample,
    startMs: (startSample / sampleRate) * 1000,
    endMs: (endSample / sampleRate) * 1000,
  };
}

/** Number of segments needed to cover `frames` samples at the given granularity. */
export function countSegments(frames: number, sampleRate: number, segmentMs: number): number {
  if (frames <= 0 || sampleRate <= 0 || segmentMs <= 0) return 0;
  return Math.ceil((frames * 1000) / (sampleRate * segmentMs));
}
