#!/usr/bin/env node
/**
 * Generate the local recording fixtures.
 *
 *   fixtures/long-60s.wav   60 s,  8 kHz mono, 500 ms segments  -> 120 segments
 *   fixtures/long-30s.wav   30 s, 16 kHz mono, 500 ms segments  ->  60 segments
 *
 * Signals are deterministic (fixed-seed LCG) so checksums are reproducible on
 * any machine. Run `npm run fixtures` after `npm run build`.
 */

import fs from 'node:fs';
import path from 'node:path';
import { encodeWavPcm16 } from '../decoder/wav';

function lcg(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s / 0x100000000;
  };
}

export function makeSignal(durationMs: number, sampleRate: number): Float64Array {
  const frames = Math.round((durationMs / 1000) * sampleRate);
  const samples = new Float64Array(frames);
  const rand = lcg(0x5eed1234);
  const noise = new Float64Array(frames);
  for (let i = 0; i < frames; i++) noise[i] = rand() * 2 - 1;
  let hp = 0;
  for (let i = 0; i < frames; i++) {
    const t = i / sampleRate;
    // Pitched tone sweep + harmonics + noise, so every segment differs and
    // the high-pass carry across boundaries visibly changes the output.
    const f = 180 + 60 * Math.sin(2 * Math.PI * 0.25 * t);
    const tone =
      0.42 * Math.sin(2 * Math.PI * f * t) +
      0.18 * Math.sin(2 * Math.PI * (2 * f) * t + 0.4) +
      0.08 * Math.sin(2 * Math.PI * (3.5 * f) * t + 1.1);
    const env = 0.55 + 0.35 * Math.sin(2 * Math.PI * 0.5 * t + 0.3);
    hp = 0.06 * (noise[i]! * 0.6) + 0.94 * hp;
    samples[i] = Math.max(-1, Math.min(1, env * tone + hp));
    // Periodic transients (clicks) give the peaks a recognisable shape.
    if (i % Math.round(sampleRate * 0.75) === 0) samples[i] = Math.min(1, samples[i]! + 0.5);
  }
  return samples;
}

export function writeFixture(dir: string, name: string, durationMs: number, sampleRate: number): string {
  const p = path.join(dir, name);
  fs.mkdirSync(dir, { recursive: true });
  const samples = makeSignal(durationMs, sampleRate);
  fs.writeFileSync(p, encodeWavPcm16(samples, sampleRate, 1));
  return p;
}

export function generateFixtures(dir: string): string[] {
  return [
    writeFixture(dir, 'long-60s.wav', 60_000, 8000),
    writeFixture(dir, 'long-30s.wav', 30_000, 16_000),
  ];
}

if (require.main === module) {
  const dir = process.argv[2] ?? path.resolve(__dirname, '../../fixtures');
  const files = generateFixtures(dir);
  process.stdout.write(`wrote ${files.join('\n')}\n`);
}
