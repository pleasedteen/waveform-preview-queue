/** Minimal RIFF/WAVE (PCM16) reader/writer used by the decoder and fixtures. */

import fs from 'node:fs';
import { AppError } from '../util/common';

export interface WavInfo {
  sampleRate: number;
  channels: number;
  bitsPerSample: number;
  frames: number;
  durationMs: number;
  dataOffset: number;
  dataLength: number;
}

export interface LoadedWav {
  info: WavInfo;
  /** Channel-mixed samples normalised to [-1, 1]. */
  samples: Float64Array;
}

export function parseWav(buffer: Buffer): LoadedWav {
  if (buffer.length < 44 || buffer.toString('ascii', 0, 4) !== 'RIFF' || buffer.toString('ascii', 8, 12) !== 'WAVE') {
    throw new AppError(400, 'bad_wav', 'not a RIFF/WAVE file');
  }
  let offset = 12;
  let fmt: { format: number; channels: number; sampleRate: number; bitsPerSample: number } | undefined;
  let dataOffset = -1;
  let dataLength = 0;
  while (offset + 8 <= buffer.length) {
    const chunkId = buffer.toString('ascii', offset, offset + 4);
    const chunkSize = buffer.readUInt32LE(offset + 4);
    const body = offset + 8;
    if (chunkId === 'fmt ') {
      if (chunkSize < 16) throw new AppError(400, 'bad_wav', 'short fmt chunk');
      fmt = {
        format: buffer.readUInt16LE(body),
        channels: buffer.readUInt16LE(body + 2),
        sampleRate: buffer.readUInt32LE(body + 4),
        bitsPerSample: buffer.readUInt16LE(body + 14),
      };
    } else if (chunkId === 'data') {
      dataOffset = body;
      dataLength = Math.min(chunkSize, buffer.length - body);
    }
    offset = body + chunkSize + (chunkSize & 1);
  }
  if (!fmt || dataOffset < 0) throw new AppError(400, 'bad_wav', 'missing fmt or data chunk');
  if (fmt.format !== 1 || fmt.bitsPerSample !== 16) {
    throw new AppError(400, 'bad_wav', 'only 16-bit PCM WAV is supported');
  }
  if (fmt.channels < 1 || fmt.sampleRate <= 0) throw new AppError(400, 'bad_wav', 'invalid fmt chunk');
  const frameBytes = fmt.channels * 2;
  const frames = Math.floor(dataLength / frameBytes);
  const samples = new Float64Array(frames);
  for (let f = 0; f < frames; f++) {
    let sum = 0;
    for (let c = 0; c < fmt.channels; c++) {
      sum += buffer.readInt16LE(dataOffset + f * frameBytes + c * 2);
    }
    samples[f] = sum / (fmt.channels * 32768);
  }
  const info: WavInfo = {
    sampleRate: fmt.sampleRate,
    channels: fmt.channels,
    bitsPerSample: fmt.bitsPerSample,
    frames,
    durationMs: (frames / fmt.sampleRate) * 1000,
    dataOffset,
    dataLength,
  };
  return { info, samples };
}

export function loadWav(path: string): LoadedWav {
  return parseWav(fs.readFileSync(path));
}

export function encodeWavPcm16(samples: Float64Array | number[], sampleRate = 8000, channels = 1): Buffer {
  const frames = Math.floor(samples.length / channels);
  const dataLength = frames * channels * 2;
  const buffer = Buffer.alloc(44 + dataLength);
  buffer.write('RIFF', 0, 'ascii');
  buffer.writeUInt32LE(36 + dataLength, 4);
  buffer.write('WAVE', 8, 'ascii');
  buffer.write('fmt ', 12, 'ascii');
  buffer.writeUInt32LE(16, 16);
  buffer.writeUInt16LE(1, 20);
  buffer.writeUInt16LE(channels, 22);
  buffer.writeUInt32LE(sampleRate, 24);
  buffer.writeUInt32LE(sampleRate * channels * 2, 28);
  buffer.writeUInt16LE(channels * 2, 32);
  buffer.writeUInt16LE(16, 34);
  buffer.write('data', 36, 'ascii');
  buffer.writeUInt32LE(dataLength, 40);
  for (let f = 0; f < frames; f++) {
    for (let c = 0; c < channels; c++) {
      const v = Math.max(-1, Math.min(1, samples[f * channels + c] ?? 0));
      buffer.writeInt16LE(Math.round(v * 32767), 44 + (f * channels + c) * 2);
    }
  }
  return buffer;
}
