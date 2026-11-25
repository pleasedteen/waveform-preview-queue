/**
 * Programmatic harness used by tests, demos and `npm run smoke`.
 *
 * Starts a queue on an ephemeral port with its own SQLite file, lets tests
 * submit fixtures, run real worker/reader loops in-process and inspect stats
 * (probe counts, takeover latencies, per-segment digests).
 */

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { AddressInfo } from 'node:net';
import { QueueService } from './queue/service';
import { createServer } from './queue/server';
import { Worker, type WorkerOptions } from './worker/worker';
import { Reader, type ReaderOptions } from './reader/reader';
import { ApiClient } from './api-client';
import type { ClaimGrant, CommitResult, QueueStats, RecordingView, TakeoverRecord } from './types';
import type { SegmentPayload } from './protocol';

export interface HarnessOptions {
  dir?: string;
  leaseMs?: number;
  defaultAckWindow?: number;
  idleProbeIntervalMs?: number;
  synchronous?: 'FULL' | 'NORMAL';
  keepDir?: boolean;
  quiet?: boolean;
}

export interface Harness {
  dir: string;
  dbPath: string;
  url: string;
  port: number;
  service: QueueService;
  client: ApiClient;
  logs: string[];
  submit(input: { id?: string; path: string; segmentMs: number; ackWindow?: number }): RecordingView;
  worker(id: string, opts?: Partial<Omit<WorkerOptions, 'queueUrl' | 'workerId'>>): Worker;
  reader(id: string, opts?: Partial<Omit<ReaderOptions, 'queueUrl' | 'recordingId'>>): Reader;
  list(): RecordingView[];
  get(id: string): RecordingView;
  segments(id: string, from?: number, limit?: number): Array<SegmentPayload & { attempt: number }>;
  ack(id: string, cursor: number): Promise<unknown>;
  stats(): QueueStats;
  waitFor(check: () => boolean | Promise<boolean>, timeoutMs: number, label: string): Promise<number>;
  sleep(ms: number): Promise<void>;
  /** Stop the HTTP server + service but keep the directory (for restart tests). */
  closeServerOnly(): Promise<void>;
  close(): Promise<void>;
}

export async function startHarness(opts: HarnessOptions = {}): Promise<Harness> {
  const dir = opts.dir ?? fs.mkdtempSync(path.join(os.tmpdir(), 'preview-queue-'));
  fs.mkdirSync(dir, { recursive: true });
  const dbPath = path.join(dir, 'queue.sqlite');
  const logs: string[] = [];
  const record = (line: string) => {
    logs.push(line);
    if (!opts.quiet && process.env.PREVIEW_QUEUE_VERBOSE === '1') process.stderr.write(`  · ${line}\n`);
  };
  const service = new QueueService({
    dbPath,
    leaseMs: opts.leaseMs ?? 600,
    defaultAckWindow: opts.defaultAckWindow ?? 4,
    idleProbeIntervalMs: opts.idleProbeIntervalMs ?? 5000,
    synchronous: opts.synchronous ?? 'NORMAL',
    logger: record,
  });
  const server = createServer(service);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as AddressInfo).port;
  const url = `http://127.0.0.1:${port}`;
  const client = new ApiClient({ host: '127.0.0.1', port, timeoutMs: 30_000 });

  const fixture = (() => {
    let dir2 = __dirname;
    for (let i = 0; i < 6; i++) {
      if (fs.existsSync(path.join(dir2, 'package.json')) && fs.existsSync(path.join(dir2, 'fixtures'))) {
        return path.join(dir2, 'fixtures');
      }
      dir2 = path.dirname(dir2);
    }
    return path.resolve(__dirname, '../fixtures');
  })();

  let serverClosed = false;
  const stopServer = async () => {
    if (serverClosed) return;
    serverClosed = true;
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections?.();
    });
    await service.close();
  };

  const harness: Harness = {
    dir,
    dbPath,
    url,
    port,
    service,
    client,
    logs,
    submit(input) {
      const wav = path.isAbsolute(input.path) ? input.path : path.join(fixture, input.path);
      return service.submit({ ...input, path: wav });
    },
    worker(id, workerOpts = {}) {
      return new Worker({
        queueUrl: url,
        workerId: id,
        decodeMs: 5,
        logger: (line) => record(`[${id}] ${line}`),
        ...workerOpts,
      });
    },
    reader(id, readerOpts = {}) {
      return new Reader({
        queueUrl: url,
        recordingId: id,
        processMs: 0,
        logger: (line) => record(`[reader:${id}] ${line}`),
        ...readerOpts,
      });
    },
    list: () => service.listRecordings(),
    get: (id) => service.getRecording(id),
    segments: (id, from = 0, limit = 500) => service.getSegments(id, from, limit).segments,
    async ack(id, cursor) {
      return client.post(`/recordings/${encodeURIComponent(id)}/acks`, { cursor });
    },
    stats: () => service.stats(),
    async waitFor(check, timeoutMs, label) {
      const started = Date.now();
      for (;;) {
        if (await check()) return Date.now() - started;
        if (Date.now() - started > timeoutMs) {
          throw new Error(`timed out after ${timeoutMs} ms waiting for: ${label}`);
        }
        await harness.sleep(5);
      }
    },
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
    async closeServerOnly() {
      await stopServer();
    },
    async close() {
      client.close();
      await stopServer();
      if (!opts.keepDir) fs.rmSync(dir, { recursive: true, force: true });
    },
  };
  return harness;
}

/** Raw-claim helper for tests that simulate a stuck/rogue worker. */
export async function rawClaim(h: Harness, workerId: string): Promise<ClaimGrant> {
  const res = await h.client.post<{ grant: ClaimGrant | null }>('/claim', { workerId, waitMs: 0 });
  if (!res.grant) throw new Error(`no grant for ${workerId}`);
  return res.grant;
}

export async function rawCommit(h: Harness, grant: ClaimGrant, segment: unknown): Promise<CommitResult> {
  return h.client.post<CommitResult>(`/recordings/${encodeURIComponent(grant.recording.id)}/segments`, {
    token: grant.token,
    segment,
  });
}

export function lastTakeover(h: Harness, recordingId?: string): TakeoverRecord | undefined {
  const all = h.stats().takeovers;
  for (let i = all.length - 1; i >= 0; i--) {
    if (!recordingId || all[i]!.recordingId === recordingId) return all[i];
  }
  return undefined;
}

/** Count probes (claim scans + renewal checks) inside [from, to). */
export function probesBetween(h: Harness, from: number, to: number): { claim: number; renew: number } {
  const stats = h.stats();
  let claim = 0;
  let renew = 0;
  for (const ev of stats.probes.events) {
    if (ev.at >= from && ev.at < to) {
      if (ev.reason === 'claim') claim++;
      else renew++;
    }
  }
  return { claim, renew };
}
