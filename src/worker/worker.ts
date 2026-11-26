/**
 * Worker: claims recordings, renews leases, decodes segments and commits them.
 *
 * A worker never writes progress itself — every advancement is accepted or
 * rejected by the queue under the current lease token. Claim requests are long
 * polls, so an idle worker costs the queue nothing until work appears.
 */

import fs from 'node:fs';
import path from 'node:path';
import { ApiClient, ApiError } from '../api-client';
import type { ClaimGrant, CommitResult } from '../types';
import type { ContinuationState, SegmentPayload } from '../protocol';
import { decodeSegment } from '../decoder/mock';
import { loadWav, type LoadedWav } from '../decoder/wav';
import { sleep } from '../util/common';

export interface WorkerOptions {
  queueUrl: string;
  workerId: string;
  /** Simulated decode time per segment, ms. */
  decodeMs: number;
  /** Lease heartbeat interval, ms. Defaults to a third of the granted lease. */
  heartbeatMs?: number;
  logger?: (line: string) => void;
  /** Called after each commit, for instrumentation/tests. */
  onCommit?: (info: {
    recordingId: string;
    idx: number;
    attempt: number;
    progress: number;
    totalSegments: number;
    completed: boolean;
    shouldYield: boolean;
    workerId: string;
    at: number;
  }) => void;
  /** Called when a lease is lost and the worker goes back to claiming. */
  onLeaseLost?: (info: { recordingId: string; code: string; workerId: string; at: number }) => void;
}

export interface WorkerStatus {
  workerId: string;
  running: boolean;
  current: { recordingId: string; attempt: number; nextIdx: number } | null;
  segmentsCommitted: number;
  claims: number;
  leaseLosses: number;
  yields: number;
  lastError: string | null;
}

interface WavCacheEntry {
  mtimeMs: number;
  size: number;
  wav: LoadedWav;
}

export class Worker {
  private readonly client: ApiClient;
  private readonly log: (line: string) => void;
  private readonly wavCache = new Map<string, WavCacheEntry>();
  private stopped = false;
  private currentAbort: AbortController | null = null;
  /** Aborts the pending claim long-poll so stop() is immediate. */
  private claimAbort: AbortController | null = null;
  private stopHeartbeat: (() => void) | null = null;
  private loopPromise: Promise<void> | null = null;
  private readonly status: WorkerStatus;

  constructor(private readonly opts: WorkerOptions) {
    this.client = new ApiClient({
      host: hostOf(opts.queueUrl),
      port: portOf(opts.queueUrl),
      timeoutMs: 30_000,
    });
    this.log = opts.logger ?? (() => {});
    this.status = {
      workerId: opts.workerId,
      running: false,
      current: null,
      segmentsCommitted: 0,
      claims: 0,
      leaseLosses: 0,
      yields: 0,
      lastError: null,
    };
  }

  getStatus(): WorkerStatus {
    return { ...this.status, current: this.status.current ? { ...this.status.current } : null };
  }

  start(): void {
    if (this.loopPromise) throw new Error('worker already started');
    this.status.running = true;
    this.loopPromise = this.loop();
  }

  /** Ask the worker to stop; in-flight decodes abort and uncommitted work is dropped. */
  async stop(): Promise<void> {
    this.stopped = true;
    this.currentAbort?.abort();
    this.claimAbort?.abort();
    this.stopHeartbeat?.();
    if (this.loopPromise) await this.loopPromise.catch(() => {});
    this.status.running = false;
    this.client.close();
  }

  private async loop(): Promise<void> {
    while (!this.stopped) {
      let grant: ClaimGrant | null = null;
      const claimAbort = new AbortController();
      this.claimAbort = claimAbort;
      try {
        // Idle claims are held open indefinitely: a parked worker must not
        // re-poll the queue on a client timeout, because that would leak
        // extra "is there work?" checks into idle windows. stop() aborts it.
        const res = await this.client.post<{ grant: ClaimGrant | null }>(
          '/claim',
          { workerId: this.opts.workerId },
          { signal: claimAbort.signal, timeoutMs: 0 },
        );
        grant = res.grant;
      } catch (err) {
        if (this.stopped) break;
        this.status.lastError = String((err as Error).message ?? err);
        this.log(`claim failed: ${this.status.lastError}`);
        await sleep(100);
        continue;
      } finally {
        this.claimAbort = null;
      }
      if (!grant) {
        if (this.stopped) break;
        await sleep(50);
        continue;
      }
      this.status.claims++;
      try {
        await this.process(grant);
      } catch (err) {
        if (!this.stopped) {
          this.status.lastError = String((err as Error).message ?? err);
          this.log(`processing ${grant.recording.id} failed: ${this.status.lastError}`);
          await sleep(25);
        }
      }
    }
  }

  private async process(grant: ClaimGrant): Promise<void> {
    const { recording } = grant;
    this.status.current = { recordingId: recording.id, attempt: grant.attempt, nextIdx: recording.progress };
    this.log(`claimed ${recording.id} attempt=${grant.attempt} at segment ${recording.progress}/${recording.totalSegments}`);

    // Report the takeover before doing any work: this is what lets the queue
    // distinguish "lease issued but the worker vanished" from "the worker
    // reported and then got stuck before its first segment landed".
    if (!(await this.reportTakeover(grant))) return;

    const wav = this.openWav(recording.path);
    let state: ContinuationState = recording.continuation;
    let nextIdx = recording.progress;
    let leaseExpiresAt = grant.expiresAt;
    let leaseLost: ApiError | null = null;

    const abort = new AbortController();
    this.currentAbort = abort;

    const heartbeatMs = Math.max(20, this.opts.heartbeatMs ?? Math.floor(grant.leaseMs / 3));
    this.stopHeartbeat = this.startHeartbeat(
      grant.token,
      recording.id,
      heartbeatMs,
      (expiresAt) => {
        leaseExpiresAt = expiresAt;
      },
      (err) => {
        leaseLost = err;
      },
    );

    try {
      while (nextIdx < recording.totalSegments) {
        if (this.stopped) return;
        if (leaseLost) {
          this.noteLeaseLost(recording.id, leaseLost);
          return;
        }
        this.status.current = { recordingId: recording.id, attempt: grant.attempt, nextIdx };
        let payload: SegmentPayload;
        try {
          payload = await decodeSegment(wav, recording.segmentMs, nextIdx, state, {
            delayMs: this.opts.decodeMs,
            signal: abort.signal,
          });
        } catch (err) {
          if (this.stopped) return;
          throw err;
        }
        if (this.stopped) return;

        let result: CommitResult;
        try {
          result = await this.client.post<CommitResult>(
            `/recordings/${encodeURIComponent(recording.id)}/segments`,
            { token: grant.token, segment: payload },
            { timeoutMs: 15_000 },
          );
        } catch (err) {
          if (err instanceof ApiError && isLeaseError(err.code)) {
            this.noteLeaseLost(recording.id, err);
            return;
          }
          throw err;
        }

        this.status.segmentsCommitted++;
        nextIdx = result.progress;
        state = {
          version: 1,
          cursor: payload.startSample + payload.sampleCount,
          checksum: payload.checksum,
          carry: payload.outputCarry,
        };
        this.opts.onCommit?.({
          recordingId: recording.id,
          idx: payload.idx,
          attempt: grant.attempt,
          progress: result.progress,
          totalSegments: result.totalSegments,
          completed: result.completed,
          shouldYield: result.shouldYield,
          workerId: this.opts.workerId,
          at: Date.now(),
        });
        this.log(
          `committed ${recording.id} segment ${payload.idx} by ${this.opts.workerId} ` +
            `(${result.progress}/${result.totalSegments})` +
            (result.completed ? ' COMPLETE' : ''),
        );
        if (result.completed) return;
        if (result.shouldYield) {
          this.status.yields++;
          this.log(
            `yielding ${recording.id}: ${result.unconfirmed}/${result.ackWindow} unconfirmed segments`,
          );
          return;
        }
        if (result.leaseExpiresAt) leaseExpiresAt = result.leaseExpiresAt;
        void leaseExpiresAt;
      }
    } finally {
      this.stopHeartbeat?.();
      this.stopHeartbeat = null;
      this.currentAbort = null;
      this.status.current = null;
    }
  }

  /**
   * Tell the queue this worker took over the claimed recording. Returns false
   * when the lease is already lost (the worker goes back to claiming instead
   * of decoding against a dead lease). Other failures are best effort: the
   * commit path enforces the lease anyway.
   */
  private async reportTakeover(grant: ClaimGrant): Promise<boolean> {
    const recordingId = grant.recording.id;
    try {
      await this.client.post(
        `/recordings/${encodeURIComponent(recordingId)}/takeover-report`,
        { token: grant.token, attempt: grant.attempt },
        { timeoutMs: 10_000 },
      );
      return true;
    } catch (err) {
      if (err instanceof ApiError && isLeaseError(err.code)) {
        this.noteLeaseLost(recordingId, err);
        return false;
      }
      if (!this.stopped) {
        this.log(`takeover report for ${recordingId} failed: ${(err as Error).message}`);
      }
      return true;
    }
  }

  private noteLeaseLost(recordingId: string, err: ApiError): void {
    this.status.leaseLosses++;
    this.log(`lease lost on ${recordingId} (${err.code}); going back to claiming`);
    this.opts.onLeaseLost?.({ recordingId, code: err.code, workerId: this.opts.workerId, at: Date.now() });
  }

  private startHeartbeat(
    token: string,
    recordingId: string,
    intervalMs: number,
    onRenewed: (expiresAt: number) => void,
    onLost: (err: ApiError) => void,
  ): () => void {
    let cancelled = false;
    let timer: NodeJS.Timeout | null = null;
    const tick = async () => {
      if (cancelled || this.stopped) return;
      try {
        const res = await this.client.post<{ expiresAt: number }>(
          `/recordings/${encodeURIComponent(recordingId)}/leases/renew`,
          { token },
          { timeoutMs: Math.min(10_000, intervalMs * 2) },
        );
        onRenewed(res.expiresAt);
      } catch (err) {
        if (err instanceof ApiError && isLeaseError(err.code)) {
          onLost(err);
          return;
        }
        if (!cancelled) this.log(`heartbeat error on ${recordingId}: ${(err as Error).message}`);
      }
      if (!cancelled) {
        timer = setTimeout(tick, intervalMs);
        timer.unref?.();
      }
    };
    timer = setTimeout(tick, intervalMs);
    timer.unref?.();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
      timer = null;
    };
  }

  private openWav(p: string): LoadedWav {
    const abs = path.resolve(p);
    const stat = fs.statSync(abs);
    const cached = this.wavCache.get(abs);
    if (cached && cached.mtimeMs === stat.mtimeMs && cached.size === stat.size) return cached.wav;
    const wav = loadWav(abs);
    this.wavCache.set(abs, { mtimeMs: stat.mtimeMs, size: stat.size, wav });
    return wav;
  }
}

function isLeaseError(code: string): boolean {
  return code === 'lease_lost' || code === 'lease_expired' || code === 'lease_fenced' || code === 'stale_lease';
}

function hostOf(url: string): string {
  return new URL(url).hostname;
}
function portOf(url: string): number {
  const u = new URL(url);
  return u.port ? Number(u.port) : u.protocol === 'https:' ? 443 : 80;
}
