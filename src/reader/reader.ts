/**
 * Preview reader: consumes segments strictly in order and confirms the
 * contiguous prefix it has processed. On reconnect it resumes from the
 * persisted ack cursor, so no segment is presented twice as "new".
 */

import { ApiClient, ApiError } from '../api-client';
import type { AckResult, RecordingView } from '../types';
import type { SegmentPayload } from '../protocol';
import { sleep } from '../util/common';

export interface ReaderOptions {
  queueUrl: string;
  recordingId: string;
  /** Simulated preview work per segment, ms. */
  processMs?: number;
  /** Poll interval when there are no new segments, ms. */
  idlePollMs?: number;
  logger?: (line: string) => void;
  /** Called for every segment presented by the reader. */
  onSegment?: (info: {
    recordingId: string;
    idx: number;
    attempt: number;
    reconnected: boolean;
    at: number;
  }) => void;
  /** Called after every persisted ack. */
  onAck?: (info: AckResult & { readerId: string; at: number }) => void;
}

export interface ReaderStatus {
  recordingId: string;
  ackCursor: number;
  processed: number;
  running: boolean;
  paused: boolean;
  reconnects: number;
  lastError: string | null;
}

export class Reader {
  private readonly client: ApiClient;
  private readonly log: (line: string) => void;
  private stopped = false;
  private requestAbort: AbortController | null = null;
  private paused = false;
  private loopPromise: Promise<void> | null = null;
  private status: ReaderStatus;

  constructor(private readonly opts: ReaderOptions) {
    this.client = new ApiClient({
      host: hostOf(opts.queueUrl),
      port: portOf(opts.queueUrl),
      timeoutMs: 15_000,
    });
    this.log = opts.logger ?? (() => {});
    this.status = {
      recordingId: opts.recordingId,
      ackCursor: -1,
      processed: 0,
      running: false,
      paused: false,
      reconnects: 0,
      lastError: null,
    };
  }

  getStatus(): ReaderStatus {
    return { ...this.status };
  }

  /** Pause the reader: it stops reading and therefore stops acking. */
  pause(): void {
    this.paused = true;
    this.status.paused = true;
    this.log(`reader paused on ${this.opts.recordingId}`);
  }

  resume(): void {
    this.paused = false;
    this.status.paused = false;
    this.log(`reader resumed on ${this.opts.recordingId}`);
  }

  start(): void {
    if (this.loopPromise) throw new Error('reader already started');
    this.status.running = true;
    this.loopPromise = this.loop().finally(() => {
      this.status.running = false;
    });
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.requestAbort?.abort();
    if (this.loopPromise) await this.loopPromise.catch(() => {});
    this.status.running = false;
    this.client.close();
  }

  private async loop(): Promise<void> {
    let cursor = 0;
    // Resume from the durable ack cursor, not from memory: a restarted reader
    // must neither skip nor re-present already confirmed segments.
    try {
      const view = await this.client.get<RecordingView>(
        `/recordings/${encodeURIComponent(this.opts.recordingId)}`,
      );
      cursor = view.ackCursor;
      this.status.ackCursor = cursor;
      this.status.reconnects++;
    } catch (err) {
      this.status.lastError = String((err as Error).message ?? err);
      this.log(`reader cannot read recording: ${this.status.lastError}`);
      return;
    }
    while (!this.stopped) {
      // A segment read after resuming from a persisted ack cursor is flagged
      // `reconnected`, so callers can tell a fresh read from a resumed one.
      let reconnected = cursor > 0;
      if (this.paused) {
        await sleep(25);
        continue;
      }
      let page: { recording: RecordingView; segments: Array<SegmentPayload & { attempt: number }>; next: number };
      try {
        const abort = new AbortController();
        this.requestAbort = abort;
        page = await this.client.get(
          `/recordings/${encodeURIComponent(this.opts.recordingId)}/segments?from=${cursor}&limit=50`,
          { signal: abort.signal },
        );
      } catch (err) {
        if (this.stopped) break;
        if (err instanceof ApiError && err.status === 404) throw err;
        this.status.reconnects++;
        this.status.lastError = String((err as Error).message ?? err);
        this.log(`reader reconnecting: ${this.status.lastError}`);
        await sleep(Math.max(25, this.opts.idlePollMs ?? 50));
        continue;
      } finally {
        this.requestAbort = null;
      }
      // The queue carries the durable ack cursor; resume from it, never from
      // memory, so a reconnecting reader cannot skip or repeat confirmed work.
      // (The queue only lets the cursor move forward, so taking the max is a
      // no-op in practice and protective against a stale response.)
      cursor = Math.max(cursor, page.recording.ackCursor);
      this.status.ackCursor = cursor;

      if (page.segments.length === 0) {
        if (page.recording.state === 'completed' && cursor >= page.recording.totalSegments) {
          this.log(`reader finished ${this.opts.recordingId}`);
          return;
        }
        await sleep(this.opts.idlePollMs ?? 40);
        continue;
      }

      for (const segment of page.segments) {
        if (this.stopped) return;
        while (this.paused && !this.stopped) await sleep(25);
        if (this.stopped) return;
        this.opts.onSegment?.({
          recordingId: this.opts.recordingId,
          idx: segment.idx,
          attempt: segment.attempt,
          reconnected,
          at: Date.now(),
        });
        if (this.opts.processMs) await sleep(this.opts.processMs);
        // The segment was fully processed, so it is confirmed even if a pause
        // arrived meanwhile: pausing never leaves a half-read segment to be
        // presented a second time.
        cursor = segment.idx + 1;
        const ack = await this.client.post<AckResult>(
          `/recordings/${encodeURIComponent(this.opts.recordingId)}/acks`,
          { cursor },
        );
        this.status.ackCursor = ack.ackCursor;
        this.status.processed++;
        this.opts.onAck?.({ ...ack, readerId: this.status.recordingId, at: Date.now() });
      }
    }
  }
}

function hostOf(url: string): string {
  return new URL(url).hostname;
}
function portOf(url: string): number {
  const u = new URL(url);
  return u.port ? Number(u.port) : u.protocol === 'https:' ? 443 : 80;
}
