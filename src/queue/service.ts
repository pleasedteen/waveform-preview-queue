/**
 * Queue service: persistence, leasing, segment commits, reader acks and the
 * idle-probe budget.
 *
 * Invariants enforced inside SQLite transactions (`BEGIN IMMEDIATE`):
 *  - A segment only takes effect together with the progress counter and the
 *    decoder continuation state. There is no window where one is persisted
 *    without the other.
 *  - `progress` advances one contiguous index at a time and segments are keyed
 *    by (recording_id, idx), so each segment exists exactly once. A retried
 *    commit of a segment that already took effect is recognised and never
 *    applied twice.
 *  - Only the holder of the current, unexpired, un-fenced lease token can
 *    advance the recording.
 *  - A recording is claimable only while `progress - ack_cursor < ack_window`.
 *    Filling that window fences the lease (`yielded = 1`) in the same
 *    transaction as the commit; the worker is told to yield, while any other
 *    worker can claim the recording the moment a reader ack frees space —
 *    without waiting for the fenced lease to expire.
 *
 * Idle budget:
 *  - Claim requests are event driven long polls: a parked worker performs no
 *    database probes.
 *  - One internal sweep runs every `idleProbeIntervalMs` (default 5000 ms) and
 *    performs exactly two probes (claim scan + lease maintenance). Two sweeps
 *    are always >= 5 s apart, so any 4 s window contains at most one sweep,
 *    i.e. at most 2 proactive checks even when every recording is paused and
 *    nothing is arriving.
 *  - Paused (fenced) leases are never renewed in a loop. Their progress is
 *    already durable; renewal of a fenced lease is refused and counted.
 */

import fs from 'node:fs';
import { Db } from './db';
import { AppError, nowMs, randomId } from '../util/common';
import type {
  AckResult,
  ClaimGrant,
  CommitResult,
  LeaseRow,
  LeaseView,
  ProbeEvent,
  QueueEvent,
  QueueOptions,
  QueueStats,
  RecordingRow,
  RecordingView,
  RenewalEvent,
  TakeoverRecord,
} from '../types';
import {
  type ContinuationState,
  type SegmentPayload,
  initialContinuation,
  validateSegmentPayload,
} from '../protocol';
import { continuationAfter } from '../decoder/mock';
import { segmentBounds } from '../segments';
import { loadWav } from '../decoder/wav';

const MAX_EVENTS = 3000;
/** Freed-space markers older than this are not reported as takeovers. */
const MAX_TAKEOVER_TRACK_MS = 120_000;

export interface QueueServiceOptions {
  dbPath: string;
  leaseMs: number;
  defaultAckWindow: number;
  idleProbeIntervalMs?: number;
  synchronous?: 'FULL' | 'NORMAL';
  logger?: (line: string) => void;
}

export interface SubmitInput {
  id?: string;
  path: string;
  segmentMs: number;
  ackWindow?: number;
}

interface Waiter {
  workerId: string;
  resolve: (grant: ClaimGrant | null) => void;
  timer?: NodeJS.Timeout;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface JoinedRow extends RecordingRow {
  w_worker_id: string | null;
  w_attempt: number | null;
  w_acquired_at: number | null;
  w_renewed_at: number | null;
  w_expires_at: number | null;
  w_yielded: number | null;
}

interface PendingFree {
  since: number;
  reason: 'ack_free' | 'lease_expiry';
}

export class QueueService {
  readonly db: Db;
  readonly options: Required<QueueOptions>;
  private readonly log: (line: string) => void;
  private readonly startedAt = nowMs();
  private waiters: Waiter[] = [];
  private readonly sweepTimer: NodeJS.Timeout;
  private expiryTimer: NodeJS.Timeout | null = null;
  private closed = false;
  /** Recordings that became claimable in the past and have not been picked up yet. */
  private readonly pendingFree = new Map<string, PendingFree>();
  /** Last time the maintenance sweep actually ran; guards against timer catch-up. */
  private lastSweepAt = 0;
  private readonly takeovers: TakeoverRecord[] = [];

  private counters = {
    submits: 0,
    grants: 0,
    renewals: 0,
    commits: 0,
    duplicateCommits: 0,
    acks: 0,
    completed: 0,
    yields: 0,
    leaseExpiries: 0,
    takeoversFenced: 0,
    takeoversExpired: 0,
    lostLeaseRejections: 0,
  };

  private probeEvents: ProbeEvent[] = [];
  private probeSeq = 0;
  private probeClaim = 0;
  private probeRenew = 0;

  private renewalEvents: RenewalEvent[] = [];
  private renewalTotal = 0;
  private renewalPaused = 0;

  private events: QueueEvent[] = [];
  private eventSeq = 0;

  constructor(opts: QueueServiceOptions) {
    this.options = {
      leaseMs: opts.leaseMs,
      defaultAckWindow: opts.defaultAckWindow,
      idleProbeIntervalMs: opts.idleProbeIntervalMs ?? 5000,
    };
    this.log = opts.logger ?? (() => {});
    this.db = new Db({ path: opts.dbPath, synchronous: opts.synchronous });

    // Leases are grants of the queue process that issued them. After a restart
    // no live worker can prove it still holds one, so they are dropped: a
    // running worker's next renew/commit gets `lease_lost` and it re-claims
    // from the last persisted progress. Recordings, progress, completion and
    // reader acks are untouched, so unfinished recordings with free space
    // resume automatically.
    const purged = this.db.run('DELETE FROM leases');
    if (purged > 0) this.log(`dropped ${purged} lease(s) left by a previous queue run`);

    this.sweepTimer = setInterval(() => this.idleSweep(), this.options.idleProbeIntervalMs);
    this.lastSweepAt = nowMs();
    this.sweepTimer.unref();
    this.scheduleExpiry();
  }

  // ---------------------------------------------------------------- helpers

  private emit(
    kind: string,
    opts: { recordingId?: string; workerId?: string; detail?: Record<string, unknown> } = {},
  ): void {
    this.eventSeq++;
    this.events.push({
      seq: this.eventSeq,
      at: nowMs(),
      kind,
      recordingId: opts.recordingId,
      workerId: opts.workerId,
      detail: opts.detail,
    });
    if (this.events.length > MAX_EVENTS) this.events.splice(0, this.events.length - MAX_EVENTS);
  }

  private probe(reason: 'claim' | 'renew'): void {
    this.probeSeq++;
    if (reason === 'claim') this.probeClaim++;
    else this.probeRenew++;
    this.probeEvents.push({ seq: this.probeSeq, at: nowMs(), reason, granted: 0 });
    if (this.probeEvents.length > MAX_EVENTS) {
      this.probeEvents.splice(0, this.probeEvents.length - MAX_EVENTS);
    }
  }

  private recordRenewal(ev: RenewalEvent): void {
    if (ev.outcome === 'renewed') {
      this.renewalTotal++;
      this.counters.renewals++;
    }
    this.renewalEvents.push(ev);
    if (this.renewalEvents.length > MAX_EVENTS) {
      this.renewalEvents.splice(0, this.renewalEvents.length - MAX_EVENTS);
    }
  }

  private joined(recordingId: string): JoinedRow | undefined {
    return this.db.get<JoinedRow>(
      `SELECT r.*, l.worker_id AS w_worker_id, l.attempt AS w_attempt,
              l.acquired_at AS w_acquired_at, l.renewed_at AS w_renewed_at,
              l.expires_at AS w_expires_at, l.yielded AS w_yielded
         FROM recordings r
         LEFT JOIN leases l ON l.recording_id = r.id
        WHERE r.id = ?`,
      recordingId,
    );
  }

  private toView(row: JoinedRow, now = nowMs()): RecordingView {
    const unconfirmed = row.progress - row.ack_cursor;
    let lease: LeaseView | null = null;
    if (row.w_worker_id !== null) {
      lease = {
        workerId: row.w_worker_id,
        attempt: row.w_attempt ?? 0,
        acquiredAt: row.w_acquired_at ?? 0,
        renewedAt: row.w_renewed_at ?? 0,
        expiresAt: row.w_expires_at ?? 0,
        yielded: row.w_yielded === 1,
        expired: (row.w_expires_at ?? 0) <= now,
      };
    }
    const paused =
      row.state === 'pending' && unconfirmed >= row.ack_window && row.progress < row.total_segments;
    return {
      id: row.id,
      state: row.state,
      progress: row.progress,
      totalSegments: row.total_segments,
      segmentMs: row.segment_ms,
      ackWindow: row.ack_window,
      ackCursor: row.ack_cursor,
      unconfirmed,
      paused,
      completedAt: row.completed_at,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      frames: row.frames,
      sampleRate: row.sample_rate,
      path: row.path,
      lease,
    };
  }

  private requireRecording(id: string): JoinedRow {
    const row = this.joined(id);
    if (!row) throw new AppError(404, 'not_found', `recording ${id} does not exist`);
    return row;
  }

  private parseContinuation(row: RecordingRow): ContinuationState {
    try {
      const parsed = JSON.parse(row.continuation_json) as ContinuationState;
      if (
        parsed === null ||
        typeof parsed !== 'object' ||
        parsed.version !== 1 ||
        !Number.isInteger(parsed.cursor) ||
        typeof parsed.checksum !== 'number' ||
        typeof parsed.carry !== 'number'
      ) {
        throw new Error('bad shape');
      }
      return parsed;
    } catch {
      throw new AppError(500, 'corrupt_state', `continuation state for ${row.id} is corrupt`);
    }
  }

  // --------------------------------------------------------------- commands

  submit(input: SubmitInput): RecordingView {
    const { segmentMs } = input;
    if (!Number.isInteger(segmentMs) || segmentMs <= 0) {
      throw new AppError(400, 'bad_request', 'segmentMs must be a positive integer');
    }
    const ackWindow = input.ackWindow ?? this.options.defaultAckWindow;
    if (!Number.isInteger(ackWindow) || ackWindow <= 0) {
      throw new AppError(400, 'bad_request', 'ackWindow must be a positive integer');
    }
    if (typeof input.path !== 'string' || input.path.length === 0) {
      throw new AppError(400, 'bad_request', 'path must be a non-empty string');
    }
    const wavPath = fs.existsSync(input.path) ? fs.realpathSync(input.path) : input.path;
    if (!fs.existsSync(wavPath) || !fs.statSync(wavPath).isFile()) {
      throw new AppError(400, 'bad_request', `wav file not found: ${wavPath}`);
    }
    const wav = loadWav(wavPath);
    if (wav.info.frames <= 0) throw new AppError(400, 'bad_request', 'wav file contains no samples');
    const totalSegments = Math.ceil((wav.info.frames * 1000) / (wav.info.sampleRate * segmentMs));
    if (totalSegments <= 0) throw new AppError(400, 'bad_request', 'recording is too short for one segment');
    const id = input.id ?? randomId('rec_');
    if (typeof id !== 'string' || id.length === 0 || id.length > 200) {
      throw new AppError(400, 'bad_request', 'id must be a non-empty string (<= 200 chars)');
    }

    const now = nowMs();
    try {
      this.db.transaction(() => {
        this.db.run(
          `INSERT INTO recordings
             (id, path, frames, sample_rate, total_segments, segment_ms, ack_window,
              state, progress, ack_cursor, attempts, continuation_json, created_at, updated_at, completed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, 0, ?, ?, ?, NULL)`,
          id,
          wavPath,
          wav.info.frames,
          wav.info.sampleRate,
          totalSegments,
          segmentMs,
          ackWindow,
          JSON.stringify(initialContinuation()),
          now,
          now,
        );
      });
    } catch (err) {
      if (String((err as Error)?.message ?? err).includes('UNIQUE')) {
        throw new AppError(409, 'duplicate_recording', `recording ${id} already exists`);
      }
      throw err;
    }
    this.counters.submits++;
    this.emit('submit', {
      recordingId: id,
      detail: { totalSegments, segmentMs, ackWindow, frames: wav.info.frames },
    });
    this.log(`submitted ${id} (${totalSegments} segments of ${segmentMs} ms)`);
    this.pumpWaiters();
    return this.toView(this.requireRecording(id));
  }

  getRecording(id: string): RecordingView {
    return this.toView(this.requireRecording(id));
  }

  listRecordings(): RecordingView[] {
    const rows = this.db.all<JoinedRow>(
      `SELECT r.*, l.worker_id AS w_worker_id, l.attempt AS w_attempt,
              l.acquired_at AS w_acquired_at, l.renewed_at AS w_renewed_at,
              l.expires_at AS w_expires_at, l.yielded AS w_yielded
         FROM recordings r
         LEFT JOIN leases l ON l.recording_id = r.id
        ORDER BY r.created_at ASC, r.id ASC`,
    );
    const now = nowMs();
    return rows.map((r) => this.toView(r, now));
  }

  getSegments(
    id: string,
    from: number,
    limit = 100,
  ): { recording: RecordingView; segments: Array<SegmentPayload & { attempt: number }>; next: number } {
    if (!Number.isInteger(from) || from < 0) throw new AppError(400, 'bad_request', 'from must be >= 0');
    if (!Number.isInteger(limit) || limit <= 0 || limit > 500) {
      throw new AppError(400, 'bad_request', 'limit must be between 1 and 500');
    }
    const view = this.getRecording(id);
    const rows = this.db.all<{ idx: number; attempt: number; payload_json: string }>(
      'SELECT idx, attempt, payload_json FROM segments WHERE recording_id = ? AND idx >= ? ORDER BY idx ASC LIMIT ?',
      id,
      from,
      limit,
    );
    const segments = rows.map((r) => {
      const payload = JSON.parse(r.payload_json) as SegmentPayload;
      return { ...payload, attempt: r.attempt };
    });
    const next = segments.length > 0 ? segments[segments.length - 1]!.idx + 1 : from;
    return { recording: view, segments, next };
  }

  /**
   * Confirm the contiguous prefix the preview reader has processed. Acks are
   * monotonic and may not run ahead of produced segments. An ack that turns a
   * paused recording back into a runnable one wakes parked workers
   * synchronously, so a takeover follows within milliseconds.
   */
  ack(id: string, cursor: number): AckResult {
    if (!Number.isInteger(cursor) || cursor < 0) {
      throw new AppError(400, 'bad_request', 'cursor must be a non-negative integer');
    }
    let result!: AckResult;
    let changed = false;
    this.db.transaction(() => {
      const row = this.requireRecording(id);
      if (cursor > row.progress) {
        throw new AppError(
          409,
          'ack_ahead',
          `cannot ack ${cursor}: only ${row.progress} segment(s) have taken effect`,
        );
      }
      const beforeUnconfirmed = row.progress - row.ack_cursor;
      const wasPaused = row.state === 'pending' && beforeUnconfirmed >= row.ack_window;
      const effective = Math.max(row.ack_cursor, cursor);
      changed = effective !== row.ack_cursor;
      if (changed) {
        this.db.run(
          'UPDATE recordings SET ack_cursor = ?, updated_at = ? WHERE id = ?',
          effective,
          nowMs(),
          id,
        );
      }
      const unconfirmed = row.progress - effective;
      const freedSpace =
        changed && wasPaused && unconfirmed < row.ack_window && row.progress < row.total_segments;
      if (freedSpace) {
        this.pendingFree.set(id, { since: nowMs(), reason: 'ack_free' });
        this.emit('ack_free', {
          recordingId: id,
          detail: { cursor: effective, unconfirmed, ackWindow: row.ack_window },
        });
      } else if (changed) {
        this.emit('ack', { recordingId: id, detail: { cursor: effective } });
      }
      result = {
        recordingId: id,
        ackCursor: effective,
        requestedCursor: cursor,
        progress: row.progress,
        unconfirmed,
        ackWindow: row.ack_window,
        freedSpace,
      };
    });
    if (changed) this.counters.acks++;
    if (result.freedSpace) {
      this.log(`reader freed space on ${id} at ack ${result.ackCursor}`);
      this.pumpWaiters();
    }
    return result;
  }

  // ---------------------------------------------------------------- leasing

  private tryClaim(workerId: string): ClaimGrant | null {
    // Every claim scan is counted: a worker request, or a wake-up triggered by
    // a submit/ack/release/expiry. Parked claim requests do not scan again
    // until woken, which is what keeps the idle budget at <= 2 checks / 4 s.
    this.probe('claim');
    const now = nowMs();
    const grant = this.db.transaction(() => {
      const candidate = this.db.get<RecordingRow>(
        `SELECT r.* FROM recordings r
           LEFT JOIN leases l ON l.recording_id = r.id
          WHERE r.state = 'pending'
            AND r.progress < r.total_segments
            AND (r.progress - r.ack_cursor) < r.ack_window
            AND (l.recording_id IS NULL OR l.yielded = 1 OR l.expires_at <= ?)
          ORDER BY CASE WHEN l.recording_id IS NULL THEN 0
                        WHEN l.yielded = 1 THEN 1
                        ELSE 2 END,
                   r.updated_at ASC, r.id ASC
          LIMIT 1`,
        now,
      );
      if (!candidate) return null;
      const prev = this.db.get<LeaseRow>('SELECT * FROM leases WHERE recording_id = ?', candidate.id);
      if (prev && prev.yielded === 0 && prev.expires_at > now) {
        // A live, un-fenced lease exists. The query must have excluded this
        // recording; returning null is the conservative answer.
        return null;
      }
      if (prev) {
        this.db.run('DELETE FROM leases WHERE recording_id = ?', candidate.id);
        if (prev.yielded === 1) this.counters.takeoversFenced++;
        else {
          this.counters.takeoversExpired++;
          this.counters.leaseExpiries++;
        }
      }
      const attempt = candidate.attempts + 1;
      const token = randomId('lease_');
      const expiresAt = now + this.options.leaseMs;
      this.db.run(
        `INSERT INTO leases
           (recording_id, worker_id, token, attempt, acquired_at, renewed_at, expires_at, yielded)
         VALUES (?, ?, ?, ?, ?, ?, ?, 0)`,
        candidate.id,
        workerId,
        token,
        attempt,
        now,
        now,
        expiresAt,
      );
      this.db.run('UPDATE recordings SET attempts = ?, updated_at = ? WHERE id = ?', attempt, now, candidate.id);
      this.counters.grants++;

      const free = this.pendingFree.get(candidate.id);
      if (free && now - free.since > MAX_TAKEOVER_TRACK_MS) {
        this.pendingFree.delete(candidate.id);
      } else if (free) {
        this.pendingFree.delete(candidate.id);
        const takeover: TakeoverRecord = {
          recordingId: candidate.id,
          workerId,
          reason: free.reason,
          freedAt: free.since,
          claimedAt: now,
          latencyMs: now - free.since,
        };
        this.takeovers.push(takeover);
        if (this.takeovers.length > 500) this.takeovers.shift();
        this.emit('takeover', {
          recordingId: candidate.id,
          workerId,
          detail: { reason: free.reason, latencyMs: takeover.latencyMs },
        });
      }
      this.emit('claim', {
        recordingId: candidate.id,
        workerId,
        detail: { attempt, expiresAt, progress: candidate.progress, replaced: prev ? (prev.yielded ? 'fenced' : 'expired') : 'none' },
      });
      const row = this.requireRecording(candidate.id);
      return {
        token,
        attempt,
        leaseMs: this.options.leaseMs,
        expiresAt,
        recording: { ...this.toView(row, now), continuation: this.parseContinuation(row) },
      } satisfies ClaimGrant;
    });
    if (grant) this.scheduleExpiry();
    return grant;
  }

  /**
   * Claim the next runnable recording.
   *
   * `waitMs === 0` returns immediately. `waitMs > 0` bounds the wait. Omitted
   * `waitMs` parks until a grant is available or the queue closes. Parked
   * requests are event driven and perform no database probes.
   */
  claim(workerId: string, waitMs?: number, signal?: AbortSignal): Promise<ClaimGrant | null> {
    if (!workerId || workerId.length > 200) {
      throw new AppError(400, 'bad_request', 'workerId must be a non-empty string');
    }
    if (signal?.aborted) return Promise.resolve(null);
    const grant = this.tryClaim(workerId);
    if (grant) return Promise.resolve(grant);
    if (waitMs === 0 || this.closed) return Promise.resolve(null);
    return new Promise<ClaimGrant | null>((resolve) => {
      const waiter: Waiter = { workerId, resolve, signal };
      if (waitMs !== undefined && waitMs > 0) {
        waiter.timer = setTimeout(() => {
          this.removeWaiter(waiter);
          resolve(null);
        }, waitMs);
      }
      if (signal) {
        waiter.onAbort = () => {
          this.removeWaiter(waiter);
          resolve(null);
        };
        signal.addEventListener('abort', waiter.onAbort, { once: true });
      }
      this.waiters.push(waiter);
    }).then((g) => {
      if (g) this.scheduleExpiry();
      return g;
    });
  }

  private removeWaiter(waiter: Waiter): void {
    if (waiter.timer) clearTimeout(waiter.timer);
    if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener('abort', waiter.onAbort);
    const idx = this.waiters.indexOf(waiter);
    if (idx >= 0) this.waiters.splice(idx, 1);
  }

  /** Hand runnable recordings to parked workers. Woken by submits and acks. */
  private pumpWaiters(): void {
    if (this.closed || this.waiters.length === 0) return;
    const queue = this.waiters;
    this.waiters = [];
    for (const waiter of queue) {
      if (waiter.timer) clearTimeout(waiter.timer);
      if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener('abort', waiter.onAbort);
      const grant = this.tryClaim(waiter.workerId);
      if (grant) waiter.resolve(grant);
      else this.waiters.push(waiter);
    }
    if (this.waiters.length > 0) this.scheduleExpiry();
  }

  renew(id: string, token: string): { expiresAt: number; attempt: number; workerId: string } {
    const now = nowMs();
    let freed = false;
    try {
      const outcome = this.db.transaction(() => {
      this.requireRecording(id);
      const lease = this.db.get<LeaseRow>('SELECT * FROM leases WHERE recording_id = ?', id);
      if (!lease || lease.token !== token) {
        this.counters.lostLeaseRejections++;
        this.recordRenewal({ at: now, recordingId: id, workerId: lease?.worker_id ?? 'unknown', outcome: 'lost' });
        throw new AppError(409, 'lease_lost', 'lease is no longer held by this worker');
      }
      if (lease.yielded === 1) {
        this.renewalPaused++;
        // Renewing a paused recording is exactly the kind of periodic check
        // the idle budget limits, so it is counted as a renewal probe.
        this.probe('renew');
        this.recordRenewal({ at: now, recordingId: id, workerId: lease.worker_id, outcome: 'fenced' });
        throw new AppError(
          409,
          'lease_fenced',
          'recording reached its unconfirmed-segment cap; lease is fenced',
        );
      }
      if (lease.expires_at <= now) {
        this.db.run('DELETE FROM leases WHERE recording_id = ?', id);
        this.counters.leaseExpiries++;
        this.pendingFree.set(id, { since: lease.expires_at, reason: 'lease_expiry' });
        this.recordRenewal({ at: now, recordingId: id, workerId: lease.worker_id, outcome: 'expired' });
        freed = true;
        throw new AppError(409, 'lease_expired', 'lease expired before renewal');
      }
      const expiresAt = now + this.options.leaseMs;
      this.db.run('UPDATE leases SET expires_at = ?, renewed_at = ? WHERE recording_id = ?', expiresAt, now, id);
      this.recordRenewal({ at: now, recordingId: id, workerId: lease.worker_id, outcome: 'renewed' });
      this.emit('renew', { recordingId: id, workerId: lease.worker_id, detail: { expiresAt } });
      return { expiresAt, attempt: lease.attempt, workerId: lease.worker_id };
      });
      return outcome;
    } finally {
      this.scheduleExpiry();
      if (freed) this.pumpWaiters();
    }
  }

  commit(id: string, token: string, input: unknown): CommitResult {
    let freedDuringCommit = false;
    try {
      const result = this.db.transaction(() => {
        const now = nowMs();
        const row = this.requireRecording(id);
        const lease = this.db.get<LeaseRow>('SELECT * FROM leases WHERE recording_id = ?', id);
        if (!lease || lease.token !== token) {
          this.counters.lostLeaseRejections++;
          throw new AppError(
            409,
            'lease_lost',
            'lease is no longer held by this worker; re-claim from persisted progress',
          );
        }
        if (lease.yielded === 1) {
          throw new AppError(409, 'lease_fenced', 'recording is paused at its unconfirmed-segment cap');
        }
        if (lease.expires_at <= now) {
          this.db.run('DELETE FROM leases WHERE recording_id = ?', id);
          this.counters.leaseExpiries++;
          this.pendingFree.set(id, { since: lease.expires_at, reason: 'lease_expiry' });
          freedDuringCommit = true;
          throw new AppError(409, 'lease_expired', 'lease expired before commit');
        }

        // Only after the caller proves it holds the current lease do we look
        // at the payload itself (so a stale holder always gets lease_lost).
        const payload = validateSegmentPayload(input);
        const unconfirmed = row.progress - row.ack_cursor;

        if (payload.idx < row.progress) {
          const stored = this.db.get<{ checksum: string }>(
            'SELECT checksum FROM segments WHERE recording_id = ? AND idx = ?',
            id,
            payload.idx,
          );
          if (stored && stored.checksum === payload.digest) {
            // Identical retry of a segment that already took effect.
            this.counters.duplicateCommits++;
            const newExpiry = now + this.options.leaseMs;
            this.db.run('UPDATE leases SET expires_at = ?, renewed_at = ? WHERE recording_id = ?', newExpiry, now, id);
            return {
              recordingId: id,
              idx: payload.idx,
              progress: row.progress,
              totalSegments: row.total_segments,
              completed: row.state === 'completed',
              ackCursor: row.ack_cursor,
              unconfirmed,
              ackWindow: row.ack_window,
              shouldYield: false,
              leaseExpiresAt: row.state === 'completed' ? null : newExpiry,
              idempotent: true,
            } satisfies CommitResult;
          }
          throw new AppError(
            409,
            'segment_conflict',
            `segment ${payload.idx} already took effect with different content`,
          );
        }
        if (payload.idx > row.progress) {
          throw new AppError(
            409,
            'out_of_order',
            `segment ${payload.idx} cannot take effect before segment ${row.progress}`,
          );
        }

        // Geometry + continuation checks: the segment must start exactly where
        // the persisted continuation says the recording resumes.
        const bounds = segmentBounds(row.frames, row.sample_rate, row.segment_ms, payload.idx);
        const expectedCount = bounds.endSample - bounds.startSample;
        if (payload.sampleRate !== row.sample_rate) {
          throw new AppError(400, 'bad_segment', `sampleRate ${payload.sampleRate} != ${row.sample_rate}`);
        }
        if (payload.startSample !== bounds.startSample) {
          throw new AppError(400, 'bad_segment', `startSample ${payload.startSample} != ${bounds.startSample}`);
        }
        if (payload.sampleCount !== expectedCount) {
          throw new AppError(400, 'bad_segment', `sampleCount ${payload.sampleCount} != ${expectedCount}`);
        }
        if (Math.abs(payload.startMs - bounds.startMs) > 0.5 || Math.abs(payload.endMs - bounds.endMs) > 0.5) {
          throw new AppError(400, 'bad_segment', 'segment time bounds do not match the recording geometry');
        }
        const continuation = this.parseContinuation(row);
        if (payload.startSample !== continuation.cursor) {
          throw new AppError(
            409,
            'continuation_mismatch',
            `segment starts at sample ${payload.startSample}, persisted continuation points at ${continuation.cursor}`,
          );
        }
        if (payload.startChecksum !== (continuation.checksum >>> 0)) {
          throw new AppError(
            409,
            'continuation_mismatch',
            `segment input checksum ${payload.startChecksum} != persisted ${continuation.checksum}`,
          );
        }
        if (Math.abs(payload.inputCarry - continuation.carry) > 1e-12) {
          throw new AppError(
            409,
            'continuation_mismatch',
            `segment input carry ${payload.inputCarry} != persisted ${continuation.carry}`,
          );
        }

        const nextProgress = row.progress + 1;
        const completed = nextProgress === row.total_segments;
        this.db.run(
          `INSERT INTO segments
             (recording_id, idx, attempt, start_ms, end_ms, sample_count, checksum, payload_json, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          id,
          payload.idx,
          lease.attempt,
          payload.startMs,
          payload.endMs,
          payload.sampleCount,
          payload.digest,
          JSON.stringify(payload),
          now,
        );
        this.db.run(
          `UPDATE recordings
              SET progress = ?, continuation_json = ?, updated_at = ?,
                  state = ?, completed_at = ?
            WHERE id = ?`,
          nextProgress,
          JSON.stringify(continuationAfter(payload)),
          now,
          completed ? 'completed' : 'pending',
          completed ? now : null,
          id,
        );
        this.counters.commits++;
        this.emit('commit', {
          recordingId: id,
          workerId: lease.worker_id,
          detail: { idx: payload.idx, progress: nextProgress, completed },
        });
        const afterUnconfirmed = nextProgress - row.ack_cursor;

        if (completed) {
          this.db.run('DELETE FROM leases WHERE recording_id = ?', id);
          this.counters.completed++;
          this.emit('complete', { recordingId: id, workerId: lease.worker_id });
          return {
            recordingId: id,
            idx: payload.idx,
            progress: nextProgress,
            totalSegments: row.total_segments,
            completed: true,
            ackCursor: row.ack_cursor,
            unconfirmed: afterUnconfirmed,
            ackWindow: row.ack_window,
            shouldYield: false,
            leaseExpiresAt: null,
            idempotent: false,
          } satisfies CommitResult;
        }

        const shouldYield = afterUnconfirmed >= row.ack_window;
        const newExpiry = now + this.options.leaseMs;
        this.db.run(
          `UPDATE leases SET yielded = ?, expires_at = ?, renewed_at = ? WHERE recording_id = ?`,
          shouldYield ? 1 : 0,
          newExpiry,
          now,
          id,
        );
        if (shouldYield) {
          this.counters.yields++;
          this.emit('yield', {
            recordingId: id,
            workerId: lease.worker_id,
            detail: { unconfirmed: afterUnconfirmed, ackWindow: row.ack_window },
          });
        }
        return {
          recordingId: id,
          idx: payload.idx,
          progress: nextProgress,
          totalSegments: row.total_segments,
          completed: false,
          ackCursor: row.ack_cursor,
          unconfirmed: afterUnconfirmed,
          ackWindow: row.ack_window,
          shouldYield,
          leaseExpiresAt: newExpiry,
          idempotent: false,
        } satisfies CommitResult;
      });
      return result;
    } finally {
      this.scheduleExpiry();
      if (freedDuringCommit) this.pumpWaiters();
    }
  }

  release(id: string, token: string): { released: boolean } {
    const released = this.db.transaction(() => {
      const lease = this.db.get<LeaseRow>('SELECT * FROM leases WHERE recording_id = ?', id);
      if (!lease || lease.token !== token) return false;
      // A fenced lease is kept: the recording stays paused until a reader ack
      // frees space, and keeping the row makes that state observable.
      if (lease.yielded === 1) return false;
      this.db.run('DELETE FROM leases WHERE recording_id = ?', id);
      this.emit('release', { recordingId: id, workerId: lease.worker_id });
      return true;
    });
    this.scheduleExpiry();
    if (released) this.pumpWaiters();
    return { released };
  }

  // ----------------------------------------------------------------- timers

  private scheduleExpiry(): void {
    if (this.closed) return;
    if (this.expiryTimer) {
      clearTimeout(this.expiryTimer);
      this.expiryTimer = null;
    }
    const row = this.db.get<{ next: number | null }>(
      'SELECT MIN(expires_at) AS next FROM leases WHERE yielded = 0',
    );
    if (!row || row.next === null) return;
    const delay = Math.max(0, row.next - nowMs());
    this.expiryTimer = setTimeout(() => this.expireLeases(), delay + 2);
    this.expiryTimer.unref();
  }

  /** Reap expired, un-fenced leases. Event driven by the earliest expiry. */
  private expireLeases(): void {
    if (this.closed) return;
    this.probe('renew');
    const now = nowMs();
    const expired = this.db.transaction(() => {
      const rows = this.db.all<LeaseRow>('SELECT * FROM leases WHERE yielded = 0 AND expires_at <= ?', now);
      for (const lease of rows) {
        this.db.run('DELETE FROM leases WHERE recording_id = ?', lease.recording_id);
        this.counters.leaseExpiries++;
        this.pendingFree.set(lease.recording_id, { since: lease.expires_at, reason: 'lease_expiry' });
        this.emit('lease_expired', {
          recordingId: lease.recording_id,
          workerId: lease.worker_id,
          detail: { attempt: lease.attempt, via: 'expiry_timer' },
        });
      }
      return rows.length;
    });
    this.scheduleExpiry();
    if (expired > 0) {
      this.log(`reaped ${expired} expired lease(s)`);
      this.pumpWaiters();
    }
  }

  /**
   * Slow maintenance sweep: one claim scan and one lease scan per interval.
   * `idleProbeIntervalMs` defaults to 5000, so a 4 second window sees at most
   * one sweep, i.e. at most 2 proactive checks — including while every
   * recording is paused and no input arrives. Paused leases are not renewed
   * here (their progress is durable and they are fenced against renewal).
   */
  private idleSweep(): void {
    if (this.closed) return;
    // setInterval() can fire late and then catch up. Never run two sweeps
    // closer than the interval: that would put more than two proactive checks
    // inside a single 4 s idle window.
    const stoppedAt = nowMs();
    if (stoppedAt - this.lastSweepAt < this.options.idleProbeIntervalMs - 500) return;
    this.lastSweepAt = stoppedAt;
    this.probe('claim');
    this.probe('renew');
    const now = nowMs();
    const expired = this.db.transaction(() => {
      const rows = this.db.all<LeaseRow>('SELECT * FROM leases WHERE yielded = 0 AND expires_at <= ?', now);
      for (const lease of rows) {
        this.db.run('DELETE FROM leases WHERE recording_id = ?', lease.recording_id);
        this.counters.leaseExpiries++;
        this.pendingFree.set(lease.recording_id, { since: lease.expires_at, reason: 'lease_expiry' });
        this.emit('lease_expired', {
          recordingId: lease.recording_id,
          workerId: lease.worker_id,
          detail: { attempt: lease.attempt, via: 'sweep' },
        });
      }
      return rows.length;
    });
    this.scheduleExpiry();
    if (expired > 0) this.log(`sweep reaped ${expired} expired lease(s)`);

    // The sweep's claim scan is real (not just a counted probe): it catches a
    // claimable recording whose wake-up was missed (e.g. a claim handler died
    // mid-wake). In the all-paused steady state this finds nothing and
    // therefore does not wake anybody, keeping the budget at 2 checks / sweep.
    const claimable = this.db.get<{ id: string }>(
      `SELECT r.id FROM recordings r
         LEFT JOIN leases l ON l.recording_id = r.id
        WHERE r.state = 'pending'
          AND r.progress < r.total_segments
          AND (r.progress - r.ack_cursor) < r.ack_window
          AND (l.recording_id IS NULL OR l.yielded = 1 OR l.expires_at <= ?)
        LIMIT 1`,
      now,
    );
    if (claimable && this.waiters.length > 0) this.pumpWaiters();
  }

  // ------------------------------------------------------------------ stats

  stats(): QueueStats {
    return {
      startedAt: this.startedAt,
      uptimeMs: nowMs() - this.startedAt,
      options: this.options,
      counters: { ...this.counters },
      probes: {
        total: this.probeClaim + this.probeRenew,
        claim: this.probeClaim,
        renew: this.probeRenew,
        events: this.probeEvents.slice(),
      },
      renewals: {
        total: this.renewalTotal,
        paused: this.renewalPaused,
        events: this.renewalEvents.slice(),
      },
      takeovers: this.takeovers.slice(),
      idleWorkers: this.waiters.map((w) => w.workerId),
      recordings: this.listRecordings(),
      events: this.events.slice(),
    };
  }

  health(): { ok: true; recordings: number; waiters: number } {
    const row = this.db.get<{ n: number }>('SELECT COUNT(*) AS n FROM recordings');
    return { ok: true, recordings: row?.n ?? 0, waiters: this.waiters.length };
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    clearInterval(this.sweepTimer);
    if (this.expiryTimer) clearTimeout(this.expiryTimer);
    this.expiryTimer = null;
    const parked = this.waiters;
    this.waiters = [];
    for (const waiter of parked) {
      if (waiter.timer) clearTimeout(waiter.timer);
      if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener('abort', waiter.onAbort);
      waiter.resolve(null);
    }
    this.db.close();
  }
}
