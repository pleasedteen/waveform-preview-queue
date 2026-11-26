/** Shared domain types for the preview queue. */

import type { ContinuationState } from './protocol';

export type RecordingState = 'pending' | 'completed';

export interface RecordingRow {
  id: string;
  path: string;
  frames: number;
  sample_rate: number;
  total_segments: number;
  segment_ms: number;
  ack_window: number;
  state: RecordingState;
  /** Number of contiguous segments that have taken effect (0..total). */
  progress: number;
  /** Number of contiguous segments the preview reader has confirmed. */
  ack_cursor: number;
  /** Monotonic claim counter; keeps lease attempts unique across restarts. */
  attempts: number;
  continuation_json: string;
  created_at: number;
  updated_at: number;
  completed_at: number | null;
}

export interface LeaseRow {
  recording_id: string;
  worker_id: string;
  token: string;
  attempt: number;
  acquired_at: number;
  renewed_at: number;
  expires_at: number;
  /** 1 = paused at the unconfirmed cap; fenced against renew/commit. */
  yielded: 0 | 1;
}

export interface SegmentRow {
  recording_id: string;
  idx: number;
  attempt: number;
  start_ms: number;
  end_ms: number;
  sample_count: number;
  checksum: string;
  payload_json: string;
  created_at: number;
}

export interface LeaseView {
  workerId: string;
  attempt: number;
  acquiredAt: number;
  renewedAt: number;
  expiresAt: number;
  /** True when the unconfirmed cap was reached and the holder was fenced. */
  yielded: boolean;
  /** True when the lease is past its expiry. */
  expired: boolean;
}

export interface RecordingView {
  id: string;
  state: RecordingState;
  progress: number;
  totalSegments: number;
  segmentMs: number;
  ackWindow: number;
  ackCursor: number;
  unconfirmed: number;
  /** The recording cannot be claimed until the reader confirms more segments. */
  paused: boolean;
  completedAt: number | null;
  createdAt: number;
  updatedAt: number;
  frames: number;
  sampleRate: number;
  path: string;
  lease: LeaseView | null;
}

export interface ClaimGrant {
  token: string;
  attempt: number;
  leaseMs: number;
  expiresAt: number;
  recording: RecordingView & { continuation: ContinuationState };
}

export interface CommitResult {
  recordingId: string;
  idx: number;
  progress: number;
  totalSegments: number;
  completed: boolean;
  ackCursor: number;
  unconfirmed: number;
  ackWindow: number;
  /** True when the unconfirmed cap is reached; the lease is fenced and the worker must stop. */
  shouldYield: boolean;
  leaseExpiresAt: number | null;
  /** True when an identical retried commit was recognised and not applied twice. */
  idempotent: boolean;
}

export interface AckResult {
  recordingId: string;
  /** Monotonic cursor after the ack (never moves backwards). */
  ackCursor: number;
  requestedCursor: number;
  progress: number;
  unconfirmed: number;
  ackWindow: number;
  /** True when this ack turned a paused recording back into a runnable one. */
  freedSpace: boolean;
}

export interface QueueOptions {
  leaseMs: number;
  defaultAckWindow: number;
  /** Interval of the single internal maintenance sweep (claim + lease scan). */
  idleProbeIntervalMs: number;
}

export interface QueueEvent {
  seq: number;
  at: number;
  kind: string;
  recordingId?: string;
  workerId?: string;
  detail?: Record<string, unknown>;
}

export interface ProbeEvent {
  seq: number;
  at: number;
  /** 'claim' = looking for claimable work, 'renew' = lease maintenance check. */
  reason: 'claim' | 'renew';
  granted: number;
}

export interface RenewalEvent {
  at: number;
  recordingId: string;
  workerId: string;
  outcome: 'renewed' | 'lost' | 'fenced' | 'expired';
}

export interface TakeoverRecord {
  recordingId: string;
  workerId: string;
  /** Why the recording became available again. */
  reason: 'ack_free' | 'lease_expiry';
  /** When the previous holder stopped being allowed to advance the recording. */
  freedAt: number;
  claimedAt: number;
  latencyMs: number;
}

/** Persisted `handovers` row: one per reader ack that actually freed a slot. */
export interface HandoverRow {
  id: string;
  recording_id: string;
  /** The reader confirmation that freed the slot. */
  ack_cursor: number;
  /** Recording progress at free time = the resume point of the next run. */
  progress_at_free: number;
  freed_at: number;
  lease_issued_at: number | null;
  reported_at: number | null;
  first_segment_at: number | null;
  first_segment_idx: number | null;
  created_at: number;
}

/** Persisted `handover_dispatches` row: one per lease issued for a handover. */
export interface HandoverDispatchRow {
  id: string;
  handover_id: string;
  recording_id: string;
  worker_id: string;
  /** Lease attempt; correlates the worker's report and its first commit. */
  attempt: number;
  lease_issued_at: number;
  reported_at: number | null;
  first_segment_at: number | null;
  first_segment_idx: number | null;
}

/**
 * Stage of a single dispatch. `awaiting_report` = lease issued but the worker
 * never reported the takeover (e.g. it disconnected); `awaiting_first_segment`
 * = the worker reported but no segment landed afterwards (stuck mid-run);
 * `superseded` = a later dispatch was issued for the same handover.
 */
export type HandoverDispatchPhase = 'awaiting_report' | 'awaiting_first_segment' | 'landed' | 'superseded';

export interface HandoverDispatchView {
  dispatchId: string;
  workerId: string;
  attempt: number;
  /** When the queue signed this lease. */
  leaseIssuedAt: number;
  /** When the worker process reported the takeover; null = no report received. */
  reportedAt: number | null;
  /** When this dispatch's first resumed segment landed; null = none landed. */
  firstSegmentAt: number | null;
  firstSegmentIdx: number | null;
  phase: HandoverDispatchPhase;
}

/** Overall stage of one ack-free handover (the furthest stage reached). */
export type HandoverState = 'awaiting_lease' | 'awaiting_report' | 'awaiting_first_segment' | 'landed';

/**
 * Tracing record for one reader confirmation that actually freed a slot for an
 * unfinished recording. Stages that have not happened yet are null, so a
 * handover stuck at any point is distinguishable from a completed one.
 * Persisted in SQLite, so the same confirmation stays traceable across queue
 * restarts; re-dispatches append to `dispatches` under the same handoverId.
 */
export interface AckHandoverView {
  handoverId: string;
  recordingId: string;
  ackCursor: number;
  progressAtFree: number;
  /** 腾位: when the ack dropped the outstanding count below the cap. */
  freedAt: number;
  /** 签发租约: first lease issued for this free event (null while unclaimed). */
  leaseIssuedAt: number | null;
  /** 工作进程回报接手: first takeover report across dispatches (null if none). */
  reportedAt: number | null;
  /** 续跑首段落库: first segment of the resumed run committed (null if none). */
  firstSegmentAt: number | null;
  firstSegmentIdx: number | null;
  leaseLatencyMs: number | null;
  reportLatencyMs: number | null;
  firstSegmentLatencyMs: number | null;
  state: HandoverState;
  /** Every lease issued for this free event, oldest first. */
  dispatches: HandoverDispatchView[];
}

export interface TakeoverReportResult {
  /** True when the report was attached to an open ack-free handover. */
  recorded: boolean;
  handoverId: string | null;
}

export interface QueueStats {
  startedAt: number;
  uptimeMs: number;
  options: QueueOptions;
  counters: {
    submits: number;
    grants: number;
    renewals: number;
    commits: number;
    duplicateCommits: number;
    acks: number;
    completed: number;
    yields: number;
    leaseExpiries: number;
    takeoversFenced: number;
    takeoversExpired: number;
    lostLeaseRejections: number;
  };
  /** Service-internal probes, i.e. liveness/claim checks performed by the service itself. */
  probes: {
    total: number;
    claim: number;
    renew: number;
    events: ProbeEvent[];
  };
  /** Lease renewals actually recorded (worker heartbeats and service renewals). */
  renewals: {
    total: number;
    /** Renewals attempted against already-paused (fenced) leases. */
    paused: number;
    events: RenewalEvent[];
  };
  takeovers: TakeoverRecord[];
  /**
   * The most recent 100 reader confirmations that actually freed a slot for
   * an unfinished recording, oldest first. Persisted across queue restarts.
   */
  ackHandovers: AckHandoverView[];
  /** Worker ids currently parked waiting for a claim. */
  idleWorkers: string[];
  recordings: RecordingView[];
  events: QueueEvent[];
}
