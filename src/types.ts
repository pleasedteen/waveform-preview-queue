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
  /** Worker ids currently parked waiting for a claim. */
  idleWorkers: string[];
  recordings: RecordingView[];
  events: QueueEvent[];
}
