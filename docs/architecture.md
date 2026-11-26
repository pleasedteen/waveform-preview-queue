# Architecture notes

## Data model (SQLite)

```
recordings(id PK, path, frames, sample_rate, total_segments, segment_ms,
           ack_window, state[pending|completed], progress, ack_cursor,
           attempts, continuation_json, created_at, updated_at, completed_at)

leases(recording_id PK → recordings, worker_id, token, attempt,
       acquired_at, renewed_at, expires_at, yielded[0|1])

segments(recording_id, idx, attempt, start_ms, end_ms, sample_count,
         checksum, payload_json, created_at, PK(recording_id, idx))

handovers(id PK, recording_id → recordings, ack_cursor, progress_at_free,
          freed_at, lease_issued_at, reported_at, first_segment_at,
          first_segment_idx, created_at)

handover_dispatches(id PK, handover_id → handovers, recording_id, worker_id,
                    attempt, lease_issued_at, reported_at, first_segment_at,
                    first_segment_idx, UNIQUE(recording_id, attempt))

meta(key PK, value)
```

`progress` = number of contiguous segments that took effect.
`ack_cursor` = number of contiguous segments the preview reader confirmed.
`continuation_json` = decoder resume state for segment `progress`.

## Why both `progress` and `ack_cursor`

Generation and consumption are decoupled: the producer may run ahead of the
reader by at most `ack_window` segments, which bounds memory/work-in-flight for
a paused preview. Both counters are monotonic and only the queue mutates them.

## Transaction boundaries

| Operation | Writes in one `BEGIN IMMEDIATE` |
| --- | --- |
| submit | recording row with fresh continuation |
| claim | replace/create lease, bump `attempts`, dispatch row when an ack-free handover is open |
| commit | segment row + `progress` + `continuation_json` + (`state`,`completed_at`) + lease expiry/fence + first-segment mark on the dispatch/handover |
| renew | `expires_at` |
| ack | `ack_cursor` + handover record (and prune to the most recent 100) when the ack frees a slot |
| takeover-report | `reported_at` on the dispatch and its handover |

SQLite is opened in WAL mode with a 5 s busy timeout. With `--synchronous FULL`
(the default) a commit is fsynced before the HTTP response, so an acknowledged
segment survives power loss.

## State machine of a recording

```
pending ──commit last segment──▶ completed
   │  ▲
   │  └── ack lowers outstanding below cap ──┐
   └── outstanding == cap ──▶ paused ────────┘
```

A paused recording keeps its fenced lease row (`yielded = 1`) so the state is
observable, but that lease cannot be renewed or committed against and the
recording is immediately claimable by any worker.

## Idle probe accounting

`GET /stats.probes` counts service-internal checks and timestamps each one:

- `claim` — a scan for claimable work (triggered by a worker request, a submit,
  an ack, a release, an expiry, or the sweep);
- `renew` — lease maintenance (expiry timer or sweep) and refused renewals of a
  paused lease.

Arrival-driven probes are not limited by the budget (they are caused by new
input). The budget applies to the steady state with no new input: parked claim
requests do not probe, and the only periodic source is the sweep, which is
≥ 5 s apart and costs exactly 2. Any 4 s window therefore contains at most one
sweep, i.e. at most 2 checks, even when all recordings are paused. A
`sweep` is skipped if `setInterval` fires early or catches up, so bursts cannot
stack.

## Takeover latency measurement

The queue records a `PendingFree` marker with the moment a recording stopped
being held by its previous worker:

- `ack_free`: the ack that dropped the outstanding count below the cap, inside
  the ack transaction;
- `lease_expiry`: the lease's `expires_at` (not the moment we noticed it).

When the next claim succeeds, `latencyMs = claimedAt - freedAt` is stored in
`stats.takeovers`. The 500 ms requirement is about the ack path: the ack
handler wakes parked claimers synchronously before responding, so the next
commit lands in single-digit milliseconds in practice.

## Ack-free handover tracing

Every ack that actually frees a slot for an unfinished recording also opens a
`handovers` row (same transaction as the ack cursor update). The row tracks
four stage times, each null until it happens:

1. `freed_at` — the confirmation that freed the slot (腾位);
2. `lease_issued_at` — the first lease signed for this free event (签发租约);
3. `reported_at` — the first worker takeover report (工作进程回报接手);
4. `first_segment_at` — the resumed run's first segment committed (续跑首段落库).

Each lease signed while the handover is open inserts a `handover_dispatches`
row keyed by `(recording_id, attempt)`. A worker reports its takeover right
after claiming (`POST /recordings/:id/takeover-report`), and the queue matches
the report and the first commit to the dispatch through the lease's attempt
number. A dispatch with `reported_at` still null means the worker vanished
before reporting; one with a report but no `first_segment_at` means it got
stuck afterwards. A re-dispatch (after a disconnect, a lease expiry or a queue
restart — the lease is gone either way) appends another dispatch to the same
handover, so every attempt stays correlated to the same confirmation.

Handover rows survive restarts (only leases are dropped on startup) and are
pruned to the most recent 100; `GET /stats.ackHandovers` exposes them with
per-stage latencies and a derived `state`
(`awaiting_lease` / `awaiting_report` / `awaiting_first_segment` / `landed`).
On startup the in-memory freed-space markers are rebuilt from still-open
handovers, so a takeover right after a restart is also attributed to the
original confirmation in `stats.takeovers`.

## Restart

On startup all lease rows are deleted. Workers with an old token get
`409 lease_lost`, return to claiming, and the next grant carries the persisted
continuation. `progress`, `state`, `completed_at` and `ack_cursor` are read
verbatim from the database.

## Determinism

Fixtures are generated from a fixed LCG seed; the decoder is a pure function of
(WAV, segment index, continuation). Tests compare full multi-worker/restart
runs against a single-pass decode digest-by-digest.
