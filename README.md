# Waveform Preview Queue

Persistent, lease-based segmented waveform preview generation for long local
recordings. TypeScript + Node.js, SQLite for storage (`node:sqlite`), no runtime
dependencies.

```
npm install          # TypeScript + @types/node only
npm run build
npm run fixtures     # writes fixtures/long-60s.wav and fixtures/long-30s.wav
npm test             # local integration tests, ~25 s
npm run smoke        # live demo with takeover latency + idle-budget report
```

`npm test` builds first, then runs every test with Node's built-in runner. The
whole suite finishes well inside 80 seconds and needs no network access.
See `docs/architecture.md` for the data model, transaction boundaries and the
exact accounting rules behind the guarantees below.

## Components

| Component | Entry point | Role |
| --- | --- | --- |
| Queue service | `src/queue/service.ts` | Persistence, leasing, commits, reader acks, idle probes |
| HTTP server | `src/queue/server.ts` | JSON API used by all processes |
| Worker | `src/worker/worker.ts` | Claims, renews, decodes, commits segmented previews |
| Preview reader | `src/reader/reader.ts` | Sequential reads + confirmations; pause/resume/reconnect |
| Mock decoder | `src/decoder/mock.ts` | Cross-segment stateful decoder (see below) |
| Fixtures | `src/cli/make-fixtures.ts` | Deterministic 30 s / 60 s WAV files |
| Observability demo | `src/cli/smoke.ts` | Takeover latency and idle check counts |
| Test harness | `src/harness.ts` | Programmatic queue+workers+readers for tests |

### Running the pieces

```bash
# Terminal 1 — queue (SQLite file created on demand)
node dist/src/cli/queue.js --db ./queue.sqlite --port 7077 \
  --lease-ms 4000 --ack-window 6 --idle-probe-ms 5000

# Terminal 2 / 3 — local worker processes
node dist/src/cli/worker.js --queue http://127.0.0.1:7077 --id w1 --decode-ms 250
node dist/src/cli/worker.js --queue http://127.0.0.1:7077 --id w2 --decode-ms 250

# Submit a recording, then read it as a preview client
node dist/src/cli/submit.js --queue http://127.0.0.1:7077 \
  --wav fixtures/long-60s.wav --segment-ms 500 --ack-window 6 --id rec-1
node dist/src/cli/reader.js --queue http://127.0.0.1:7077 --recording rec-1
```

Restarting the queue against the same `--db` file preserves progress,
completion and read confirmations; workers reconnect and continue automatically.
Shift-Ctrl-C / SIGTERM shuts any process down cleanly.

## Decoder: state crosses segment boundaries

`decodeSegment(wav, segmentMs, idx, state)` is a pure function of the WAV file
and an explicit continuation state:

```ts
type ContinuationState = {
  version: 1;
  cursor: number;   // absolute sample index where the next segment starts
  checksum: number; // FNV-1a of every sample decoded so far
  carry: number;    // last raw sample of the previous segment (high-pass carry)
};
```

Each segment payload echoes the state it started from and the state it leaves
behind (`startSample`, `startChecksum`, `inputCarry` → `checksum`,
`outputCarry`). The queue stores a segment only if its input state equals the
continuation persisted with the previous segment, so a takeover that resumes
from the wrong place is rejected instead of silently corrupting the preview.
Deterministic output means every handover reproduces byte-identical segments;
the tests compare full runs against a single-pass decode.

## Guarantees and how they are met

**Only the current lease holder can advance a recording.** `commit` is a single
`BEGIN IMMEDIATE` transaction that re-checks the lease token, fence flag and
expiry. A stale token gets `409 lease_lost`, a fenced one `409 lease_fenced`.

**Segment + continuation are atomic, each segment exists once.** The segment row
(`PRIMARY KEY (recording_id, idx)`), `progress + 1`, the new continuation JSON
and the lease renewal are written in one transaction. Out-of-order commits are
refused; an identical retry of an already-applied segment is recognised as
idempotent and not counted twice; a conflicting retry is refused.

**Completion is visible as soon as the last segment takes effect.** The same
transaction flips `state = completed`. Unconfirmed results stay readable: the
segments endpoint serves every segment that took effect regardless of whether
the preview client has confirmed it.

**Unconfirmed-segment cap with a single worker.** A recording is claimable only
while `progress - ack_cursor < ack_window`. Filling the window fences the lease
(`yielded = 1`) and tells the worker to stop; the worker goes back to the claim
loop and picks up any other recording that has space. No other worker is
needed.

**Acknowledge-driven handover within 500 ms.** A reader ack that lowers the
outstanding count below the cap wakes parked long-poll claimers synchronously
inside the ack request. Because the fenced lease is immediately re-claimable,
takeover does not wait for lease expiry. The tests assert ack→next commit under
500 ms while the previous producer is still holding an unexpired lease.

**Idle budget: at most two checks per 4 s.** Idle workers park in event-driven
long polls and perform no database probes. The service itself runs one sweep
every `--idle-probe-ms` (default 5000 ms) doing exactly two probes (claim scan +
lease maintenance). Since sweeps are ≥ 5 s apart, any 4 s window contains at
most one sweep = two checks, even with every recording paused and no input.
Paused (fenced) leases are never renewed in a loop; renewal attempts against
them are refused and counted (`renewals.paused`).

**Restart safety.** On startup the queue drops lease grants from the previous
run (no worker can prove it still holds one). Recordings, `progress`,
`completion`, `completed_at` and `ack_cursor` persist. Workers get
`lease_lost` on their next request and re-claim from the persisted continuation.

## Observing behaviour

`GET /stats` returns counters, probe events with timestamps, renewal events,
recorded takeovers (`reason`, `freedAt`, `claimedAt`, `latencyMs`), parked
workers and per-recording views (progress, ack cursor, unconfirmed count,
paused flag, lease state). `npm run smoke` prints the same numbers as a
readable timeline:

```
=== 1. takeover after a stalled producer (lease expiry) ===
backup worker took over after 1287 ms (recorded latency 4 ms, reason lease_expiry)
all 60 segments match a single-pass decode: true

=== 2. reader ack frees space; takeover must be <= 500 ms ===
after ack at 2: progress resumed after 6 ms (recorded latency 6 ms)

=== 3. idle budget with every incomplete recording paused ===
window 1: service checks = 2 (claim scans 1, renewal checks 1) — limit 2
window 2: service checks = 2 (claim scans 1, renewal checks 1) — limit 2
```

`probes` counts service-internal checks only; parked claimers and worker
heartbeats sitting on their own lease are reported separately.

## HTTP API

| Method & path | Purpose |
| --- | --- |
| `POST /recordings` | `{id?, path, segmentMs, ackWindow?}` → recording view |
| `GET /recordings` | all recordings |
| `GET /recordings/:id` | one recording (progress, ack, lease, paused) |
| `GET /recordings/:id/segments?from=&limit=` | contiguous segments already in effect |
| `POST /claim` | `{workerId, waitMs?}` → long-polled `{grant}` (null on timeout) |
| `POST /recordings/:id/segments` | `{token, segment}` → commit result (`shouldYield`, `completed`) |
| `POST /recordings/:id/leases/renew` | `{token}` → new expiry |
| `POST /recordings/:id/leases/release` | `{token}` → give up the lease |
| `POST /recordings/:id/acks` | `{cursor}` → monotonic read confirmation |
| `GET /stats`, `GET /health` | observability |

Errors are JSON with `error.code` (`lease_lost`, `lease_expired`,
`lease_fenced`, `out_of_order`, `segment_conflict`, `continuation_mismatch`,
`ack_ahead`, …) so workers can distinguish "re-claim" from "give up".

## Tests

`npm test` covers:

- geometry and decoder continuation round-trips (unit);
- takeover after lease expiry, with results compared to a single-pass decode;
- pause at the cap, another recording continuing with one worker, resume by ack;
- ack-to-takeover latency under 500 ms with a still-valid stalled lease;
- idle budget across multiple 4 s windows;
- queue restart preserving progress/completion/acks and resuming work;
- lease fencing: stale token, out-of-order, duplicate, conflicting commits;
- reader pause/resume (cap fills, readable while paused) and reconnect without
  duplicates;
- a worker that survives a queue restart re-claiming from persisted progress;
- completion semantics, readable-but-unconfirmed results, ack monotonicity and
  per-recording caps.

The fixture generator is deterministic, so digest comparisons are stable.
