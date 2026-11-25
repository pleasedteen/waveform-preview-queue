/** SQLite connection + schema for the preview queue. */

import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync, type StatementSync } from 'node:sqlite';

export interface DbOptions {
  /** Path to the SQLite database file, or ':memory:'. */
  path: string;
  /** Durability: FULL fsyncs each commit so a crash cannot lose acknowledged work. */
  synchronous?: 'FULL' | 'NORMAL';
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recordings (
  id TEXT PRIMARY KEY,
  path TEXT NOT NULL,
  frames INTEGER NOT NULL,
  sample_rate INTEGER NOT NULL,
  total_segments INTEGER NOT NULL CHECK (total_segments > 0),
  segment_ms INTEGER NOT NULL CHECK (segment_ms > 0),
  ack_window INTEGER NOT NULL CHECK (ack_window > 0),
  state TEXT NOT NULL CHECK (state IN ('pending','completed')),
  progress INTEGER NOT NULL DEFAULT 0 CHECK (progress >= 0),
  ack_cursor INTEGER NOT NULL DEFAULT 0 CHECK (ack_cursor >= 0),
  attempts INTEGER NOT NULL DEFAULT 0,
  continuation_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  completed_at INTEGER
);

CREATE TABLE IF NOT EXISTS leases (
  recording_id TEXT PRIMARY KEY REFERENCES recordings(id) ON DELETE CASCADE,
  worker_id TEXT NOT NULL,
  token TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  acquired_at INTEGER NOT NULL,
  renewed_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  -- A lease that has reached the unconfirmed-segment cap is "paused": it is
  -- kept for observability but every mutation (renew/commit) is fenced. Any
  -- other worker may claim the recording the moment a reader frees a slot.
  yielded INTEGER NOT NULL DEFAULT 0 CHECK (yielded IN (0,1))
);

CREATE TABLE IF NOT EXISTS segments (
  recording_id TEXT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
  idx INTEGER NOT NULL,
  attempt INTEGER NOT NULL,
  start_ms REAL NOT NULL,
  end_ms REAL NOT NULL,
  sample_count INTEGER NOT NULL,
  checksum TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (recording_id, idx)
);

CREATE INDEX IF NOT EXISTS idx_segments_recording_idx ON segments(recording_id, idx);
CREATE INDEX IF NOT EXISTS idx_leases_expiry ON leases(expires_at);
`;

export class Db {
  readonly raw: DatabaseSync;
  private statements = new Map<string, StatementSync>();

  constructor(opts: DbOptions) {
    if (opts.path !== ':memory:') {
      fs.mkdirSync(path.dirname(path.resolve(opts.path)), { recursive: true });
    }
    this.raw = new DatabaseSync(opts.path, { timeout: 5000 });
    this.raw.exec('PRAGMA journal_mode = WAL;');
    this.raw.exec(`PRAGMA synchronous = ${opts.synchronous ?? 'FULL'};`);
    this.raw.exec('PRAGMA foreign_keys = ON;');
    this.raw.exec('PRAGMA busy_timeout = 5000;');
    this.raw.exec(SCHEMA);
    this.raw
      .prepare('INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value')
      .run('schema_version', '1');
  }

  stmt(sql: string): StatementSync {
    let s = this.statements.get(sql);
    if (!s) {
      s = this.raw.prepare(sql);
      this.statements.set(sql, s);
    }
    return s;
  }

  get<T>(sql: string, ...params: (string | number | null | bigint | Uint8Array)[]): T | undefined {
    return this.stmt(sql).get(...(params as never[])) as T | undefined;
  }

  all<T>(sql: string, ...params: (string | number | null | bigint | Uint8Array)[]): T[] {
    return this.stmt(sql).all(...(params as never[])) as T[];
  }

  run(sql: string, ...params: (string | number | null | bigint | Uint8Array)[]): number {
    const r = this.stmt(sql).run(...(params as never[]));
    return Number(r.changes);
  }

  /**
   * Run fn inside BEGIN IMMEDIATE. Every mutation that couples a segment with
   * progress/continuation/lease state must go through this so the write is
   * atomic and durable.
   */
  transaction<T>(fn: () => T): T {
    this.raw.exec('BEGIN IMMEDIATE;');
    try {
      const result = fn();
      this.raw.exec('COMMIT;');
      return result;
    } catch (err) {
      try {
        this.raw.exec('ROLLBACK;');
      } catch {
        /* already rolled back */
      }
      throw err;
    }
  }

  close(): void {
    this.statements.clear();
    this.raw.close();
  }
}
