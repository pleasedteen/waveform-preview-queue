#!/usr/bin/env node
/** Queue process: serves the HTTP API over SQLite. */

import { QueueService } from '../queue/service';
import { createServer } from '../queue/server';
import { AppError } from '../util/common';

export interface QueueCliOptions {
  db: string;
  port: number;
  host: string;
  leaseMs: number;
  defaultAckWindow: number;
  idleProbeIntervalMs: number;
  synchronous: 'FULL' | 'NORMAL';
  logLevel: 'silent' | 'info' | 'debug';
}

export function parseQueueArgs(argv: string[]): QueueCliOptions {
  const get = (name: string, dflt?: string): string | undefined => {
    const i = argv.indexOf(`--${name}`);
    if (i >= 0 && i + 1 < argv.length) return argv[i + 1];
    const inline = argv.find((a) => a.startsWith(`--${name}=`));
    if (inline) return inline.slice(name.length + 3);
    return dflt;
  };
  const num = (name: string, dflt: number): number => {
    const raw = get(name);
    if (raw === undefined) return dflt;
    const n = Number(raw);
    if (!Number.isFinite(n)) throw new AppError(400, 'bad_request', `--${name} must be a number`);
    return n;
  };
  const logLevel = (get('log-level', 'info') as QueueCliOptions['logLevel']) ?? 'info';
  if (!['silent', 'info', 'debug'].includes(logLevel)) throw new AppError(400, 'bad_request', 'bad --log-level');
  return {
    db: get('db', './queue.sqlite')!,
    host: get('host', '127.0.0.1')!,
    port: num('port', 7077),
    leaseMs: num('lease-ms', 4000),
    defaultAckWindow: num('ack-window', 6),
    idleProbeIntervalMs: num('idle-probe-ms', 5000),
    synchronous: (get('synchronous', 'FULL') as 'FULL' | 'NORMAL') ?? 'FULL',
    logLevel,
  };
}

export async function runQueueCli(argv: string[]): Promise<void> {
  const opts = parseQueueArgs(argv);
  const log = (line: string) => {
    if (opts.logLevel !== 'silent') process.stderr.write(`[queue ${new Date().toISOString()}] ${line}\n`);
  };
  const service = new QueueService({
    dbPath: opts.db,
    leaseMs: opts.leaseMs,
    defaultAckWindow: opts.defaultAckWindow,
    idleProbeIntervalMs: opts.idleProbeIntervalMs,
    synchronous: opts.synchronous,
    logger: opts.logLevel === 'debug' ? log : () => {},
  });
  const server = createServer(service);
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(opts.port, opts.host, () => resolve());
  });
  const addr = server.address();
  const port = typeof addr === 'object' && addr ? addr.port : opts.port;
  process.stdout.write(`queue listening on http://${opts.host}:${port} (db=${opts.db})\n`);
  log(`lease=${opts.leaseMs}ms ackWindow=${opts.defaultAckWindow} idleProbe=${opts.idleProbeIntervalMs}ms`);

  let closing = false;
  const shutdown = async (signal: string) => {
    if (closing) return;
    closing = true;
    log(`shutting down (${signal})`);
    server.close();
    await service.close();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

if (require.main === module) {
  runQueueCli(process.argv.slice(2)).catch((err) => {
    process.stderr.write(`queue failed: ${(err as Error)?.stack ?? err}\n`);
    process.exit(1);
  });
}
