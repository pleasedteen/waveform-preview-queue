#!/usr/bin/env node
/** Worker process CLI. */

import { Worker } from '../worker/worker';

function get(argv: string[], name: string, dflt?: string): string | undefined {
  const i = argv.indexOf(`--${name}`);
  if (i >= 0 && i + 1 < argv.length) return argv[i + 1];
  const inline = argv.find((a) => a.startsWith(`--${name}=`));
  return inline ? inline.slice(name.length + 3) : dflt;
}

export function runWorkerCli(argv: string[]): void {
  const queueUrl = get(argv, 'queue', 'http://127.0.0.1:7077')!;
  const workerId = get(argv, 'id', `worker-${process.pid}`)!;
  const decodeMs = Number(get(argv, 'decode-ms', '250'));
  const heartbeatMs = get(argv, 'heartbeat-ms');
  const quiet = argv.includes('--quiet');
  const log = (line: string) =>
    quiet ? undefined : process.stderr.write(`[${workerId} ${new Date().toISOString()}] ${line}\n`);

  const worker = new Worker({
    queueUrl,
    workerId,
    decodeMs: Number.isFinite(decodeMs) ? Math.max(0, decodeMs) : 250,
    heartbeatMs: heartbeatMs ? Number(heartbeatMs) : undefined,
    logger: log,
  });
  worker.start();
  process.stdout.write(`worker ${workerId} watching ${queueUrl}\n`);

  let closing = false;
  const shutdown = async () => {
    if (closing) return;
    closing = true;
    await worker.stop();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

if (require.main === module) {
  try {
    runWorkerCli(process.argv.slice(2));
  } catch (err) {
    process.stderr.write(`worker failed: ${(err as Error)?.stack ?? err}\n`);
    process.exit(1);
  }
}
