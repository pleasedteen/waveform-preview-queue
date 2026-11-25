#!/usr/bin/env node
/** Preview reader CLI: prints segment peaks and confirms them as it goes. */

import { Reader } from '../reader/reader';

function get(argv: string[], name: string, dflt?: string): string | undefined {
  const i = argv.indexOf(`--${name}`);
  if (i >= 0 && i + 1 < argv.length) return argv[i + 1];
  const inline = argv.find((a) => a.startsWith(`--${name}=`));
  return inline ? inline.slice(name.length + 3) : dflt;
}

export async function runReaderCli(argv: string[]): Promise<void> {
  const queueUrl = get(argv, 'queue', 'http://127.0.0.1:7077')!;
  const recordingId = get(argv, 'recording');
  if (!recordingId) {
    process.stderr.write('usage: reader --recording <id> [--queue URL] [--process-ms N]\n');
    process.exit(2);
  }
  const processMs = Number(get(argv, 'process-ms', '0'));
  const reader = new Reader({
    queueUrl,
    recordingId,
    processMs: Number.isFinite(processMs) ? Math.max(0, processMs) : 0,
    logger: (line) => process.stderr.write(`[reader ${new Date().toISOString()}] ${line}\n`),
    onSegment: (info) => {
      process.stdout.write(`segment ${info.idx} (attempt ${info.attempt})\n`);
    },
  });
  reader.start();
  process.on('SIGTERM', () => void reader.stop().then(() => process.exit(0)));
  process.on('SIGINT', () => void reader.stop().then(() => process.exit(0)));
  await new Promise<void>((resolve) => {
    const timer = setInterval(() => {
      if (!reader.getStatus().running) {
        clearInterval(timer);
        resolve();
      }
    }, 100);
  });
}

if (require.main === module) {
  runReaderCli(process.argv.slice(2)).catch((err) => {
    process.stderr.write(`reader failed: ${(err as Error)?.message ?? err}\n`);
    process.exit(1);
  });
}
