#!/usr/bin/env node
/** Submit a recording to a running queue. */

import { ApiClient } from '../api-client';

function get(argv: string[], name: string, dflt?: string): string | undefined {
  const i = argv.indexOf(`--${name}`);
  if (i >= 0 && i + 1 < argv.length) return argv[i + 1];
  const inline = argv.find((a) => a.startsWith(`--${name}=`));
  return inline ? inline.slice(name.length + 3) : dflt;
}

export async function runSubmitCli(argv: string[]): Promise<void> {
  const wav = get(argv, 'wav');
  if (!wav) {
    process.stderr.write('usage: submit --wav <path> [--segment-ms 500] [--ack-window 6] [--id NAME] [--queue URL]\n');
    process.exit(2);
  }
  const queueUrl = new URL(get(argv, 'queue', 'http://127.0.0.1:7077')!);
  const client = new ApiClient({
    host: queueUrl.hostname,
    port: queueUrl.port ? Number(queueUrl.port) : 80,
  });
  try {
    const view = await client.post(
      '/recordings',
      {
        id: get(argv, 'id'),
        path: wav,
        segmentMs: Number(get(argv, 'segment-ms', '500')),
        ackWindow: get(argv, 'ack-window') ? Number(get(argv, 'ack-window')) : undefined,
      },
    );
    process.stdout.write(JSON.stringify(view, null, 2) + '\n');
  } finally {
    client.close();
  }
}

if (require.main === module) {
  runSubmitCli(process.argv.slice(2)).catch((err) => {
    process.stderr.write(`submit failed: ${(err as Error)?.message ?? err}\n`);
    process.exit(1);
  });
}
