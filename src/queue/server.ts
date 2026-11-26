/** HTTP surface for the queue. Plain node:http, no framework, no deps. */

import http from 'node:http';
import { URL } from 'node:url';
import { AppError } from '../util/common';
import type { QueueService } from './service';
import type { RecordingView } from '../types';

export interface ServerOptions {
  host?: string;
  port?: number;
  /** Long-poll claims may be held open this long; 0 = until queue closes. */
  maxClaimHoldMs?: number;
}

function send(res: http.ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(text),
    'cache-control': 'no-store',
  });
  res.end(text);
}

async function readJson(req: http.IncomingMessage, limitBytes = 8 * 1024 * 1024): Promise<unknown> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    const buf = chunk as Buffer;
    size += buf.length;
    if (size > limitBytes) throw new AppError(413, 'too_large', 'request body too large');
    chunks.push(buf);
  }
  if (size === 0) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new AppError(400, 'bad_json', 'request body is not valid JSON');
  }
}

function intParam(url: URL, name: string, dflt: number): number {
  const raw = url.searchParams.get(name);
  if (raw === null || raw === '') return dflt;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0) throw new AppError(400, 'bad_request', `${name} must be an integer >= 0`);
  return n;
}

/** Exposed so tests can look up the view type in one place. */
export type { RecordingView };

export function createServer(service: QueueService, opts: ServerOptions = {}): http.Server {
  const maxHold = opts.maxClaimHoldMs ?? 0;

  const server = http.createServer((req, res) => {
    void handle(req, res).catch((err) => {
      if (res.headersSent) {
        res.destroy();
        return;
      }
      if (err instanceof AppError) {
        send(res, err.status, { error: { code: err.code, message: err.message, details: err.details ?? null } });
      } else {
        send(res, 500, { error: { code: 'internal', message: String((err as Error)?.message ?? err) } });
      }
    });
  });

  // Long polls must not be killed by the default request timeout.
  server.requestTimeout = 0;
  server.headersTimeout = 60_000;
  server.keepAliveTimeout = 65_000;

  async function handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const parts = url.pathname.split('/').filter(Boolean);
    const method = req.method ?? 'GET';

    if (method === 'GET' && url.pathname === '/health') {
      send(res, 200, service.health());
      return;
    }
    if (method === 'GET' && url.pathname === '/stats') {
      send(res, 200, service.stats());
      return;
    }

    // POST /recordings
    if (method === 'POST' && parts.length === 1 && parts[0] === 'recordings') {
      const body = (await readJson(req)) as Record<string, unknown>;
      const view = service.submit({
        id: typeof body.id === 'string' ? body.id : undefined,
        path: body.path as string,
        segmentMs: body.segmentMs as number,
        ackWindow: body.ackWindow as number | undefined,
      });
      send(res, 201, view);
      return;
    }

    // GET /recordings
    if (method === 'GET' && parts.length === 1 && parts[0] === 'recordings') {
      send(res, 200, { recordings: service.listRecordings() });
      return;
    }

    // POST /claim
    if (method === 'POST' && parts.length === 1 && parts[0] === 'claim') {
      const body = (await readJson(req)) as Record<string, unknown>;
      const workerId = body.workerId;
      if (typeof workerId !== 'string' || workerId.length === 0) {
        throw new AppError(400, 'bad_request', 'workerId is required');
      }
      let waitMs: number | undefined;
      if (typeof body.waitMs === 'number') {
        if (!Number.isInteger(body.waitMs) || body.waitMs < 0) {
          throw new AppError(400, 'bad_request', 'waitMs must be an integer >= 0');
        }
        waitMs = body.waitMs;
      } else if (maxHold > 0) {
        waitMs = maxHold;
      }
      const abort = new AbortController();
      res.on('close', () => abort.abort());
      const grant = await service.claim(workerId, waitMs, abort.signal);
      if (abort.signal.aborted) return;
      send(res, 200, { grant });
      return;
    }

    if (parts.length >= 2 && parts[0] === 'recordings') {
      const id = decodeURIComponent(parts[1]!);

      if (method === 'GET' && parts.length === 2) {
        send(res, 200, service.getRecording(id));
        return;
      }
      if (method === 'GET' && parts.length === 3 && parts[2] === 'segments') {
        const from = intParam(url, 'from', 0);
        const limit = intParam(url, 'limit', 100);
        send(res, 200, service.getSegments(id, from, limit));
        return;
      }
      if (method === 'POST' && parts.length === 3 && parts[2] === 'segments') {
        const body = (await readJson(req)) as Record<string, unknown>;
        if (typeof body.token !== 'string' || body.token.length === 0) {
          throw new AppError(400, 'bad_request', 'token is required');
        }
        const result = service.commit(id, body.token, body.segment);
        send(res, 200, result);
        return;
      }
      if (method === 'POST' && parts.length === 3 && parts[2] === 'acks') {
        const body = (await readJson(req)) as Record<string, unknown>;
        if (typeof body.cursor !== 'number') throw new AppError(400, 'bad_request', 'cursor is required');
        const result = service.ack(id, body.cursor);
        send(res, 200, result);
        return;
      }
      if (method === 'POST' && parts.length === 3 && parts[2] === 'takeover-report') {
        const body = (await readJson(req)) as Record<string, unknown>;
        if (typeof body.token !== 'string' || body.token.length === 0) {
          throw new AppError(400, 'bad_request', 'token is required');
        }
        send(res, 200, service.reportTakeover(id, body.token));
        return;
      }
      if (method === 'POST' && parts.length === 4 && parts[2] === 'leases' && parts[3] === 'renew') {
        const body = (await readJson(req)) as Record<string, unknown>;
        if (typeof body.token !== 'string') throw new AppError(400, 'bad_request', 'token is required');
        send(res, 200, service.renew(id, body.token));
        return;
      }
      if (method === 'POST' && parts.length === 4 && parts[2] === 'leases' && parts[3] === 'release') {
        const body = (await readJson(req)) as Record<string, unknown>;
        if (typeof body.token !== 'string') throw new AppError(400, 'bad_request', 'token is required');
        send(res, 200, service.release(id, body.token));
        return;
      }
    }

    send(res, 404, { error: { code: 'no_route', message: `${method} ${url.pathname} is not a route` } });
  }

  return server;
}
