/** Minimal HTTP client used by workers, readers and tests. */

import http from 'node:http';

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly body?: unknown,
  ) {
    super(`${status} ${code}: ${message}`);
    this.name = 'ApiError';
  }
}

export interface ApiOptions {
  host: string;
  port: number;
  /** Per-request timeout; long polls pass a longer value explicitly. */
  timeoutMs?: number;
}

export class ApiClient {
  private readonly agent: http.Agent;
  constructor(private readonly opts: ApiOptions) {
    this.agent = new http.Agent({ keepAlive: true, maxSockets: 16 });
  }

  request<T>(
    method: string,
    path: string,
    body?: unknown,
    opts: { timeoutMs?: number; signal?: AbortSignal } = {},
  ): Promise<T> {
    const payload = body === undefined ? null : Buffer.from(JSON.stringify(body));
    return new Promise<T>((resolve, reject) => {
      const req = http.request(
        {
          host: this.opts.host,
          port: this.opts.port,
          method,
          path,
          agent: this.agent,
          headers: payload
            ? { 'content-type': 'application/json', 'content-length': payload.length }
            : { 'content-length': 0 },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on('data', (c: Buffer) => chunks.push(c));
          res.on('end', () => {
            const text = Buffer.concat(chunks).toString('utf8');
            let parsed: unknown = text;
            try {
              parsed = text ? JSON.parse(text) : null;
            } catch {
              /* leave as text */
            }
            const status = res.statusCode ?? 0;
            if (status >= 200 && status < 300) {
              resolve(parsed as T);
            } else {
              const err = (parsed as { error?: { code?: string; message?: string } } | null)?.error;
              reject(new ApiError(status, err?.code ?? 'http_error', err?.message ?? text, parsed));
            }
          });
        },
      );
      const timeout = opts.timeoutMs ?? this.opts.timeoutMs ?? 30_000;
      if (timeout > 0) {
        req.setTimeout(timeout, () => req.destroy(new Error(`request timed out after ${timeout} ms`)));
      }
      if (opts.signal) {
        const onAbort = () => req.destroy(new Error('aborted'));
        opts.signal.addEventListener('abort', onAbort, { once: true });
        req.on('close', () => opts.signal?.removeEventListener('abort', onAbort));
      }
      req.on('error', reject);
      if (payload) req.write(payload);
      req.end();
    });
  }

  get<T>(path: string, opts?: { timeoutMs?: number; signal?: AbortSignal }): Promise<T> {
    return this.request<T>('GET', path, undefined, opts);
  }

  post<T>(path: string, body?: unknown, opts?: { timeoutMs?: number; signal?: AbortSignal }): Promise<T> {
    return this.request<T>('POST', path, body ?? {}, opts);
  }

  close(): void {
    this.agent.destroy();
  }
}
