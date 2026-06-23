/**
 * resilient-websocket.ts — Lightweight auto-reconnecting WebSocket.
 *
 * Default reconnect schedule (exponential backoff):
 *   attempt 0 →    0 ms  (immediate)
 *   attempt 1 →  100 ms
 *   attempt 2 →  200 ms
 *   attempt 3 →  400 ms
 *   attempt 4 →  800 ms
 *   attempt 5+ → 1000 ms (capped)
 */

import { WebSocket } from "ws";
import type { ClientOptions, RawData } from "ws";
import type { Agent as HttpAgent } from "node:http";
import type { Agent as HttpsAgent } from "node:https";
import type {
  DataPayload,
  ResilientWebSocketEvents,
  ResilientWebSocketOptions,
  Sendable,
} from "./types.js";

// ---------------------------------------------------------------------------
// Internal emitter helpers
// ---------------------------------------------------------------------------

type AnyFn = (...args: any[]) => void;

interface ListenerEntry {
  fn: AnyFn;
  once: boolean;
}

// ---------------------------------------------------------------------------
// Default backoff
// ---------------------------------------------------------------------------

function defaultBackoff(attempt: number): number {
  if (attempt === 0) return 0;
  return Math.min(100 * 2 ** (attempt - 1), 1_000);
}

// ---------------------------------------------------------------------------
// Frame helpers
// ---------------------------------------------------------------------------

const textDecoder = new TextDecoder();

/**
 * Normalise the various shapes `ws` can deliver a frame in
 * (`Buffer` / `ArrayBuffer` / `Buffer[]` for fragmented messages) into a single
 * `Uint8Array`. Typed-array inputs are returned as a zero-copy view; fragmented
 * inputs are concatenated.
 */
function toUint8Array(raw: RawData): Uint8Array {
  if (raw instanceof ArrayBuffer) return new Uint8Array(raw);
  if (Array.isArray(raw)) {
    let total = 0;
    for (const chunk of raw) total += chunk.byteLength;
    const out = new Uint8Array(total);
    let offset = 0;
    for (const chunk of raw) {
      out.set(
        new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength),
        offset,
      );
      offset += chunk.byteLength;
    }
    return out;
  }
  return new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength);
}

/**
 * Like {@link toUint8Array} but guarantees the consumer owns the bytes.
 *
 * A fragmented (`Buffer[]`) frame is already concatenated into a fresh array by
 * `toUint8Array`, so it is returned as-is — no second copy. A single `Buffer`
 * (default nodebuffer mode) or an `ArrayBuffer` may be a view over `ws`'s reused
 * socket read buffer, so its bytes are copied out before they can be overwritten
 * by a later frame.
 */
function ownFrameBytes(raw: RawData): Uint8Array {
  if (Array.isArray(raw)) return toUint8Array(raw);
  return toUint8Array(raw).slice();
}

function asError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

// ---------------------------------------------------------------------------
// ResilientWebSocket
// ---------------------------------------------------------------------------

export class ResilientWebSocket {
  readonly url: string;

  // ── Options (immutable after construction) ──────────────────────────────

  private readonly protocols: string | string[] | undefined;
  private readonly maxRetries: number;
  private readonly backoffFn: (attempt: number) => number;
  private readonly bufferWhileReconnecting: boolean;
  private readonly handshakeTimeoutMs: number;
  private readonly agentOption:
    | HttpAgent
    | HttpsAgent
    | ((
        url: string,
      ) => HttpAgent | HttpsAgent | Promise<HttpAgent | HttpsAgent>)
    | undefined;

  // ── Socket state ─────────────────────────────────────────────────────────

  private ws: WebSocket | null = null;
  private _openGeneration = 0;
  private attempt = 0;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private _closing = false;
  private _exhausted = false;
  private _everConnected = false;
  private _pendingWs: WebSocket | null = null;

  // ── Send buffer ──────────────────────────────────────────────────────────

  private _sendBuffer: Sendable[] = [];

  // ── Event emitter ────────────────────────────────────────────────────────
  // Stored as ListenerEntry[] (not Set) so that:
  //   1. `once` state travels with the entry — no wrapper closure needed.
  //   2. `off(originalListener)` always works, even for once-registered handlers.
  //   3. Snapshot-on-emit prevents ordering bugs when listeners add/remove
  //      other listeners during dispatch.

  private readonly _listeners = new Map<string, ListenerEntry[]>();

  // ── Constructor ──────────────────────────────────────────────────────────

  constructor(url: string, options: ResilientWebSocketOptions = {}) {
    this.url = url;
    // Normalise: treat empty array the same as omitted to avoid passing an
    // empty protocols list to the WebSocket constructor (some runtimes error).
    this.protocols = options.protocols?.length ? options.protocols : undefined;
    this.maxRetries = options.maxRetries ?? Infinity;
    this.backoffFn = options.backoff ?? defaultBackoff;
    this.bufferWhileReconnecting = options.bufferWhileReconnecting ?? true;
    this.handshakeTimeoutMs = options.handshakeTimeoutMs ?? 10_000;
    // Stored (not pre-resolved) so a function factory is invoked per connection
    // attempt — a transient failure can recover, and the URL is re-passed each
    // time. Resolution errors are caught at the call site (see `_resolveAgent`).
    this.agentOption = options.agent;
  }

  // ── Static factory ───────────────────────────────────────────────────────

  /**
   * Create a `ResilientWebSocket` instance and start connecting immediately.
   *
   * @example
   * const ws = ResilientWebSocket.connect("wss://echo.example.com");
   * ws.on("message", (e) => console.log(e));
   */
  static connect(
    url: string,
    options?: ResilientWebSocketOptions,
  ): ResilientWebSocket {
    const instance = new ResilientWebSocket(url, options);
    instance.connect();
    return instance;
  }

  // ── Lifecycle ────────────────────────────────────────────────────────────

  /**
   * Open the connection.
   *
   * - No-op if the socket is already CONNECTING or OPEN.
   * - Cancels any pending backoff timer and reconnects immediately if called
   *   while waiting to reconnect.
   * - Safe to call after `close()` to restart the connection.
   */
  connect(): this {
    if (this.ws !== null && this.ws.readyState < WebSocket.CLOSING) return this;
    this._clearTimer();
    this._closing = false;
    this._exhausted = false;
    this._open();
    return this;
  }

  /**
   * Force-close the current socket and reconnect immediately, regardless of
   * the current state (CONNECTING, OPEN, or waiting for backoff).
   *
   * Unlike `close()` + `connect()`, this always tears down the existing socket
   * and opens a fresh one in one atomic step.
   *
   * If a `softRestart()` is in progress, the pending socket is cancelled and
   * this force restart takes over.
   */
  restart(): this {
    this._closing = false;
    this._exhausted = false;
    this._clearTimer();
    this._cancelPending();
    // _detach() already terminates the current socket; bump the generation so
    // any in-flight agent resolution from a prior _open() is discarded.
    this._detach();
    this._openGeneration++;
    this._open();
    return this;
  }

  /**
   * Gracefully restart: opens a new connection first, then silently closes the
   * current one the moment the new socket reaches OPEN — a seamless switchover
   * with no visible gap to the application.
   *
   * - No-op if a soft restart is already in progress.
   * - No-op if the socket has been explicitly `close()`d.
   * - If the new socket fails before opening, the current socket remains active
   *   and no events are emitted for the failed attempt.
   * - Calling `restart()` while a soft restart is pending cancels the pending
   *   socket and performs an immediate force restart instead.
   */
  softRestart(): this {
    if (this._pendingWs || this._closing) return this;
    this._exhausted = false;
    // Claim this soft restart synchronously. Agent resolution is async and
    // `_pendingWs` is not set until it completes, so without a token a second
    // call in the same tick would slip past the guard above and open a second,
    // leaked socket. A later softRestart()/restart()/reconnect bumps the
    // generation too, which cancels this now-stale in-flight soft open.
    const gen = ++this._openGeneration;

    const doSoftOpen = (agent?: HttpAgent | HttpsAgent) => {
      if (this._closing || this._openGeneration !== gen) return;
      const wsOpts: ClientOptions = {
        handshakeTimeout: this.handshakeTimeoutMs,
      };
      if (agent) wsOpts.agent = agent;
      const pendingWs = new WebSocket(this.url, this.protocols, wsOpts);
      this._pendingWs = pendingWs;

      pendingWs.on("open", () => {
        // Bail if cancelled by restart()/close() (_pendingWs cleared) or if the
        // socket was closed while this pending handshake was still in flight —
        // otherwise a soft restart that opens after close() would revive a live
        // connection the caller believes is shut down.
        if (this._pendingWs !== pendingWs || this._closing) return;
        this._pendingWs = null;

        this._detach();
        this.ws?.close(1000, "Soft restart");
        this.ws = pendingWs;
        this._attachActiveHandlers(pendingWs);

        this.attempt = 0;
        this._everConnected = true;
        this._flushBuffer(pendingWs);
      });

      // Silent abort: if the new socket fails before opening, keep the old one.
      pendingWs.on("close", () => {
        if (this._pendingWs === pendingWs) this._pendingWs = null;
      });
      // Swallow errors from the pending socket so they don't surface as
      // unhandled — the subsequent close will clear _pendingWs.
      pendingWs.on("error", () => {});
    };

    this._resolveAgent().then(
      (agent) => doSoftOpen(agent),
      () => {
        // Agent resolution failed: keep the current socket and abort the soft
        // restart silently, consistent with "new socket failed before opening".
      },
    );

    return this;
  }

  /**
   * Gracefully close the socket.
   *
   * Cancels any pending reconnect timer. The send buffer is **preserved** so
   * that a subsequent `connect()` will still flush it — call `clearBuffer()`
   * first if you want to discard queued messages.
   *
   * No reconnect will be attempted until `connect()` is called again.
   */
  close(code = 1000, reason = "Normal closure"): this {
    this._closing = true;
    this._clearTimer();
    // Cancel any in-flight soft restart: without this, a pending socket that
    // finishes its handshake after close() would resurrect a live connection.
    this._cancelPending();
    this.ws?.close(code, reason);
    return this;
  }

  // ── Sending ──────────────────────────────────────────────────────────────

  /**
   * Send data.
   *
   * | State                   | bufferWhileReconnecting | Behaviour        |
   * |-------------------------|-------------------------|------------------|
   * | Exhausted               | any                     | **Throw**        |
   * | OPEN                    | any                     | Send immediately |
   * | Before first connection | any                     | Buffer           |
   * | Reconnecting            | `true` (default)        | Buffer           |
   * | Reconnecting            | `false`                 | **Throw**        |
   */
  send(data: Sendable): void {
    if (this._exhausted) {
      throw new Error(
        "ResilientWebSocket: cannot send — connection exhausted (maxRetries reached)",
      );
    }

    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      return;
    }

    if (!this._everConnected || this.bufferWhileReconnecting) {
      this._sendBuffer.push(data);
      this._emit("buffered", data, this._sendBuffer.length);
      return;
    }

    throw new Error(
      `ResilientWebSocket: cannot send — socket is ${this._stateName()} ` +
        `and bufferWhileReconnecting is disabled`,
    );
  }

  // ── Buffer management ────────────────────────────────────────────────────

  /** Number of messages currently waiting in the send buffer. */
  get bufferSize(): number {
    return this._sendBuffer.length;
  }

  /**
   * Discard all queued messages.
   * @returns The number of messages dropped.
   */
  clearBuffer(): number {
    const n = this._sendBuffer.length;
    this._sendBuffer = [];
    return n;
  }

  // ── State ────────────────────────────────────────────────────────────────

  /**
   * The underlying socket's `readyState`.
   * Returns `WebSocket.CLOSED` when no socket has been created yet.
   */
  get readyState(): number {
    return this.ws?.readyState ?? WebSocket.CLOSED;
  }

  get isOpen(): boolean {
    return this.readyState === WebSocket.OPEN;
  }

  // ── Event emitter ────────────────────────────────────────────────────────

  /**
   * Register a persistent listener for one event or a list of events.
   *
   * @example — single event
   * ws.on("open", () => { ... });
   *
   * @example — multiple events sharing one handler
   * ws.on(["close", "exhausted"], () => cleanup());
   */
  on<K extends keyof ResilientWebSocketEvents>(
    event: K | K[],
    listener: ResilientWebSocketEvents[K],
  ): this {
    for (const e of ([] as K[]).concat(event)) {
      this._entries(e).push({ fn: listener as AnyFn, once: false });
    }
    return this;
  }

  /**
   * Register a one-shot listener for one event or a list of events.
   * Each event fires the listener at most once independently.
   * Passing the original `listener` reference to `off()` cancels it before
   * it fires.
   *
   * @example
   * ws.once(["open", "exhausted"], () => console.log("settled"));
   */
  once<K extends keyof ResilientWebSocketEvents>(
    event: K | K[],
    listener: ResilientWebSocketEvents[K],
  ): this {
    for (const e of ([] as K[]).concat(event)) {
      this._entries(e).push({ fn: listener as AnyFn, once: true });
    }
    return this;
  }

  /**
   * Remove a listener from one event or a list of events.
   *
   * @example
   * ws.off(["close", "exhausted"], handler);
   */
  off<K extends keyof ResilientWebSocketEvents>(
    event: K | K[],
    listener: ResilientWebSocketEvents[K],
  ): this {
    for (const e of ([] as K[]).concat(event)) {
      const entries = this._listeners.get(e);
      if (!entries) continue;
      const idx = entries.findIndex(
        (entry) => entry.fn === (listener as AnyFn),
      );
      if (idx !== -1) entries.splice(idx, 1);
    }
    return this;
  }

  // ── Private: emitter ─────────────────────────────────────────────────────

  private _entries(event: string): ListenerEntry[] {
    let arr = this._listeners.get(event);
    if (!arr) {
      arr = [];
      this._listeners.set(event, arr);
    }
    return arr;
  }

  private _emit<K extends keyof ResilientWebSocketEvents>(
    event: K,
    ...args: Parameters<ResilientWebSocketEvents[K]>
  ): void {
    const entries = this._listeners.get(event);
    if (!entries || entries.length === 0) return;

    // Snapshot before iteration so that listeners adding/removing listeners
    // during dispatch do not affect the current emit cycle.
    for (const entry of [...entries]) {
      if (entry.once) {
        const idx = entries.indexOf(entry);
        if (idx !== -1) entries.splice(idx, 1);
      }
      entry.fn(...(args as unknown[]));
    }
  }

  // ── Private: socket management ───────────────────────────────────────────

  private _open(): void {
    if (this._closing) return;

    // A soft restart may have already swapped in a healthy socket while this
    // reconnect sat in the backoff queue — bail rather than tear it down to
    // open a duplicate. (restart() force-reopens by detaching first, so its
    // socket is CLOSING/CLOSED here and this guard does not block it.)
    if (this.ws !== null && this.ws.readyState < WebSocket.CLOSING) return;

    // The active socket is down, so a soft restart's seamless-switchover
    // premise no longer holds: cancel any still-pending soft socket and
    // reconnect cleanly instead of racing two sockets to become active.
    this._cancelPending();

    // Detach stale handlers from the previous socket to prevent a CLOSING
    // socket's onclose from firing and triggering a duplicate reconnect cycle.
    this._detach();

    if (this.agentOption === undefined) {
      this._doOpen();
      return;
    }

    const gen = ++this._openGeneration;
    this._resolveAgent().then(
      (agent) => {
        if (this._closing || this._openGeneration !== gen) return;
        this._doOpen(agent);
      },
      (err) => {
        // Agent resolution failed (factory threw/rejected). Surface it and let
        // the backoff loop retry instead of leaving the socket silently dead.
        if (this._closing || this._openGeneration !== gen) return;
        this._emit("error", asError(err));
        this._scheduleReconnect();
      },
    );
  }

  /**
   * Resolve the configured agent for a single connection attempt. A function
   * factory is invoked fresh each call (deferred so a synchronous throw becomes
   * a rejection rather than escaping the caller). Resolves to `undefined` when
   * no agent is configured.
   */
  private _resolveAgent(): Promise<HttpAgent | HttpsAgent | undefined> {
    const a = this.agentOption;
    if (a === undefined) return Promise.resolve(undefined);
    if (typeof a === "function") return Promise.resolve().then(() => a(this.url));
    return Promise.resolve(a);
  }

  private _doOpen(agent?: HttpAgent | HttpsAgent): void {
    const wsOpts: ClientOptions = {
      handshakeTimeout: this.handshakeTimeoutMs,
    };
    if (agent) wsOpts.agent = agent;
    const ws = new WebSocket(this.url, this.protocols, wsOpts);
    this.ws = ws;

    ws.on("open", () => {
      // Generation guard: if this.ws has been replaced (e.g. by softRestart or
      // a prior _detach + reconnect), ignore the stale open event so we don't
      // operate on the wrong socket.
      if (this.ws !== ws) return;
      this.attempt = 0;
      this._everConnected = true;
      // Flush buffered sends *before* emitting "open" so that the remote
      // receives them in the order send() was called, and any sends made
      // inside the "open" handler are appended after the backlog.
      this._flushBuffer(ws);
      this._emit("open");
    });

    this._attachActiveHandlers(ws);
  }

  private _parseData(raw: RawData, isBinary: boolean): DataPayload {
    // Trust the frame's opcode (ws passes `isBinary`) rather than the JS type
    // of `raw`: with the default nodebuffer mode every frame — text or binary —
    // arrives as a Buffer, so type sniffing alone cannot tell them apart.
    if (isBinary) {
      // ownFrameBytes copies only when the frame is a view over `ws`'s reused
      // read buffer, so the consumer always owns the bytes without copying a
      // freshly-concatenated fragmented frame twice. The `message` event still
      // exposes the raw frame for callers that explicitly want zero-copy.
      return { type: "binary", data: ownFrameBytes(raw) };
    }

    // Text frame — decode the bytes, then attempt a JSON parse.
    const text = textDecoder.decode(toUint8Array(raw));
    try {
      return { type: "json", data: JSON.parse(text) };
    } catch {
      return { type: "text", data: text };
    }
  }

  private _attachActiveHandlers(ws: WebSocket): void {
    ws.on("message", (data: RawData, isBinary: boolean) => {
      if (this.ws !== ws) return;
      this._emit("message", data);
      this._emit("data", this._parseData(data, isBinary));
    });
    ws.on("error", (error: Error) => {
      if (this.ws !== ws) return;
      this._emit("error", error);
    });
    ws.on("close", (status: number) => {
      if (this.ws !== ws) return;
      this._emit("close", status);
      if (!this._closing) this._scheduleReconnect();
    });
  }

  private _cancelPending(): void {
    if (!this._pendingWs) return;
    const ws = this._pendingWs;
    this._pendingWs = null;
    // Keep softRestart's listeners attached:
    //   - open/close handlers are guarded by `_pendingWs !== pendingWs` (now
    //     null), so they're no-ops.
    //   - the error handler must remain because close() on a CONNECTING socket
    //     synchronously emits 'error' via abortHandshake; without a listener,
    //     Node's EventEmitter rethrows it as an uncaught error.
    try {
      ws.close(1000, "Cancelled");
    } catch {
      // Defensive: some ws@8 states still throw synchronously.
    }
  }

  private _detach(): void {
    const ws = this.ws;
    if (!ws) return;
    // Drop our handlers so a stale socket cannot drive a duplicate reconnect
    // cycle. But the socket being detached may still be CONNECTING (its
    // internal handshake-timeout timer still armed) or a zombie OPEN one, and
    // ws emits the resulting 'error' asynchronously (process.nextTick). With
    // no 'error' listener Node's EventEmitter rethrows it as an uncaught
    // exception, and crucially ws's emitErrorAndClose() throws at emit('error')
    // *before* reaching emitClose() — so the socket never finishes closing and
    // its ws/TLS/receiver buffers leak. Keep a no-op 'error' sink so the error
    // is absorbed and emitClose() can run, then force-destroy the socket so it
    // is torn down immediately instead of lingering until the OS TCP timeout.
    ws.removeAllListeners();
    ws.on("error", () => {});
    if (ws.readyState !== WebSocket.CLOSED) {
      try {
        ws.terminate();
      } catch {
        // Defensive: some ws@8 states still throw synchronously.
      }
    }
  }

  private _flushBuffer(ws: WebSocket): void {
    if (this._sendBuffer.length === 0) return;
    // Swap out the buffer before draining so that any send() called
    // synchronously inside an "open" listener appends to a fresh queue.
    const queue = this._sendBuffer;
    this._sendBuffer = [];
    for (let i = 0; i < queue.length; i++) {
      if (ws.readyState !== WebSocket.OPEN) {
        // Socket is no longer OPEN (race with close). Put the un-sent tail
        // back at the head of the buffer so a future open flushes it.
        this._sendBuffer = queue.slice(i).concat(this._sendBuffer);
        return;
      }
      ws.send(queue[i]!);
    }
  }

  private _scheduleReconnect(): void {
    if (this.attempt >= this.maxRetries) {
      this._exhausted = true;
      this._emit("exhausted");
      return;
    }

    const delayMs = this.backoffFn(this.attempt);
    this._emit("reconnect", { attempt: this.attempt, delayMs });
    this.attempt++;

    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this._open();
    }, delayMs);
  }

  private _clearTimer(): void {
    if (this.retryTimer !== null) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
  }

  private _stateName(): string {
    return (
      (["CONNECTING", "OPEN", "CLOSING", "CLOSED"] as const)[this.readyState] ??
      "UNKNOWN"
    );
  }
}
