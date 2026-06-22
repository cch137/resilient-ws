/**
 * Public types for {@link ResilientWebSocket}.
 */

import type { RawData } from "ws";
import type { Agent as HttpAgent } from "node:http";
import type { Agent as HttpsAgent } from "node:https";

export type Sendable = string | ArrayBufferLike | Blob | ArrayBufferView;

export interface ResilientWebSocketOptions {
  /**
   * WebSocket sub-protocols forwarded to the underlying socket.
   * Omit or leave empty for none.
   */
  protocols?: string | string[];

  /**
   * Maximum total reconnect attempts before giving up.
   * @default Infinity
   */
  maxRetries?: number;

  /**
   * Override the default backoff formula.
   * Receives the zero-based attempt index; returns a delay in milliseconds.
   */
  backoff?: (attempt: number) => number;

  /**
   * Buffer outgoing `send()` calls while the socket is disconnected **after**
   * the first successful connection, and flush them in order once reconnected.
   *
   * Before the very first successful connection, messages are **always**
   * buffered regardless of this setting.
   *
   * Set to `false` to throw immediately on any `send()` call when not OPEN
   * (after the first connection).
   *
   * @default true
   */
  bufferWhileReconnecting?: boolean;

  /**
   * HTTP/HTTPS proxy agent (or an async factory receiving the target URL).
   * Lets the socket route through a proxy without coupling this library to any
   * particular proxy implementation.
   */
  agent?:
    | HttpAgent
    | HttpsAgent
    | ((
        url: string,
      ) => HttpAgent | HttpsAgent | Promise<HttpAgent | HttpsAgent>);

  /**
   * Maximum time (ms) to wait for the WebSocket opening handshake to complete
   * before the underlying socket aborts with an error. Without this, a network
   * limbo (e.g. SYN swallowed by a firewall) can leave the pending socket
   * stuck in CONNECTING forever, blocking softRestart from making progress.
   *
   * @default 10_000
   */
  handshakeTimeoutMs?: number;
}

export interface ReconnectInfo {
  /** Zero-based reconnect attempt index. */
  attempt: number;
  /** Milliseconds until the next socket open attempt. */
  delayMs: number;
}

/**
 * Parsed payload delivered by the `data` event.
 *
 * | `type`   | `data`      | When                                        |
 * |----------|-------------|---------------------------------------------|
 * | `"json"` | `unknown`   | Text frame whose content is valid JSON      |
 * | `"text"` | `string`    | Text frame that is not valid JSON           |
 * | `"binary"` | `Uint8Array` | Binary frame (`binaryType = "arraybuffer"`) |
 * | `"blob"` | `Blob`      | Binary frame (`binaryType = "blob"`)        |
 */
export type DataPayload =
  | { type: "json"; data: unknown }
  | { type: "text"; data: string }
  | { type: "binary"; data: Uint8Array }
  | { type: "blob"; data: Blob };

export interface ResilientWebSocketEvents {
  /** The underlying socket opened successfully. */
  open: () => void;
  /** A message was received. */
  message: (event: RawData) => void;
  /** The underlying socket emitted an error. Always followed by `close`. */
  error: (event: Error) => void;
  /** The underlying socket closed (whether or not a reconnect will follow). */
  close: (status: number) => void;
  /** A reconnect has been scheduled. Fires before the delay, not before open. */
  reconnect: (info: ReconnectInfo) => void;
  /** `maxRetries` exhausted; no further reconnect attempts will be made. */
  exhausted: () => void;
  /** A `send()` call was queued because the socket is not currently OPEN. */
  buffered: (data: Sendable, queueSize: number) => void;
  /**
   * Parsed version of `message`. Fires after `message` for the same frame.
   * Prefer this over `message` when you do not need the raw frame.
   */
  data: (payload: DataPayload) => void;
}

export interface InactivityWatchdogOptions {
  /**
   * Trigger a recovery attempt when no `open`/`data` event has fired for this
   * many milliseconds.
   */
  silenceMs: number;
  /**
   * After this many ms of *continuous* silence (soft restarts not taking),
   * escalate from `softRestart()` to a forced `restart()`. Use `Infinity` to
   * never escalate.
   *
   * @default silenceMs * 4
   */
  forceRestartAfterMs?: number;
}
