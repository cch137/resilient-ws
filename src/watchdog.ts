/**
 * Inactivity watchdog for {@link ResilientWebSocket}.
 */

import type { ResilientWebSocket } from "./resilient-websocket.js";
import type { InactivityWatchdogOptions } from "./types.js";

/**
 * Attach a data-inactivity watchdog to a {@link ResilientWebSocket}.
 *
 * While the socket is connected but no `open`/`data` event has fired for
 * `silenceMs`, the watchdog calls `softRestart()` (a seamless switchover with
 * no visible gap). If silence persists for `forceRestartAfterMs`, it escalates
 * to `restart()`.
 *
 * The escalation matters because {@link ResilientWebSocket}'s built-in backoff
 * is driven solely by the underlying socket's `close` event. The common
 * real-world failure on a flaky network is a *zombie* connection (no FIN/RST,
 * especially when the host is resource-starved): `close` never fires, so the
 * backoff never engages and `softRestart()` alone (no-op while a pending
 * socket is in flight) may never tear the dead socket down. A forced
 * `restart()` unconditionally destroys it and re-engages the reconnect loop.
 *
 * The watchdog auto-stops on `close`/`exhausted` and re-arms on the next
 * `open`/`data`, so it costs nothing while the socket is intentionally closed.
 *
 * @returns A function that detaches the watchdog: clears its timer and removes
 *   all listeners it registered. Call it when disposing a socket that will not
 *   be reused (e.g. a short-lived per-subscription socket) so listeners do not
 *   accumulate.
 */
export function attachInactivityWatchdog(
  ws: ResilientWebSocket,
  options: InactivityWatchdogOptions,
): () => void {
  const silenceMs = options.silenceMs;
  const forceAfterMs = options.forceRestartAfterMs ?? silenceMs * 4;

  let timer: ReturnType<typeof setTimeout> | null = null;
  let downSince = 0;
  let lastReset = 0;

  const fire = (): void => {
    if (downSince === 0) downSince = Date.now();
    if (Date.now() - downSince >= forceAfterMs) {
      // Sustained silence: softRestart hasn't taken (zombie OPEN socket that
      // never emits 'close', or a network limbo where the pending socket
      // never opens). Force a hard restart.
      downSince = 0;
      ws.restart();
    } else {
      // softRestart() is a no-op while a pending socket is in flight; the
      // handshake timeout inside ResilientWebSocket will eventually free it.
      ws.softRestart();
    }
    // Re-arm so a stuck or failed restart keeps being retried until data
    // resumes (which calls `reset` and replaces this timer).
    timer = setTimeout(fire, silenceMs);
  };

  const arm = (): void => {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(fire, silenceMs);
  };

  // Leading-edge throttle: re-arming on every frame is wasteful on a
  // high-frequency feed, so coalesce resets to at most one per silenceMs/10.
  const reset = (): void => {
    const now = Date.now();
    if (now - lastReset < silenceMs * 0.1) return;
    lastReset = now;
    downSince = 0;
    arm();
  };

  const stop = (): void => {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
  };

  ws.on("open", reset);
  ws.on("data", reset);
  ws.on(["close", "exhausted"], stop);

  return () => {
    stop();
    ws.off("open", reset);
    ws.off("data", reset);
    ws.off(["close", "exhausted"], stop);
  };
}
