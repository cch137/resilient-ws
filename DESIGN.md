# Design — @cch137/resilient-ws

## Goal

A minimal, dependable auto-reconnecting WebSocket for long-lived real-time
feeds (market data, price streams). It must survive flaky networks unattended:
ordinary disconnects, slow/limbo handshakes, and *zombie* connections that go
dead without ever emitting `close`.

## Non-goals

- Not a server. Client only.
- No subscription/topic re-sync layer. On reconnect, the app re-subscribes in
  its `open` handler — this library reports lifecycle, it does not replay
  application protocol state.
- No message framing/serialization beyond the optional `parseWsData` helper.
- No shared-socket multiplexing.

## Engine

Built on [`ws`](https://github.com/websockets/ws) (a peer dependency), not the
platform `WebSocket`. `ws` is required for the proxy `agent` option,
`handshakeTimeout`, `terminate()` (force-destroy a half-dead socket), and the
Node `EventEmitter` interface the reconnect logic relies on — none of which the
standard `WebSocket` exposes. Works under Node ≥ 20 and Deno (via `npm:`).

## Reconnect model

The built-in reconnect loop is driven **solely** by the underlying socket's
`close` event. On close (unless the app called `close()`), it schedules the
next open via `backoff(attempt)` — default exponential, `0, 100, 200, 400,
800ms`, capped at `1s` — until `maxRetries` is reached, at which point it emits
`exhausted` and refuses further sends.

A monotonically increasing generation guard and a `_detach()` step (which
removes listeners, installs a no-op `error` sink, and `terminate()`s the old
socket) ensure a stale or superseded socket can never drive a duplicate
reconnect cycle or leak its TLS/receiver buffers.

### Send buffering

Before the first successful connection, sends are **always** buffered. After
that, buffering while disconnected is on by default (`bufferWhileReconnecting`)
and the queue is flushed, in order, immediately before the `open` event so the
remote receives backlog then any sends made inside the `open` handler.

### softRestart vs restart

- `restart()` — tear down the current socket and open a fresh one immediately.
- `softRestart()` — open a *new* socket first and switch over only when it
  reaches `open`, so the app sees no gap. No-op if one is already pending or the
  socket was explicitly closed; if the pending socket fails before opening, the
  current one is kept and nothing is emitted.

## Zombie connections & the watchdog

The hard real-world failure on a starved or flaky host is a connection that
stops delivering data but never sends FIN/RST: `close` never fires, so the
backoff loop never engages and `softRestart()` alone (a no-op while a pending
socket is in flight) may never tear the dead socket down.

`attachInactivityWatchdog` closes this gap. It tracks `open`/`data` activity and,
after `silenceMs` of silence, calls `softRestart()`; after sustained silence of
`forceRestartAfterMs` (default `silenceMs * 4`) it escalates to `restart()`,
which unconditionally destroys the socket and re-engages the reconnect loop. It
auto-stops on `close`/`exhausted`, re-arms on the next `open`/`data`, and
leading-edge-throttles resets so a high-frequency feed doesn't re-arm per frame.
Pair it with a short `handshakeTimeoutMs` so a stuck CONNECTING socket churns
fast enough for the escalation to take effect.

## Decoupling

The library never imports any proxy/VPN code. Routing is injected through the
`agent` option (value or async `(url) => agent` factory), keeping deployment
concerns in the caller.
