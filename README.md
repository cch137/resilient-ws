# @cch137/resilient-ws

Auto-reconnecting WebSocket client for Node and Deno. Wraps [`ws`](https://github.com/websockets/ws)
with exponential-backoff reconnection, send buffering, a seamless soft-restart,
and an inactivity watchdog that recovers *zombie* connections (a dead socket
that never emits `close`). A small typed event emitter sits on top, plus a
`data` event that pre-parses each frame to JSON/text/binary/blob.

Runs on Node ≥ 20 and Deno.

See [DESIGN.md](./DESIGN.md) for goals, the reconnect model, and the zombie-socket rationale.

## Install

```bash
npm i @cch137/resilient-ws ws
```

`ws` is a peer dependency — install it alongside.

```ts
import { ResilientWebSocket } from "@cch137/resilient-ws";
// Deno: import { ResilientWebSocket } from "npm:@cch137/resilient-ws";
```

## Usage

```ts
import { ResilientWebSocket } from "@cch137/resilient-ws";

const ws = ResilientWebSocket.connect("wss://example.com/stream");

ws.on("open", () => ws.send(JSON.stringify({ type: "subscribe", channel: "ticks" })));
ws.on("data", (payload) => {
  // payload is { type: "json" | "text" | "binary" | "blob", data }
  if (payload.type === "json") handle(payload.data);
});
ws.on("reconnect", ({ attempt, delayMs }) => console.log(`retry #${attempt} in ${delayMs}ms`));
```

`send()` before the first connection (or while reconnecting) is buffered and
flushed, in order, on the next open. Disable with `bufferWhileReconnecting: false`.

### Always-JSON feeds

```ts
import { parseWsData } from "@cch137/resilient-ws";

ws.on("data", async (payload) => {
  const msg = await parseWsData(payload); // text/json/binary/blob → value | undefined
  if (msg) handle(msg);
});
```

### Inactivity watchdog (zombie-connection recovery)

```ts
import { attachInactivityWatchdog } from "@cch137/resilient-ws";

// No open/data for 10s → softRestart(); sustained 40s of silence → restart().
const detach = attachInactivityWatchdog(ws, { silenceMs: 10_000 });
// detach() when disposing a socket you won't reuse.
```

### Proxy / custom agent

The `agent` option (or an async `(url) => agent` factory) is forwarded to `ws`,
so you can route through an HTTP/HTTPS proxy without coupling this library to
any proxy implementation:

```ts
new ResilientWebSocket(url, { agent: (url) => getProxyAgent(url), handshakeTimeoutMs: 5_000 });
```

## API

| Member | Description |
|---|---|
| `new ResilientWebSocket(url, options?)` | Construct without connecting. |
| `ResilientWebSocket.connect(url, options?)` | Construct and connect. |
| `.connect()` / `.close(code?, reason?)` | Open / gracefully close (buffer preserved). |
| `.restart()` | Force a hard reconnect now. |
| `.softRestart()` | Open a new socket, switch over on its open (no visible gap). |
| `.send(data)` | Send, or buffer/throw per state. |
| `.on/.once/.off(event \| event[], fn)` | Typed event emitter. |
| `.readyState` / `.isOpen` / `.bufferSize` | State. |
| `attachInactivityWatchdog(ws, { silenceMs, forceRestartAfterMs? })` | Returns a detach fn. |
| `parseWsData(payload)` | `DataPayload` → JSON value \| `undefined`. |

**Events:** `open`, `message` (raw frame), `data` (parsed `DataPayload`), `error`,
`close`, `reconnect`, `exhausted`, `buffered`.

### Options

| Option | Default | Description |
|---|---|---|
| `protocols` | — | WebSocket sub-protocols. |
| `maxRetries` | `Infinity` | Reconnect attempts before `exhausted`. |
| `backoff` | exp, capped 1s | `(attempt) => delayMs`. |
| `bufferWhileReconnecting` | `true` | Buffer sends while disconnected (post-first-connect). |
| `agent` | — | HTTP(S) agent or async `(url) => agent` factory. |
| `handshakeTimeoutMs` | `10_000` | Abort a stuck opening handshake. |

## License

MIT
