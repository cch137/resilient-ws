import { test } from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { WebSocketServer, type WebSocket as ServerSocket } from "ws";

import { ResilientWebSocket } from "../src/resilient-websocket.js";
import { attachInactivityWatchdog } from "../src/watchdog.js";
import { parseWsData } from "../src/parse.js";
import type { ResilientWebSocketEvents } from "../src/types.js";

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

interface Server {
  url: string;
  /** Every server-side socket accepted, in order. */
  connections: ServerSocket[];
  /** Resolves once `n` connections have been accepted. */
  waitConnections(n: number, ms?: number): Promise<void>;
  close(): Promise<void>;
}

async function startServer(
  onConnection?: (sock: ServerSocket, index: number) => void,
): Promise<Server> {
  const wss = new WebSocketServer({ port: 0 });
  await once(wss, "listening");
  const port = (wss.address() as AddressInfo).port;

  const connections: ServerSocket[] = [];
  const waiters: { n: number; resolve: () => void }[] = [];

  wss.on("connection", (sock) => {
    const index = connections.length;
    connections.push(sock);
    sock.on("error", () => {});
    onConnection?.(sock, index);
    for (const w of waiters.slice()) {
      if (connections.length >= w.n) {
        w.resolve();
        waiters.splice(waiters.indexOf(w), 1);
      }
    }
  });

  return {
    url: `ws://127.0.0.1:${port}`,
    connections,
    waitConnections(n, ms = 2000) {
      if (connections.length >= n) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const t = setTimeout(
          () =>
            reject(
              new Error(
                `timeout waiting for ${n} connections (have ${connections.length})`,
              ),
            ),
          ms,
        );
        waiters.push({
          n,
          resolve: () => {
            clearTimeout(t);
            resolve();
          },
        });
      });
    },
    close() {
      for (const c of connections) {
        try {
          c.terminate();
        } catch {
          // ignore
        }
      }
      return new Promise((resolve) => wss.close(() => resolve()));
    },
  };
}

function waitFor<K extends keyof ResilientWebSocketEvents>(
  ws: ResilientWebSocket,
  event: K,
  ms = 2000,
): Promise<Parameters<ResilientWebSocketEvents[K]>[0]> {
  return new Promise((resolve, reject) => {
    const handler = ((arg: unknown) => {
      clearTimeout(timer);
      resolve(arg as never);
    }) as ResilientWebSocketEvents[K];
    const timer = setTimeout(() => {
      ws.off(event, handler);
      reject(new Error(`timeout waiting for "${event}"`));
    }, ms);
    ws.once(event, handler);
  });
}

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test("connects and surfaces a JSON frame the data event can parse", async () => {
  // NOTE: `ws` delivers even text frames as a Buffer, so the raw payload is
  // tagged "text"; the consumer-facing path is parseWsData(), asserted here.
  const server = await startServer((sock) => sock.send(JSON.stringify({ hi: 1 })));
  const ws = ResilientWebSocket.connect(server.url);
  try {
    const payload = await waitFor(ws, "data");
    assert.deepEqual(await parseWsData(payload), { hi: 1 });
    assert.equal(ws.isOpen, true);
  } finally {
    ws.close();
    await server.close();
  }
});

test("buffers sends made before the first connection and flushes them in order", async () => {
  const received: string[] = [];
  const server = await startServer((sock) =>
    sock.on("message", (m) => received.push(m.toString())),
  );
  const ws = new ResilientWebSocket(server.url);
  // Sent before connect() — must be buffered, then flushed in order on open.
  ws.send("a");
  ws.send("b");
  ws.send("c");
  assert.equal(ws.bufferSize, 3);
  ws.connect();
  try {
    await waitFor(ws, "open");
    await delay(50);
    assert.deepEqual(received, ["a", "b", "c"]);
    assert.equal(ws.bufferSize, 0);
  } finally {
    ws.close();
    await server.close();
  }
});

test("auto-reconnects after the server drops the first connection", async () => {
  // Close only the first accepted connection; the second stays open.
  const server = await startServer((sock, index) => {
    if (index === 0) sock.close();
  });
  const ws = ResilientWebSocket.connect(server.url);
  try {
    await waitFor(ws, "open"); // first open
    await waitFor(ws, "close"); // server dropped it
    await waitFor(ws, "open"); // reconnected
    assert.equal(ws.isOpen, true);
    assert.ok(server.connections.length >= 2);
  } finally {
    ws.close();
    await server.close();
  }
});

test("softRestart swaps to a fresh socket without an app-visible close", async () => {
  const server = await startServer();
  const ws = ResilientWebSocket.connect(server.url);
  let appClose = 0;
  ws.on("close", () => appClose++);
  try {
    await waitFor(ws, "open");
    await server.waitConnections(1);
    ws.softRestart();
    await server.waitConnections(2);
    await delay(50);
    assert.equal(ws.isOpen, true);
    assert.equal(appClose, 0, "soft restart must not surface a close to the app");
  } finally {
    ws.close();
    await server.close();
  }
});

test("stops reconnecting and emits exhausted once maxRetries is hit", async () => {
  // Point at a port with no listener (connection refused) so every open fails
  // without an intervening success. With maxRetries: 1 the client gives up
  // after one retry. (A success resets the attempt counter, so the connection
  // must genuinely fail to reach exhaustion.)
  const server = await startServer();
  const url = server.url;
  await server.close(); // free the port → subsequent connects are refused
  const ws = ResilientWebSocket.connect(url, { maxRetries: 1 });
  try {
    await waitFor(ws, "exhausted", 3000);
    assert.throws(() => ws.send("x"), /exhausted/);
  } finally {
    ws.close();
  }
});

test("inactivity watchdog soft-restarts a silent-but-open socket, and detaches cleanly", async () => {
  // Server accepts but never sends data → the watchdog should fire.
  const server = await startServer();
  const ws = ResilientWebSocket.connect(server.url);
  const detach = attachInactivityWatchdog(ws, {
    silenceMs: 60,
    forceRestartAfterMs: Infinity, // stay on softRestart for a deterministic test
  });
  try {
    await waitFor(ws, "open");
    await server.waitConnections(2, 2000); // watchdog forced a reconnect
    const afterFire = server.connections.length;
    detach();
    await delay(200);
    assert.equal(
      server.connections.length,
      afterFire,
      "no new connections must appear after detach",
    );
  } finally {
    ws.close();
    await server.close();
  }
});
