import { test } from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { Agent } from "node:http";
import type { AddressInfo } from "node:net";
import { WebSocketServer, type WebSocket as ServerSocket } from "ws";

import { ResilientWebSocket } from "../src/resilient-websocket.js";
import { attachInactivityWatchdog } from "../src/watchdog.js";
import { parseWsData } from "../src/parse.js";
import type { DataPayload, ResilientWebSocketEvents } from "../src/types.js";

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
  // A JSON text frame is decoded and tagged "json"; parseWsData() returns the
  // value either way, which is what consumers rely on.
  const server = await startServer((sock) => sock.send(JSON.stringify({ hi: 1 })));
  const ws = ResilientWebSocket.connect(server.url);
  try {
    const payload = await waitFor(ws, "data");
    assert.deepEqual(parseWsData(payload), { hi: 1 });
    assert.equal(ws.isOpen, true);
  } finally {
    ws.close();
    await server.close();
  }
});

test("tags JSON text frames as json and binary frames as binary without corruption", async () => {
  const server = await startServer((sock) => {
    sock.send(JSON.stringify({ kind: "text" })); // text frame
    sock.send(Uint8Array.from([1, 2, 3, 255])); // binary frame (non-UTF8 byte)
  });
  const ws = ResilientWebSocket.connect(server.url);
  const payloads: DataPayload[] = [];
  try {
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("timeout")), 2000);
      ws.on("data", (p) => {
        payloads.push(p);
        if (payloads.length === 2) {
          clearTimeout(t);
          resolve();
        }
      });
    });
    const json = payloads.find((p) => p.type === "json");
    const bin = payloads.find((p) => p.type === "binary");
    assert.ok(json, "JSON text frame must be tagged json");
    assert.deepEqual(json.data, { kind: "text" });
    assert.ok(bin, "binary frame must be tagged binary");
    // 0xFF must survive intact — the old String() path corrupted it to U+FFFD.
    assert.deepEqual([...(bin.data as Uint8Array)], [1, 2, 3, 255]);
  } finally {
    ws.close();
    await server.close();
  }
});

test("a failing agent factory surfaces an error and exhausts instead of dying silently", async () => {
  // The factory rejects on every call; with maxRetries: 0 the client should
  // emit one `error` then `exhausted` — never an unhandled rejection or a
  // permanently dead socket.
  const ws = new ResilientWebSocket("ws://127.0.0.1:1/never", {
    agent: () => {
      throw new Error("boom");
    },
    maxRetries: 0,
  });
  try {
    const errP = waitFor(ws, "error", 2000);
    const exhaustedP = waitFor(ws, "exhausted", 2000);
    ws.connect();
    const err = await errP;
    assert.match((err as Error).message, /boom/);
    await exhaustedP;
  } finally {
    ws.close();
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

test("concurrent softRestart() calls open exactly one new socket (no leak)", async () => {
  // Two softRestart() calls in the same tick: agent resolution is async, so the
  // guard cannot rely on _pendingWs being set yet. The generation token must
  // cancel the first soft open so only the second produces a socket — otherwise
  // the first socket is created and then orphaned (never closed).
  const server = await startServer();
  const ws = ResilientWebSocket.connect(server.url);
  try {
    await waitFor(ws, "open");
    await server.waitConnections(1);
    ws.softRestart();
    ws.softRestart();
    await server.waitConnections(2);
    await delay(80);
    assert.equal(ws.isOpen, true);
    assert.equal(
      server.connections.length,
      2,
      "concurrent soft restarts must not leak an extra socket",
    );
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

test("close() cancels an in-flight softRestart instead of reviving the socket", async () => {
  // The pending socket from softRestart() needs a handshake RTT to open. If
  // close() lands in that window, the pending open must NOT resurrect a live
  // connection the caller believes is shut down.
  const server = await startServer();
  const ws = ResilientWebSocket.connect(server.url);
  try {
    await waitFor(ws, "open");
    await server.waitConnections(1);
    ws.softRestart();
    await delay(0); // let doSoftOpen create the pending socket (still handshaking)
    ws.close(); // close mid-handshake
    await delay(200); // the pending handshake would have completed by now
    assert.equal(
      ws.isOpen,
      false,
      "close() must cancel the pending soft restart, not revive the connection",
    );
  } finally {
    ws.close();
    await server.close();
  }
});

test("restart() force-reconnects with a fresh socket", async () => {
  const server = await startServer();
  const ws = ResilientWebSocket.connect(server.url);
  try {
    await waitFor(ws, "open");
    await server.waitConnections(1);
    const reopened = waitFor(ws, "open");
    ws.restart();
    await reopened;
    await server.waitConnections(2);
    assert.equal(ws.isOpen, true);
    assert.ok(server.connections.length >= 2);
  } finally {
    ws.close();
    await server.close();
  }
});

test("bufferWhileReconnecting:false throws on send() while reconnecting", async () => {
  // Drop the first connection so the client enters the reconnect backoff window.
  const server = await startServer((sock, index) => {
    if (index === 0) sock.close();
  });
  const ws = ResilientWebSocket.connect(server.url, {
    bufferWhileReconnecting: false,
  });
  try {
    await waitFor(ws, "open"); // first connection
    // `reconnect` fires synchronously inside the close handler, before the
    // backoff timer reopens — so we are definitely not OPEN at this point.
    await waitFor(ws, "reconnect");
    assert.throws(
      () => ws.send("x"),
      /bufferWhileReconnecting is disabled/,
      "send() must throw while reconnecting when buffering is disabled",
    );
  } finally {
    ws.close();
    await server.close();
  }
});

test("inactivity watchdog escalates from softRestart to restart on sustained silence", async () => {
  // Server accepts but never sends data. softRestart() can't help a zombie that
  // never opens/closes, so after forceRestartAfterMs the watchdog must escalate
  // to the hard restart().
  const server = await startServer();
  const ws = ResilientWebSocket.connect(server.url);
  let restarts = 0;
  const origRestart = ws.restart.bind(ws);
  ws.restart = () => {
    restarts++;
    return origRestart();
  };
  const detach = attachInactivityWatchdog(ws, {
    silenceMs: 40,
    forceRestartAfterMs: 80,
  });
  try {
    await waitFor(ws, "open");
    await delay(300);
    assert.ok(
      restarts >= 1,
      `watchdog must escalate to restart() under sustained silence (got ${restarts})`,
    );
  } finally {
    detach();
    ws.close();
    await server.close();
  }
});

test("clearBuffer() drops queued messages and returns the count", async () => {
  // No server: sends before the first connection are always buffered.
  const ws = new ResilientWebSocket("ws://127.0.0.1:1/never");
  try {
    ws.send("a");
    ws.send("b");
    assert.equal(ws.bufferSize, 2);
    assert.equal(ws.clearBuffer(), 2);
    assert.equal(ws.bufferSize, 0);
    assert.equal(ws.clearBuffer(), 0);
  } finally {
    ws.close();
  }
});

test("invokes an async agent factory per attempt and connects through it", async () => {
  // Exercises the success path of the function-factory agent: it is called once
  // with the target URL, and the connection still opens through the returned
  // agent.
  const server = await startServer();
  let calls = 0;
  let seenUrl = "";
  const ws = ResilientWebSocket.connect(server.url, {
    agent: async (url) => {
      calls++;
      seenUrl = url;
      return new Agent();
    },
  });
  try {
    await waitFor(ws, "open");
    assert.equal(ws.isOpen, true);
    assert.equal(calls, 1);
    assert.equal(seenUrl, server.url);
  } finally {
    ws.close();
    await server.close();
  }
});

test("accepts a static agent instance and connects through it", async () => {
  const server = await startServer();
  const agent = new Agent();
  const ws = ResilientWebSocket.connect(server.url, { agent });
  try {
    await waitFor(ws, "open");
    assert.equal(ws.isOpen, true);
  } finally {
    ws.close();
    await server.close();
  }
});

test("softRestart flushes buffered messages once the new socket opens", async () => {
  // Messages queued before any socket exists must still be flushed, in order,
  // by the socket softRestart() brings up — exercises _flushBuffer on the soft
  // path (the active-socket path is covered by the pre-connect-buffer test).
  const received: string[] = [];
  const server = await startServer((sock) =>
    sock.on("message", (m) => received.push(m.toString())),
  );
  const ws = new ResilientWebSocket(server.url);
  try {
    ws.send("a");
    ws.send("b");
    ws.send("c");
    assert.equal(ws.bufferSize, 3);
    ws.softRestart();
    await server.waitConnections(1);
    await delay(50);
    assert.deepEqual(received, ["a", "b", "c"]);
    assert.equal(ws.bufferSize, 0);
  } finally {
    ws.close();
    await server.close();
  }
});
