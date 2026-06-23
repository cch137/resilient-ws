/**
 * Payload parsing helper for the {@link ResilientWebSocket} `data` event.
 */

import type { DataPayload } from "./types.js";

const textDecoder = new TextDecoder();

/**
 * Parse a {@link DataPayload} (text / json / binary) to its JSON value, or
 * `undefined` for empty or non-JSON frames.
 *
 * Convenience for feeds that always send JSON, regardless of whether the
 * transport delivered it as a text or binary frame. A non-JSON frame yields
 * `undefined` rather than throwing, so a stray control/heartbeat frame on the
 * same socket is skipped instead of rejecting the handler:
 *
 * @example
 * ws.on("data", (payload) => {
 *   const msg = parseWsData(payload);
 *   if (msg) handle(msg);
 * });
 */
export function parseWsData(payload: DataPayload): unknown {
  switch (payload.type) {
    case "json":
      return payload.data;
    case "text":
      return tryParseJson(payload.data);
    case "binary":
      return tryParseJson(textDecoder.decode(payload.data));
  }
}

/** JSON-parse a frame body; `undefined` for empty or malformed input. */
function tryParseJson(raw: string): unknown {
  const text = raw.trim();
  if (!text) return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}
