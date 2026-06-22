/**
 * Payload parsing helper for the {@link ResilientWebSocket} `data` event.
 */

import type { DataPayload } from "./types.js";

const textDecoder = new TextDecoder();

/**
 * Parse a {@link DataPayload} (text / json / binary / blob) to its JSON value,
 * or `undefined` for empty / non-JSON frames.
 *
 * Convenience for feeds that always send JSON, regardless of whether the
 * transport delivered it as a text or binary frame:
 *
 * @example
 * ws.on("data", async (payload) => {
 *   const msg = await parseWsData(payload);
 *   if (msg) handle(msg);
 * });
 */
export async function parseWsData(payload: DataPayload): Promise<unknown> {
  switch (payload.type) {
    case "text": {
      const text = payload.data.trim();
      return text ? JSON.parse(text) : undefined;
    }
    case "json":
      return payload.data;
    case "binary": {
      const text = textDecoder.decode(payload.data).trim();
      return text ? JSON.parse(text) : undefined;
    }
    case "blob": {
      const text = textDecoder.decode(await payload.data.arrayBuffer()).trim();
      return text ? JSON.parse(text) : undefined;
    }
  }
}
