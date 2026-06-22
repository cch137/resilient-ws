import { test } from "node:test";
import assert from "node:assert/strict";

import { parseWsData } from "../src/parse.js";
import type { DataPayload } from "../src/types.js";

test("parseWsData: json frame returns the value verbatim", async () => {
  const payload: DataPayload = { type: "json", data: { a: 1 } };
  assert.deepEqual(await parseWsData(payload), { a: 1 });
});

test("parseWsData: text frame is JSON-parsed", async () => {
  const payload: DataPayload = { type: "text", data: '{"b":2}' };
  assert.deepEqual(await parseWsData(payload), { b: 2 });
});

test("parseWsData: empty text frame returns undefined", async () => {
  assert.equal(await parseWsData({ type: "text", data: "   " }), undefined);
});

test("parseWsData: binary frame is decoded then JSON-parsed", async () => {
  const bytes = new TextEncoder().encode('{"c":3}');
  assert.deepEqual(await parseWsData({ type: "binary", data: bytes }), { c: 3 });
});

test("parseWsData: empty binary frame returns undefined", async () => {
  assert.equal(
    await parseWsData({ type: "binary", data: new Uint8Array() }),
    undefined,
  );
});

test("parseWsData: blob frame is decoded then JSON-parsed", async () => {
  const blob = new Blob([new TextEncoder().encode('{"d":4}')]);
  assert.deepEqual(await parseWsData({ type: "blob", data: blob }), { d: 4 });
});
