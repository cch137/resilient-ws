import { test } from "node:test";
import assert from "node:assert/strict";

import { parseWsData } from "../src/parse.js";
import type { DataPayload } from "../src/types.js";

test("parseWsData: json frame returns the value verbatim", () => {
  const payload: DataPayload = { type: "json", data: { a: 1 } };
  assert.deepEqual(parseWsData(payload), { a: 1 });
});

test("parseWsData: text frame is JSON-parsed", () => {
  const payload: DataPayload = { type: "text", data: '{"b":2}' };
  assert.deepEqual(parseWsData(payload), { b: 2 });
});

test("parseWsData: empty text frame returns undefined", () => {
  assert.equal(parseWsData({ type: "text", data: "   " }), undefined);
});

test("parseWsData: non-JSON text frame returns undefined (not a throw)", () => {
  // A stray control/heartbeat frame on a JSON feed must be skipped, not crash
  // the handler.
  assert.equal(parseWsData({ type: "text", data: "pong" }), undefined);
});

test("parseWsData: binary frame is decoded then JSON-parsed", () => {
  const bytes = new TextEncoder().encode('{"c":3}');
  assert.deepEqual(parseWsData({ type: "binary", data: bytes }), { c: 3 });
});

test("parseWsData: empty binary frame returns undefined", () => {
  assert.equal(parseWsData({ type: "binary", data: new Uint8Array() }), undefined);
});

test("parseWsData: non-JSON binary frame returns undefined (not a throw)", () => {
  const bytes = new TextEncoder().encode("not json");
  assert.equal(parseWsData({ type: "binary", data: bytes }), undefined);
});
