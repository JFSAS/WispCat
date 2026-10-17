import { test } from "node:test";
import assert from "node:assert/strict";
import {
  connectionFromDraft,
  updateConnection,
  validateCredential,
  validateListen,
  safeManagementURL,
  accountPath,
} from "./model.ts";
import type { ConnectionPlan } from "./types.ts";

test("editing a connection preserves siblings and maps HTTP secrets to the backend schema", () => {
  const plan: ConnectionPlan = {
    reverse: [
      {
        name: "old",
        url: "ws://127.0.0.1:6199/ws",
        token_env: "OLD",
        role: "Event",
      },
      {
        name: "keep",
        url: "ws://127.0.0.1:8080/ws",
        token_env: "KEEP",
        role: "API",
      },
    ],
    http_post: [],
  };
  const before = structuredClone(plan);
  const next = updateConnection(
    plan,
    {
      kind: "http_post",
      name: "webhook",
      url: "http://127.0.0.1:8080/event",
      token_env: "POST_SECRET",
      role: "Universal",
    },
    { kind: "reverse", index: 0 },
  );
  assert.deepEqual(plan, before);
  assert.deepEqual(next.reverse, [before.reverse![1]]);
  assert.deepEqual(next.http_post, [
    {
      name: "webhook",
      url: "http://127.0.0.1:8080/event",
      secret_env: "POST_SECRET",
    },
  ]);
  assert.throws(
    () =>
      updateConnection(plan, {
        kind: "reverse",
        name: "keep",
        url: "ws://127.0.0.1:3",
        token_env: "TOKEN",
        role: "Universal",
      }),
    /重复/,
  );
});
test("forms reject incompatible URLs, embedded credentials, invalid roles and public listeners", () => {
  for (const url of [
    "http://127.0.0.1:80",
    "ws://user:pass@host/ws",
    "ws://127.0.0.1/ws#token",
    "file:///tmp/cache",
  ])
    assert.throws(() =>
      connectionFromDraft({
        kind: "reverse",
        name: "bot",
        url,
        token_env: "TOKEN",
        role: "Universal",
      }),
    );
  assert.throws(() =>
    connectionFromDraft({
      kind: "reverse",
      name: "bot",
      url: "ws://127.0.0.1:3",
      token_env: "TOKEN",
      role: "Other",
    }),
  );
  for (const listen of [
    "0.0.0.0:5099",
    "localhost:5099",
    "127.0.0.1:65536",
    "127.300.0.1:5000",
    "127.0.0.1:5099/path",
  ])
    assert.throws(() => validateListen(listen));
  assert.equal(validateListen("[::1]:5099"), "[::1]:5099");
  assert.equal(safeManagementURL("127.0.0.1:5108"), "http://127.0.0.1:5108/");
  assert.equal(safeManagementURL("127.0.0.1:0"), null);
  assert.equal(safeManagementURL("evil.invalid:5099"), null);
  assert.equal(accountPath("x/y", "login"), "/accounts/x%2Fy/login");
});
test("credential validation enforces file bounds and password bytes before upload", () => {
  const file = new File(["encrypted"], "login.wxlogin");
  validateCredential(file, "long-local-preview-password");
  assert.throws(() => validateCredential(file, "short"));
  assert.throws(() =>
    validateCredential(
      new File(["x"], "plain.txt"),
      "long-local-preview-password",
    ),
  );
  assert.throws(() =>
    validateCredential(
      new File([new Uint8Array(1048577)], "large.wxlogin"),
      "long-local-preview-password",
    ),
  );
});
