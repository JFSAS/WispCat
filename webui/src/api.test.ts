import { test } from "node:test";
import assert from "node:assert/strict";
import { RuntimeClient } from "./api.ts";
import { DemoClient } from "./demo.ts";

test("the admin token stays in headers and cannot follow redirects or external paths", async () => {
  const calls: { path: string; init: RequestInit | undefined }[] = [];
  const transport = async function (
    this: typeof globalThis,
    path: string | URL | Request,
    init?: RequestInit,
  ) {
    assert.equal(this, globalThis);
    calls.push({ path: String(path), init });
    return new Response("{}", {
      headers: { "Content-Type": "application/json" },
    });
  } as typeof fetch;
  const client = new RuntimeClient("synthetic-only-preview-token", transport);
  await client.request("/accounts");
  assert.equal(
    (calls[0].init?.headers as Record<string, string>).Authorization,
    "Bearer synthetic-only-preview-token",
  );
  assert.equal(calls[0].init?.redirect, "error");
  assert.equal(calls[0].init?.cache, "no-store");
  for (const path of [
    "//evil.invalid/x",
    "/\\evil.invalid/x",
    "https://evil.invalid/x",
  ])
    await assert.rejects(client.request(path));
  client.clear();
  await assert.rejects(client.request("/accounts"), /管理令牌/);
  assert.equal(calls.length, 1);
});
test("credential uploads keep the browser-generated multipart boundary", async () => {
  let sent: RequestInit | undefined;
  const client = new RuntimeClient("synthetic-only-preview-token", (async (
    _path: unknown,
    init: RequestInit,
  ) => {
    sent = init;
    return new Response('{"status":"queued"}');
  }) as typeof fetch);
  const body = new FormData();
  body.set("file", new File(["cipher"], "login.wxlogin"));
  body.set("passphrase", "synthetic-only-password");
  await client.request("/accounts/main/login/import", "POST", body);
  assert.equal(sent?.body, body);
  assert.equal(
    (sent?.headers as Record<string, string>)["Content-Type"],
    undefined,
  );
});
test("sample mode makes no network requests and supports saved/apply states", async () => {
  const original = globalThis.fetch;
  let requests = 0;
  globalThis.fetch = (async () => {
    requests++;
    throw new Error("network forbidden");
  }) as typeof fetch;
  try {
    const client = new DemoClient();
    const view = await client.request<{
      revision: string;
      saved: unknown;
      applied: boolean;
    }>("/accounts/main/connections");
    await client.request("/accounts/main/connections", "POST", {
      revision: view.revision,
      plan: view.saved,
    });
    assert.equal(
      (await client.request<{ applied: boolean }>("/accounts/main/connections"))
        .applied,
      false,
    );
    await client.request("/accounts/main/connections/apply", "POST", {});
    assert.equal(
      (await client.request<{ applied: boolean }>("/accounts/main/connections"))
        .applied,
      true,
    );
    await assert.rejects(
      client.request("/accounts/main/login/start", "POST", {}),
      /演示/,
    );
    assert.equal(requests, 0);
  } finally {
    globalThis.fetch = original;
  }
});
