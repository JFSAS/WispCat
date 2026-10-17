import type {
  Client,
  Snapshot,
  ConnectionView,
  ListenerView,
  LoginView,
  Stats,
  AccountConfig,
  RuntimeAccount,
} from "./types.ts";
// Explicit in-browser sample mode. No HTTP request or real credential is used.
export class DemoClient implements Client {
  #serial = 1;
  #snapshot: Snapshot;
  #connections: Record<string, ConnectionView> = {};
  #listeners: Record<string, ListenerView> = {};
  constructor() {
    const accounts: AccountConfig[] = [
      {
        name: "main",
        instance: "instance:main",
        mode: "offline",
        listen: "127.0.0.1:5700",
        token_env: "WISPCAT_ONEBOT_TOKEN",
        reverse: [
          {
            name: "astrbot",
            url: "ws://127.0.0.1:6199/ws",
            token_env: "WISPCAT_ASTRBOT_TOKEN",
            role: "Universal",
          },
        ],
        http_post: [],
      },
      {
        name: "sandbox",
        instance: "instance:sandbox",
        mode: "offline",
        listen: "127.0.0.1:5701",
        token_env: "WISPCAT_ONEBOT_TOKEN",
        reverse: [],
        http_post: [],
      },
    ];
    const management = {
      version: 1,
      listen: "127.0.0.1:5108",
      token_env: "WISPCAT_ADMIN_TOKEN",
    };
    this.#snapshot = {
      accounts: [
        this.runtime(accounts[0], true),
        this.runtime(accounts[1], false),
      ],
      topology: {
        saved: { version: 1, accounts: structuredClone(accounts) },
        revision: "demo-1",
        applied: true,
      },
      management: {
        saved: management,
        revision: "demo-1",
        applied: true,
        listener: { address: management.listen, state: "listening" },
      },
      configuration: {
        saved: { version: 1, management, accounts },
        revision: "demo-1",
        persistence: "ready",
      },
      listeners: { revision: "demo-1", applied: true },
      storage: {
        format: "encrypted_identity_index_v2",
        identities: 156,
        cursors: 3,
        file_bytes: 131072,
        legacy_imports: 0,
      },
    };
    for (const a of accounts) {
      this.#connections[a.name] = {
        saved: { reverse: structuredClone(a.reverse || []), http_post: [] },
        revision: "demo-1",
        applied: true,
      };
      this.#listeners[a.name] = {
        saved: { listen: a.listen, token_env: a.token_env },
        revision: "demo-1",
        applied: true,
        listener: { address: a.listen, state: "listening" },
      };
    }
  }
  private runtime(a: AccountConfig, online: boolean): RuntimeAccount {
    return {
      name: a.name,
      onebot_address: a.listen,
      status: {
        state: online ? "connected" : "offline",
        mode: online ? "online" : "offline",
        session_state: online ? "validated" : "unverified",
        detail: online ? "示例账号连接正常" : "示例账号暂未登录",
        last_sync_at: online ? Math.floor(Date.now() / 1000) - 18 : 0,
        contact_count: online ? 128 : 0,
        sync_calls: online ? 246 : 0,
        send_calls: online ? 32 : 0,
        metrics: { unique_messages: online ? 864 : 0 },
        login_source: online ? "checkpoint" : "",
        restored_from_checkpoint: online,
      },
      connections: (a.reverse || []).map((r) => ({
        name: r.name,
        kind: "wsClient",
        state: online ? "connected" : "disconnected",
      })),
    };
  }
  clear() {}
  async image(): Promise<Blob> {
    throw new Error("演示模式不会生成真实微信二维码。");
  }
  async request<T>(path: string, method = "GET", body?: unknown): Promise<T> {
    const out = await this.handle(path, method, body);
    if (method === "POST") {
      const s = this.#snapshot;
      s.configuration.saved = {
        version: 1,
        management: structuredClone(s.management.saved),
        accounts: s.topology.saved.accounts.map((a) => ({
          ...structuredClone(a),
          ...this.#listeners[a.name]?.saved,
          reverse: structuredClone(
            this.#connections[a.name]?.saved.reverse || a.reverse || [],
          ),
          http_post: structuredClone(
            this.#connections[a.name]?.saved.http_post || a.http_post || [],
          ),
        })),
      };
      s.configuration.revision = `demo-${++this.#serial}`;
      s.listeners.applied = s.topology.saved.accounts.every(
        (a) => this.#listeners[a.name]?.applied,
      );
      s.listeners.revision = `demo-${this.#serial}`;
    }
    return structuredClone(out) as T;
  }
  private async handle(
    path: string,
    method: string,
    body?: unknown,
  ): Promise<unknown> {
    const s = this.#snapshot;
    if (method === "GET") {
      const roots: Record<string, unknown> = {
        "/accounts": s.accounts,
        "/topology": s.topology,
        "/management": s.management,
        "/configuration": s.configuration,
        "/listeners": s.listeners,
        "/storage": s.storage,
      };
      if (path in roots) return roots[path];
    }
    const revision = () => `demo-${++this.#serial}`;
    const input = body as { revision?: string; plan?: unknown };
    if (path === "/topology") {
      if (method === "POST") {
        s.topology.saved = structuredClone(
          input.plan,
        ) as Snapshot["topology"]["saved"];
        s.topology.revision = revision();
        s.topology.applied = false;
      }
      return s.topology;
    }
    if (path === "/topology/apply") {
      s.accounts = s.topology.saved.accounts.map(
        (a) =>
          s.accounts.find((x) => x.name === a.name) || this.runtime(a, false),
      );
      for (const a of s.topology.saved.accounts) {
        this.#connections[a.name] ||= {
          saved: { reverse: a.reverse || [], http_post: a.http_post || [] },
          revision: revision(),
          applied: true,
        };
        this.#listeners[a.name] ||= {
          saved: { listen: a.listen, token_env: a.token_env },
          revision: revision(),
          applied: true,
          listener: { address: a.listen, state: "listening" },
        };
      }
      s.topology.applied = true;
      return s.topology;
    }
    if (path === "/management") {
      s.management.saved = structuredClone(
        input.plan,
      ) as Snapshot["management"]["saved"];
      s.management.revision = revision();
      s.management.applied = false;
      return s.management;
    }
    if (path === "/management/apply") {
      s.management.applied = true;
      return {
        operation: { id: "demo", state: "completed" },
        management_address: s.management.saved.listen,
        management_token_env: s.management.saved.token_env,
      };
    }
    if (path === "/configuration") {
      s.configuration.saved = structuredClone(
        input.plan,
      ) as Snapshot["configuration"]["saved"];
      s.configuration.revision = revision();
      for (const desired of s.configuration.saved.accounts) {
        this.#listeners[desired.name] = {
          ...this.#listeners[desired.name],
          saved: { listen: desired.listen, token_env: desired.token_env },
          revision: revision(),
          applied: false,
        };
      }
      s.listeners.applied = false;
      return s.configuration;
    }
    if (path === "/configuration/recover") {
      s.configuration.persistence = "ready";
      return s.configuration;
    }
    if (path === "/listeners/apply") {
      for (const a of s.accounts) {
        const l = this.#listeners[a.name];
        if (l) {
          l.applied = true;
          a.onebot_address = l.saved.listen;
        }
      }
      s.listeners.applied = true;
      return s.listeners;
    }
    const match = /^\/accounts\/([^/]+)\/(.+)$/.exec(path);
    if (!match) throw new Error("演示模式暂不支持此操作。");
    const name = decodeURIComponent(match[1]),
      action = match[2],
      a = s.accounts.find((a) => a.name === name);
    if (!a) throw new Error("演示账号不存在。");
    if (method === "GET") {
      if (action === "connections") return this.#connections[name];
      if (action === "listener") return this.#listeners[name];
      if (action === "login")
        return {
          state: a.status.state === "connected" ? "confirmed" : "cancelled",
          image_ready: false,
        } satisfies LoginView;
      const stats: Record<string, Stats> = {
        history: {
          messages: name === "main" ? 864 : 0,
          conversations: 12,
          users: 128,
          file_bytes: 3145728,
          working_cache_bytes: 278528,
        },
        archive: {
          total: 32,
          active: 0,
          archived: 32,
          completed: 32,
          file_bytes: 262144,
        },
        streams: {
          total_bytes: 2359296,
          quota_bytes: 134217728,
          file_count: 3,
          active_uploads: 0,
        },
        "media-cache": {
          accounted_payload_bytes: 6402048,
          shared_payload_quota_bytes: 268435456,
          accounted_files: 18,
          active_native_tasks: 0,
        },
        "outgoing-media": {
          accounted_payload_bytes: 1572864,
          protected: 3,
          pending_deletions: 0,
          released_files: 5,
        },
      };
      if (action in stats) return stats[action];
    }
    if (action === "connections") {
      this.#connections[name] = {
        saved: structuredClone(input.plan) as ConnectionView["saved"],
        revision: revision(),
        applied: false,
      };
      return this.#connections[name];
    }
    if (action === "connections/apply") {
      this.#connections[name].applied = true;
      a.connections = (this.#connections[name].saved.reverse || []).map(
        (r) => ({ name: r.name, kind: "wsClient", state: "connected" }),
      );
      return { status: "queued" };
    }
    if (action === "listener") {
      this.#listeners[name] = {
        ...this.#listeners[name],
        saved: structuredClone(input.plan) as ListenerView["saved"],
        revision: revision(),
        applied: false,
      };
      return this.#listeners[name];
    }
    if (action === "listener/apply") {
      this.#listeners[name].applied = true;
      a.onebot_address = this.#listeners[name].saved.listen;
      return { status: "queued" };
    }
    if (action === "online" || action === "offline") {
      a.status.state = action === "online" ? "connected" : "offline";
      a.status.mode = action;
      return { status: "queued" };
    }
    if (action === "login/start")
      throw new Error("演示模式不会向微信申请二维码；请在真实本地服务中体验。");
    if (action === "login/import") {
      a.status.state = "connected";
      a.status.session_state = "validated";
      return { status: "queued" };
    }
    return { status: "queued" };
  }
}
