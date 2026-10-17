import { useState, type FormEvent } from "react";
import { useDashboard, useResource } from "./context.tsx";
import {
  accountPath,
  updateConnection,
  validateEnv,
  validateListen,
  type ConnectionDraft,
} from "./model.ts";
import {
  Badge,
  Button,
  Card,
  Empty,
  ErrorText,
  Field,
  Icon,
  Loading,
  Modal,
  Notice,
  StateBadge,
} from "./components.tsx";
import type { ConnectionView, ListenerView, Reverse, Post } from "./types.ts";

function ConnectionEditor({
  view,
  original,
  onClose,
  onSaved,
}: {
  view: ConnectionView;
  original?: { kind: "reverse" | "http_post"; index: number };
  onClose: () => void;
  onSaved: () => void;
}) {
  const { client, selected, toast } = useDashboard();
  const existing = original
    ? view.saved[original.kind]?.[original.index]
    : undefined;
  const [kind, setKind] = useState<"reverse" | "http_post">(
    original?.kind || "reverse",
  );
  const [preset, setPreset] = useState("custom"),
    [url, setURL] = useState(existing?.url || ""),
    [name, setName] = useState(existing?.name || ""),
    [env, setEnv] = useState(
      (existing as Reverse)?.token_env ||
        (existing as Post)?.secret_env ||
        "WISPCAT_BOT_TOKEN",
    ),
    [role, setRole] = useState((existing as Reverse)?.role || "Universal");
  const [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  function choose(value: string) {
    setPreset(value);
    if (value === "astrbot") {
      setKind("reverse");
      setName("astrbot");
      setURL("ws://127.0.0.1:6199/ws");
      setEnv("WISPCAT_ASTRBOT_TOKEN");
    }
    if (value === "nonebot") {
      setKind("reverse");
      setName("nonebot");
      setURL("ws://127.0.0.1:8080/onebot/v11/ws");
      setEnv("WISPCAT_BOT_TOKEN");
    }
  }
  async function submit(e: FormEvent) {
    e.preventDefault();
    setError("");
    try {
      const draft: ConnectionDraft = {
        kind,
        name: name.trim(),
        url: url.trim(),
        token_env: env.trim(),
        role,
      };
      const plan = updateConnection(view.saved, draft, original);
      setBusy(true);
      await client.request(accountPath(selected, "connections"), "POST", {
        revision: view.revision,
        plan,
      });
      toast("连接设置已保存，应用后生效。");
      onSaved();
      onClose();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal
      title={existing ? "编辑机器人连接" : "添加机器人连接"}
      onClose={onClose}
    >
      <p>保存后可单独应用连接配置，无需重启微信会话。</p>
      <form onSubmit={submit}>
        <div className="form-grid">
          <Field label="框架预设">
            <select value={preset} onChange={(e) => choose(e.target.value)}>
              <option value="custom">自定义配置</option>
              <option value="astrbot">AstrBot</option>
              <option value="nonebot">NoneBot</option>
            </select>
          </Field>
          <Field label="连接类型">
            <select
              value={kind}
              onChange={(e) => setKind(e.target.value as typeof kind)}
            >
              <option value="reverse">反向 WebSocket</option>
              <option value="http_post">HTTP 事件上报</option>
            </select>
          </Field>
          <Field label="连接名称">
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="例如 astrbot"
              required
              autoFocus
            />
          </Field>
          <Field label="令牌环境变量" hint="需在运行时启动环境中预先设置">
            <input
              value={env}
              onChange={(e) => setEnv(e.target.value)}
              required
            />
          </Field>
          <div className="full">
            <Field
              label={kind === "reverse" ? "WebSocket 地址" : "事件上报地址"}
              hint={
                kind === "reverse"
                  ? "AstrBot 常用路径为 /ws，NoneBot 常用路径为 /onebot/v11/ws。"
                  : "运行时将 OneBot 事件 POST 到此地址，并使用共享密钥签名。"
              }
            >
              <input
                value={url}
                onChange={(e) => setURL(e.target.value)}
                required
                placeholder={
                  kind === "reverse"
                    ? "ws://127.0.0.1:6199/ws"
                    : "http://127.0.0.1:8080/events"
                }
              />
            </Field>
          </div>
          {kind === "reverse" && (
            <div className="full">
              <Field label="连接角色">
                <select value={role} onChange={(e) => setRole(e.target.value)}>
                  <option value="Universal">Universal · 动作与事件</option>
                  <option value="API">API · 仅动作</option>
                  <option value="Event">Event · 仅事件</option>
                </select>
              </Field>
            </div>
          )}
        </div>
        <ErrorText error={error} />
        <div className="form-actions">
          <Button onClick={onClose}>取消</Button>
          <Button type="submit" variant="primary" disabled={busy}>
            {busy ? "保存中…" : "保存连接"}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
function ListenerForm({
  view,
  onSaved,
}: {
  view: ListenerView;
  onSaved: () => void;
}) {
  const { client, selected, toast, confirm, markDirty } = useDashboard();
  const [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  async function save(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const values = new FormData(e.currentTarget);
    setError("");
    try {
      const plan = {
        listen: validateListen(String(values.get("listen")).trim()),
        token_env: validateEnv(String(values.get("token_env")).trim()),
      };
      setBusy(true);
      await client.request(accountPath(selected, "listener"), "POST", {
        revision: view.revision,
        plan,
      });
      toast("OneBot 监听设置已保存。");
      onSaved();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function apply() {
    if (
      !(await confirm(
        "应用 OneBot 监听设置",
        "相关 HTTP 请求和正向 WebSocket 将重新连接，微信会话会继续运行。",
      ))
    )
      return;
    setBusy(true);
    try {
      await client.request(accountPath(selected, "listener/apply"), "POST", {});
      toast("监听应用请求已接受。");
      onSaved();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <Card
      title="OneBot 入站接口"
      description="供外部程序主动调用 HTTP 动作或连接正向 WebSocket"
      action={
        <Badge tone={view.applied ? "good" : "warn"}>
          {view.applied ? "已应用" : "等待应用"}
        </Badge>
      }
    >
      <form onSubmit={save} onChange={markDirty}>
        <div className="form-grid">
          <Field label="监听地址">
            <input name="listen" defaultValue={view.saved.listen} required />
          </Field>
          <Field label="令牌环境变量" hint="填写环境变量名，不是令牌的实际值">
            <input
              name="token_env"
              defaultValue={view.saved.token_env}
              required
            />
          </Field>
        </div>
        <div className="feature-note">
          <Icon name="info" />
          <span>
            仅接受回环地址。远程接入请使用 SSH
            转发；多个账号需要分别使用不同端口。
          </span>
        </div>
        <ErrorText
          error={
            error ||
            (view.error ? "监听存在异常，请检查端口和令牌环境变量。" : "")
          }
        />
        <div className="form-actions">
          <Button type="submit" icon="check" disabled={busy}>
            保存设置
          </Button>
          <Button
            variant="primary"
            onClick={apply}
            disabled={busy || view.applied}
          >
            应用已保存设置
          </Button>
        </div>
      </form>
    </Card>
  );
}
export function Connections() {
  const { client, selected, account, revision, toast, confirm, refresh } =
    useDashboard();
  const [editor, setEditor] = useState<
      { kind: "reverse" | "http_post"; index: number } | "new" | null
    >(null),
    [busy, setBusy] = useState(false);
  const { data, error, reload } = useResource(
    () =>
      Promise.all([
        client.request<ConnectionView>(accountPath(selected, "connections")),
        client.request<ListenerView>(accountPath(selected, "listener")),
      ]),
    [client, selected, revision],
    !!selected,
  );
  if (!account)
    return (
      <Card title="机器人连接">
        <Empty
          title="先选择一个账号"
          description="每个账号分别配置自己的机器人连接。"
          icon="plug"
        />
      </Card>
    );
  if (error)
    return (
      <Notice tone="error" title="无法读取连接配置">
        {error}
        <Button small onClick={reload}>
          重新加载
        </Button>
      </Notice>
    );
  if (!data) return <Loading />;
  const [view, listener] = data;
  const rows = [
    ...(view.saved.reverse || []).map((v, i) => ({
      value: v,
      kind: "reverse" as const,
      index: i,
    })),
    ...(view.saved.http_post || []).map((v, i) => ({
      value: v,
      kind: "http_post" as const,
      index: i,
    })),
  ];
  function done() {
    reload();
    refresh();
  }
  async function apply() {
    if (
      !(await confirm(
        "应用机器人连接",
        "新增或修改的下游连接将重新建立，已移除的连接将关闭；微信会话继续运行。",
      ))
    )
      return;
    setBusy(true);
    try {
      await client.request(
        accountPath(selected, "connections/apply"),
        "POST",
        {},
      );
      toast("机器人连接应用请求已接受。");
      done();
    } catch (e) {
      toast((e as Error).message, true);
    } finally {
      setBusy(false);
    }
  }
  async function remove(
    kind: "reverse" | "http_post",
    index: number,
    name: string,
  ) {
    if (
      !(await confirm(
        "移除机器人连接",
        `将保存移除 ${name} 的设置，应用连接配置后才会断开。`,
      ))
    )
      return;
    const plan = structuredClone(view.saved);
    plan[kind]?.splice(index, 1);
    setBusy(true);
    try {
      await client.request(accountPath(selected, "connections"), "POST", {
        revision: view.revision,
        plan,
      });
      toast("移除设置已保存，等待应用。");
      done();
    } catch (e) {
      toast((e as Error).message, true);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="stack">
      {!view.applied && (
        <div className="notice warn">
          <span>
            <strong>连接配置已保存，尚未应用</strong>
            <p>当前连接继续使用之前的配置。点击应用后建立新连接。</p>
          </span>
          <Button variant="primary" small onClick={apply} disabled={busy}>
            应用连接配置
          </Button>
        </div>
      )}
      <Card
        title="下游机器人连接"
        description="将微信事件推送给 AstrBot、NoneBot 或自定义服务"
        raw
        action={
          <Button
            small
            variant="primary"
            icon="plus"
            onClick={() => setEditor("new")}
          >
            添加连接
          </Button>
        }
        footer={
          <>
            <span className="hint">
              {rows.length} 个连接 · 当前账号 {selected}
            </span>
            <Badge tone={view.applied ? "good" : "warn"}>
              {view.applied ? "配置已应用" : "等待应用"}
            </Badge>
          </>
        }
      >
        {rows.length ? (
          rows.map(({ value, kind, index }) => {
            const state = account.connections.find(
              (c) => c.name === value.name,
            );
            return (
              <div className="connection-row" key={`${kind}-${value.name}`}>
                <span
                  className={`framework-icon ${kind === "http_post" ? "blue" : ""}`}
                >
                  {value.name.toLowerCase().includes("astrbot")
                    ? "A"
                    : kind === "http_post"
                      ? "H"
                      : "N"}
                </span>
                <div className="connection-info">
                  <h3>
                    {value.name}
                    <Badge>
                      {kind === "reverse" ? "反向 WebSocket" : "HTTP 上报"}
                    </Badge>
                  </h3>
                  <p className="mono">{value.url}</p>
                  {state?.error && (
                    <ErrorText error="连接失败，请检查地址、下游服务和两端令牌。" />
                  )}
                </div>
                <StateBadge state={state?.state || "offline"} />
                <div className="list-actions">
                  <button
                    className="icon-btn"
                    aria-label={`编辑连接 ${value.name}`}
                    onClick={() => setEditor({ kind, index })}
                  >
                    <Icon name="edit" />
                  </button>
                  <button
                    className="icon-btn"
                    aria-label={`移除连接 ${value.name}`}
                    disabled={busy}
                    onClick={() => remove(kind, index, value.name)}
                  >
                    <Icon name="trash" />
                  </button>
                </div>
              </div>
            );
          })
        ) : (
          <Empty
            title="添加你的第一个机器人连接"
            description="选择框架预设，填写连接地址和令牌环境变量即可。"
            icon="plug"
          >
            <Button variant="soft" icon="plus" onClick={() => setEditor("new")}>
              添加连接
            </Button>
          </Empty>
        )}
      </Card>
      {view.error && (
        <Notice tone="error" title="连接配置未能完成应用">
          请检查下游服务状态和令牌环境变量，再重新应用。
        </Notice>
      )}
      <ListenerForm key={listener.revision} view={listener} onSaved={done} />
      {editor && (
        <ConnectionEditor
          view={view}
          original={editor === "new" ? undefined : editor}
          onClose={() => setEditor(null)}
          onSaved={done}
        />
      )}
    </div>
  );
}
