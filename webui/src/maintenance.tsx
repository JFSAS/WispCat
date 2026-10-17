import { useState, type FormEvent } from "react";
import { useDashboard, useResource } from "./context.tsx";
import {
  accountPath,
  bytes,
  count,
  safeManagementURL,
  validateEnv,
  validateListen,
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
  Notice,
  StatusLine,
} from "./components.tsx";
import type { ConfigurationPlan, Stats } from "./types.ts";

export function Storage() {
  const {
    client,
    selected,
    account,
    revision,
    snapshot: s,
    confirm,
    toast,
  } = useDashboard();
  const [busy, setBusy] = useState(false);
  const kinds = [
    "history",
    "archive",
    "streams",
    "media-cache",
    "outgoing-media",
  ];
  const resource = useResource(
    async () => {
      const results = await Promise.allSettled(
        kinds.map((k) => client.request<Stats>(accountPath(selected, k))),
      );
      return Object.fromEntries(
        results.map((r, i) => [
          kinds[i],
          r.status === "fulfilled"
            ? { stats: r.value, error: "" }
            : { stats: {}, error: (r.reason as Error).message },
        ]),
      ) as Record<string, { stats: Stats; error: string }>;
    },
    [client, selected, revision],
    !!selected,
  );
  if (!account)
    return (
      <Card title="存储维护">
        <Empty
          title="先选择一个账号"
          description="各账号独立维护消息历史和媒体缓存。"
          icon="database"
        />
      </Card>
    );
  if (resource.error)
    return (
      <Notice tone="error" title="无法读取存储状态">
        {resource.error}
      </Notice>
    );
  if (!resource.data) return <Loading />;
  const data = resource.data;
  const items = [
    {
      key: "history",
      title: "消息历史",
      description: "账号的持久消息索引与事件记录",
      icon: "database",
      value: "file_bytes",
      countKey: "messages",
      countLabel: "条消息",
    },
    {
      key: "streams",
      title: "临时传输",
      description: "分块上传与下载的临时文件",
      icon: "folder",
      value: "total_bytes",
      countKey: "file_count",
      countLabel: "个文件",
      quota: "quota_bytes",
      action: "streams/clean",
      actionLabel: "清理过期临时文件",
    },
    {
      key: "media-cache",
      title: "接收媒体缓存",
      description: "微信接收媒体的本地加密副本",
      icon: "file",
      value: "accounted_payload_bytes",
      countKey: "accounted_files",
      countLabel: "个媒体文件",
      quota: "shared_payload_quota_bytes",
      action: "media-cache/compact",
      actionLabel: "回收可释放的接收副本",
    },
    {
      key: "outgoing-media",
      title: "发送媒体副本",
      description: "保留投递核验所需的媒体数据",
      icon: "upload",
      value: "accounted_payload_bytes",
      countKey: "pending_deletions",
      countLabel: "项等待删除",
      action: "outgoing-media/compact",
      actionLabel: "回收已确认发送副本",
    },
  ];
  async function clean(action: string, label: string) {
    if (
      !(await confirm(
        label,
        "运行时只会处理符合回收条件的数据；活跃传输、受保护副本及必要的投递记录会保留。",
      ))
    )
      return;
    setBusy(true);
    try {
      await client.request(accountPath(selected, action), "POST", {});
      toast("维护操作已完成，正在刷新用量。");
      resource.reload();
    } catch (e) {
      toast((e as Error).message, true);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="stack">
      <div className="two-col">
        {items.map((item) => {
          const { stats, error } = data[item.key],
            used = stats[item.value],
            quota = item.quota ? stats[item.quota] : undefined;
          return (
            <Card
              key={item.key}
              title={item.title}
              description={item.description}
              icon={item.icon}
              footer={
                item.action ? (
                  <>
                    <span className="hint">保留正在使用的数据</span>
                    <Button
                      small
                      icon="refresh"
                      disabled={busy || !!error}
                      onClick={() => clean(item.action!, item.actionLabel!)}
                    >
                      {item.actionLabel}
                    </Button>
                  </>
                ) : (
                  <span className="hint">
                    历史索引会持久保留，不提供整库清空操作。
                  </span>
                )
              }
            >
              {error ? (
                <ErrorText error={error} />
              ) : (
                <>
                  <div className="storage-value">{bytes(used)}</div>
                  {typeof used === "number" &&
                    typeof quota === "number" &&
                    quota > 0 && (
                      <progress
                        className="storage-meter"
                        max={quota}
                        value={Math.min(used, quota)}
                        aria-label={`${item.title}占用`}
                      />
                    )}
                  <div className="storage-label">
                    <span>
                      {count(stats[item.countKey])} {item.countLabel}
                    </span>
                    <span>
                      {quota ? `配额 ${bytes(quota)}` : "按账号独立存储"}
                    </span>
                  </div>
                </>
              )}
            </Card>
          );
        })}
      </div>
      <div className="two-col">
        <Card
          title="全局身份索引"
          description="按账号隔离的持久编号映射"
          icon="users"
        >
          <StatusLine label="索引大小">
            {bytes(s.storage.file_bytes)}
          </StatusLine>
          <StatusLine label="已映射身份">
            {count(s.storage.identities)}
          </StatusLine>
          <StatusLine label="消费者游标">{count(s.storage.cursors)}</StatusLine>
          <div className="feature-note">
            <Icon name="shield" />
            <span>账号停用、重新登录与界面更名不会重置持久编号。</span>
          </div>
        </Card>
        <Card
          title="投递归档"
          description="整理已完成的投递记录，保留查询依据"
          icon="database"
        >
          <p className="hint">
            归档不会重发消息。未知或未确认的投递不会被视为成功，也不会因本操作自动重试。
          </p>
          <ErrorText error={data.archive.error} />
          <div className="form-actions">
            <Button
              icon="folder"
              disabled={busy || !!data.archive.error}
              onClick={() => clean("archive/compact", "归档已完成投递")}
            >
              归档已完成投递
            </Button>
          </div>
        </Card>
      </div>
    </div>
  );
}
function BulkConfiguration() {
  const {
    snapshot: s,
    client,
    toast,
    refresh,
    confirm,
    markDirty,
  } = useDashboard();
  const [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  async function save(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError("");
    const form = new FormData(e.currentTarget);
    try {
      const plan: ConfigurationPlan = structuredClone(s.configuration.saved);
      plan.accounts = plan.accounts.map((a, i) => ({
        ...a,
        listen: validateListen(String(form.get(`listen-${i}`)).trim()),
        token_env: validateEnv(String(form.get(`token-${i}`)).trim()),
      }));
      setBusy(true);
      await client.request("/configuration", "POST", {
        revision: s.configuration.revision,
        plan,
      });
      toast("批量配置已保存，监听地址等待应用。");
      refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function apply() {
    if (
      !(await confirm(
        "协调应用全部监听",
        "将一起应用各账号已保存的 OneBot 监听设置，支持端口交换。相关客户端需要重新连接，微信会话继续运行。",
      ))
    )
      return;
    setBusy(true);
    try {
      await client.request("/listeners/apply", "POST", {
        revision: s.listeners.revision,
      });
      toast("全部监听设置已应用。");
      refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <Card
      title="批量监听配置"
      description="一起保存多个账号的端口，可用于交换监听端口"
      action={
        <Badge tone={s.listeners.applied ? "good" : "warn"}>
          {s.listeners.applied ? "已应用" : "等待应用"}
        </Badge>
      }
    >
      <form onSubmit={save} onChange={markDirty}>
        <div className="stack">
          {s.configuration.saved.accounts.map((a, i) => (
            <div key={a.name}>
              <h3 className="card-subhead">{a.name}</h3>
              <div className="form-grid">
                <Field label="OneBot 监听地址">
                  <input
                    name={`listen-${i}`}
                    defaultValue={a.listen}
                    required
                  />
                </Field>
                <Field label="令牌环境变量">
                  <input
                    name={`token-${i}`}
                    defaultValue={a.token_env}
                    required
                  />
                </Field>
              </div>
            </div>
          ))}
        </div>
        <ErrorText error={error} />
        <div className="form-actions">
          <span className="hint">保存不会立即更改运行中的连接。</span>
          <Button
            type="submit"
            disabled={busy || !s.configuration.saved.accounts.length}
          >
            保存批量配置
          </Button>
          <Button
            variant="primary"
            disabled={busy || s.listeners.applied}
            onClick={apply}
          >
            应用全部监听
          </Button>
        </div>
      </form>
    </Card>
  );
}
export function Settings() {
  const {
    snapshot: s,
    client,
    toast,
    refresh,
    confirm,
    markDirty,
    handover,
    demo,
  } = useDashboard();
  const view = s.management;
  const [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  async function save(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError("");
    const form = new FormData(e.currentTarget);
    try {
      const plan = {
        version: 1,
        listen: validateListen(String(form.get("listen")).trim()),
        token_env: validateEnv(String(form.get("token_env")).trim()),
      };
      setBusy(true);
      await client.request("/management", "POST", {
        revision: view.revision,
        plan,
      });
      toast("管理设置已保存，当前地址继续有效。");
      refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function apply() {
    if (
      !(await confirm(
        "切换管理接口",
        `将应用管理地址 ${view.saved.listen} 及新的令牌引用。当前管理连接可能结束，切换后需在新地址重新登录。`,
      ))
    )
      return;
    setBusy(true);
    setError("");
    try {
      const result = await client.request<{
        management_address: string;
        management_token_env: string;
      }>("/management/apply", "POST", {});
      const url = safeManagementURL(result.management_address);
      if (!url)
        throw new Error("应用请求已接受，请查看运行时监听地址后重新连接。");
      if (demo) {
        toast("演示管理设置已应用，未改变真实监听地址。");
        refresh();
      } else handover(url, result.management_token_env);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function recover() {
    setBusy(true);
    try {
      await client.request("/configuration/recover", "POST", {});
      toast("配置恢复操作已完成。");
      refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="stack">
      {s.configuration.persistence !== "ready" && (
        <div className="notice warn">
          <div>
            <strong>配置保存需要恢复</strong>
            <p>上次保存尚未完成，请先恢复已提交的配置，再进行其他修改。</p>
          </div>
          <Button small onClick={recover} disabled={busy}>
            恢复配置保存
          </Button>
        </div>
      )}
      <div className="content-grid">
        <Card
          title="管理接口"
          description="管理页面与 API 使用独立的管理令牌"
          action={
            <Badge tone={view.applied ? "good" : "warn"}>
              {view.applied ? "已应用" : "等待应用"}
            </Badge>
          }
        >
          <form onSubmit={save} key={view.revision} onChange={markDirty}>
            <div className="form-grid">
              <Field label="管理监听地址">
                <input
                  name="listen"
                  defaultValue={view.saved.listen}
                  required
                />
              </Field>
              <Field
                label="管理令牌环境变量"
                hint="引用启动环境中的变量，不在网页中设置密钥"
              >
                <input
                  name="token_env"
                  defaultValue={view.saved.token_env}
                  required
                />
              </Field>
            </div>
            <div className="feature-note">
              <Icon name="shield" />
              <span>
                管理地址仅限回环。切换端口前，请同步调整 SSH
                转发；转发两端使用相同端口。
              </span>
            </div>
            <ErrorText
              error={
                error ||
                (view.error
                  ? "管理接口存在异常，请检查端口和令牌环境变量。"
                  : "")
              }
            />
            <div className="form-actions">
              <Button type="submit" disabled={busy}>
                保存管理设置
              </Button>
              <Button
                variant="primary"
                disabled={busy || view.applied}
                onClick={apply}
              >
                应用管理设置
              </Button>
            </div>
          </form>
        </Card>
        <Card title="访问与隐私" icon="shield">
          <StatusLine label="访问范围">本机回环</StatusLine>
          <StatusLine label="管理鉴权">
            <Badge tone="good">独立令牌</Badge>
          </StatusLine>
          <StatusLine label="令牌持久保存">关闭</StatusLine>
          <div className="feature-note">
            <Icon name="info" />
            <span>
              刷新页面后需要重新输入管理令牌；账号登录缓存由运行时独立加密保存。
            </span>
          </div>
        </Card>
      </div>
      <BulkConfiguration key={s.configuration.revision} />
      <Notice title="WispCat · 0.1.0-alpha.2">
        本项目为非官方学习研究项目。协议变化可能影响连接与账号安全，请使用已授权的账号并妥善保管凭证。
      </Notice>
    </div>
  );
}
