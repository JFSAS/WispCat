import { useState, type FormEvent } from "react";
import { useDashboard } from "./context.tsx";
import {
  accountPath,
  count,
  time,
  validateEnv,
  validateListen,
  validateName,
} from "./model.ts";
import {
  Avatar,
  Badge,
  Button,
  Card,
  Empty,
  ErrorText,
  Field,
  Modal,
  Notice,
  StateBadge,
  StatusLine,
} from "./components.tsx";
import type { AccountConfig } from "./types.ts";

function AddAccount({ onClose }: { onClose: () => void }) {
  const { client, snapshot: s, refresh, toast } = useDashboard();
  const [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = new FormData(e.currentTarget);
    try {
      const value = (name: string) => String(form.get(name) || "").trim();
      const account: AccountConfig = {
        name: validateName(value("name")),
        instance: value("instance"),
        mode: "offline",
        listen: validateListen(value("listen")),
        token_env: validateEnv(value("token_env")),
      };
      if (
        !account.instance ||
        (!account.instance.startsWith("instance:") &&
          !account.instance.startsWith("/"))
      )
        throw new Error("实例标识应为 instance:名称，或微信安装的绝对路径。");
      if (
        s.topology.saved.accounts.some(
          (a) => a.name === account.name || a.instance === account.instance,
        )
      )
        throw new Error("账号名称或实例标识已经存在。");
      setBusy(true);
      await client.request("/topology", "POST", {
        revision: s.topology.revision,
        plan: { version: 1, accounts: [...s.topology.saved.accounts, account] },
      });
      toast("账号设置已保存。应用账号列表后即可登录。");
      refresh();
      onClose();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <Modal title="添加微信账号" onClose={onClose}>
      <p>每个微信账号使用独立实例。新账号默认离线，应用后再进行登录。</p>
      <form onSubmit={submit}>
        <div className="form-grid">
          <Field label="账号名称" hint="英文字母、数字、短横线或下划线">
            <input
              name="name"
              required
              maxLength={64}
              placeholder="例如 personal"
              autoFocus
            />
          </Field>
          <Field label="实例标识" hint="用于隔离账号缓存与消息编号">
            <input name="instance" required placeholder="instance:personal" />
          </Field>
          <Field label="OneBot 监听地址">
            <input name="listen" defaultValue="127.0.0.1:5702" required />
          </Field>
          <Field label="令牌环境变量" hint="需在运行时的启动环境中预先设置">
            <input
              name="token_env"
              defaultValue="WISPCAT_ONEBOT_TOKEN"
              required
            />
          </Field>
        </div>
        <ErrorText error={error} />
        <div className="form-actions">
          <Button onClick={onClose}>取消</Button>
          <Button type="submit" variant="primary" icon="plus" disabled={busy}>
            {busy ? "保存中…" : "保存账号"}
          </Button>
        </div>
      </form>
    </Modal>
  );
}
export function Accounts() {
  const {
    snapshot: s,
    client,
    account,
    select,
    navigate,
    toast,
    refresh,
    confirm,
  } = useDashboard();
  const [adding, setAdding] = useState(false),
    [busy, setBusy] = useState(false);
  async function apply() {
    if (
      !(await confirm(
        "应用账号列表",
        "将启动新加入的账号，并停止已移除账号的连接。现有缓存与历史会保留。",
      ))
    )
      return;
    setBusy(true);
    try {
      await client.request("/topology/apply", "POST", {});
      toast("账号列表已应用。");
      refresh();
    } catch (e) {
      toast((e as Error).message, true);
    } finally {
      setBusy(false);
    }
  }
  async function operate(action: string) {
    if (!account) return;
    if (
      !(await confirm(
        action === "offline" ? "切换账号为离线" : "从缓存恢复连接",
        action === "offline"
          ? "将停止此账号的微信会话。登录缓存和消息历史会保留。"
          : "将使用当前实例的已有缓存尝试连接微信；没有可用缓存时，需要先登录。",
      ))
    )
      return;
    setBusy(true);
    try {
      await client.request(accountPath(account.name, action), "POST", {});
      toast("操作已接受，请等待账号状态更新。");
      refresh();
    } catch (e) {
      toast((e as Error).message, true);
    } finally {
      setBusy(false);
    }
  }
  async function remove(name: string) {
    if (
      !(await confirm(
        "保存账号停用设置",
        `将从账号列表中移除 ${name}。应用列表后才会停止连接，缓存与历史会继续保留。`,
      ))
    )
      return;
    setBusy(true);
    try {
      await client.request("/topology", "POST", {
        revision: s.topology.revision,
        plan: {
          ...s.topology.saved,
          accounts: s.topology.saved.accounts.filter((a) => a.name !== name),
        },
      });
      toast("停用设置已保存，等待应用账号列表。");
      refresh();
    } catch (e) {
      toast((e as Error).message, true);
    } finally {
      setBusy(false);
    }
  }
  return (
    <>
      {!s.topology.applied && (
        <div className="notice warn">
          <span>
            <strong>账号列表有未应用的修改</strong>
            <p>保存的账号列表与运行中的实例不同，应用后生效。</p>
          </span>
          <Button variant="primary" small onClick={apply} disabled={busy}>
            应用账号列表
          </Button>
        </div>
      )}
      {s.topology.error && (
        <Notice tone="error" title="账号列表暂未完成应用">
          请检查监听端口和环境变量后重试；尚未完成退出的账号会继续保留状态所有权。
        </Notice>
      )}
      <div className="content-grid">
        <Card
          title="账号列表"
          description="最多支持 16 个独立账号实例"
          raw
          action={
            <Button
              small
              variant="primary"
              icon="plus"
              onClick={() => setAdding(true)}
              disabled={s.topology.saved.accounts.length >= 16}
            >
              添加账号
            </Button>
          }
        >
          {s.accounts.length ? (
            s.accounts.map((a, i) => (
              <div className="account-row" key={a.name}>
                <Avatar
                  online={a.status.state === "connected"}
                  purple={i % 2 === 1}
                />
                <div className="account-info">
                  <div className="account-name">
                    {a.name}
                    <StateBadge state={a.status.state} />
                  </div>
                  <div className="account-sub mono">{a.onebot_address}</div>
                </div>
                <Button
                  small
                  variant={account?.name === a.name ? "soft" : ""}
                  onClick={() => select(a.name)}
                >
                  {account?.name === a.name ? "当前账号" : "查看详情"}
                </Button>
              </div>
            ))
          ) : (
            <Empty
              title="还没有账号"
              description="添加一个账号实例，开始连接微信。"
              icon="users"
            />
          )}
          {s.topology.saved.accounts
            .filter((a) => !s.accounts.some((r) => r.name === a.name))
            .map((a) => (
              <div className="account-row" key={a.name}>
                <Avatar purple />
                <div className="account-info">
                  <div className="account-name">
                    {a.name}
                    <Badge tone="warn">等待应用</Badge>
                  </div>
                  <div className="account-sub">新账号已保存，尚未启动</div>
                </div>
                <Button small onClick={apply} disabled={busy}>
                  应用列表
                </Button>
              </div>
            ))}
          {s.topology.retiring?.map((a) => (
            <div className="account-row" key={`retiring-${a.name}`}>
              <div className="account-info">
                <div className="account-name">
                  {a.name}
                  <Badge tone="warn">正在退出</Badge>
                </div>
                <div className="account-sub">
                  等待已有操作结束后释放账号资源
                </div>
              </div>
            </div>
          ))}
        </Card>
        <div className="stack">
          {account ? (
            <Card
              title="账号详情"
              action={<StateBadge state={account.status.state} />}
            >
              <div className="account-detail">
                <Avatar online={account.status.state === "connected"} />
                <div>
                  <h2>{account.name}</h2>
                  <span className="hint">独立微信账号实例</span>
                </div>
              </div>
              <div className="metric-pair">
                <div>
                  <strong>{count(account.status.contact_count)}</strong>
                  <span>已观察联系人</span>
                </div>
                <div>
                  <strong>
                    {count(account.status.metrics?.unique_messages)}
                  </strong>
                  <span>当前缓存消息</span>
                </div>
                <div>
                  <strong>{count(account.status.send_calls)}</strong>
                  <span>发送调用</span>
                </div>
              </div>
              <StatusLine label="OneBot 地址">
                <span className="mono">{account.onebot_address}</span>
              </StatusLine>
              <StatusLine label="会话校验">
                <StateBadge state={account.status.session_state} />
              </StatusLine>
              <StatusLine label="最近同步">
                {time(account.status.last_sync_at)}
              </StatusLine>
              <StatusLine label="缓存恢复">
                {account.status.restored_from_checkpoint
                  ? "已从缓存恢复"
                  : "尚未从缓存恢复"}
              </StatusLine>
              {account.status.error_category && (
                <Notice tone="error" title="账号当前存在异常">
                  请检查登录状态；必要时重新扫码或导入加密凭证。
                </Notice>
              )}
              <div className="form-actions">
                <Button icon="key" onClick={() => navigate("login")}>
                  登录管理
                </Button>
                <Button
                  icon="power"
                  onClick={() =>
                    operate(
                      account.status.state === "connected"
                        ? "offline"
                        : "online",
                    )
                  }
                  disabled={busy}
                >
                  {account.status.state === "connected"
                    ? "切为离线"
                    : "恢复连接"}
                </Button>
              </div>
              <div className="feature-note">
                <span>
                  停用账号会保留缓存及编号。
                  <button
                    className="btn link small"
                    onClick={() => remove(account.name)}
                    disabled={
                      busy ||
                      !s.topology.saved.accounts.some(
                        (a) => a.name === account.name,
                      )
                    }
                  >
                    保存停用设置
                  </button>
                </span>
              </div>
            </Card>
          ) : (
            <Card title="账号详情">
              <Empty
                title="选择一个账号"
                description="在左侧选择账号查看会话与同步状态。"
              />
            </Card>
          )}
          <Notice title="账号实例与微信身份">
            实例用于隔离数据。更换微信账号时，请创建新的实例，避免混用登录缓存。
          </Notice>
        </div>
      </div>
      {adding && <AddAccount onClose={() => setAdding(false)} />}
    </>
  );
}
