import { useDashboard } from "./context.tsx";
import {
  Avatar,
  Badge,
  Button,
  Card,
  Cat,
  Empty,
  Icon,
  Stat,
  StateBadge,
  StatusLine,
} from "./components.tsx";
import { count, time } from "./model.ts";

export function Overview() {
  const { snapshot: s, navigate, select, account } = useDashboard();
  const online = s.accounts.filter(
    (a) => a.status.state === "connected",
  ).length;
  const connections = s.accounts.flatMap((a) => a.connections || []),
    connected = connections.filter((c) => c.state === "connected").length;
  const pending = [
    s.management.applied,
    s.topology.applied,
    s.listeners.applied,
  ].filter((x) => !x).length;
  const hasAccount = s.accounts.length > 0,
    hasOnline = online > 0,
    hasConnection = connections.length > 0;
  const steps = [
    {
      title: "添加微信账号",
      description: hasAccount
        ? `${s.accounts.length} 个账号实例已就绪`
        : "建立一个独立的微信账号实例",
      done: hasAccount,
      route: "accounts" as const,
    },
    {
      title: "登录并连接微信",
      description: hasOnline
        ? "微信会话已连接"
        : "扫码登录，或导入 Mac 加密凭证",
      done: hasOnline,
      route: "login" as const,
    },
    {
      title: "连接机器人框架",
      description: connected
        ? "机器人框架连接正常"
        : "配置 AstrBot 或 NoneBot 的连接地址",
      done: connected > 0,
      route: "connections" as const,
    },
  ];
  return (
    <>
      <div className="stats-grid">
        <Stat
          label="微信账号"
          value={online}
          unit={`/ ${s.accounts.length}`}
          note={online ? "微信会话已连接" : "等待账号登录"}
          icon="users"
          positive={online > 0}
        />
        <Stat
          label="机器人连接"
          value={connected}
          unit="个"
          note={
            connections.length
              ? `已配置 ${connections.length} 个下游连接`
              : "还没有配置下游框架"
          }
          icon="plug"
          tone="purple"
        />
        <Stat
          label="全局配置"
          value={pending ? "待应用" : "已同步"}
          note={
            pending
              ? `${pending} 项全局设置等待应用`
              : "账号、监听与管理设置已应用"
          }
          icon="shield"
          tone="blue"
          positive={!pending}
        />
        <Stat
          label="运行时版本"
          value="0.1.0"
          unit="alpha.2"
          note="OneBot v11 · 微信协议运行时"
          icon="server"
          tone="amber"
        />
      </div>
      <div className="content-grid">
        <div className="stack">
          <Card
            title="微信账号"
            description="各账号独立维护会话、编号和消息历史"
            raw
            action={
              <Button
                small
                variant="link"
                icon="arrow"
                onClick={() => navigate("accounts")}
              >
                管理账号
              </Button>
            }
            footer={
              <>
                <span className="hint">
                  {online} 个在线 · {s.accounts.length - online} 个离线
                </span>
                <Badge tone="good">独立账号空间</Badge>
              </>
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
                    <div className="account-sub">
                      OneBot{" "}
                      <span className="mono">
                        {a.onebot_address || "尚未监听"}
                      </span>
                    </div>
                  </div>
                  <Button
                    small
                    onClick={() => {
                      select(a.name);
                      navigate(
                        a.status.state === "connected" ? "accounts" : "login",
                      );
                    }}
                    icon={a.status.state === "connected" ? "chevron" : "key"}
                  >
                    {a.status.state === "connected" ? "查看详情" : "去登录"}
                  </Button>
                </div>
              ))
            ) : (
              <Empty
                title="从第一个账号开始"
                description="添加账号后，可选择扫码登录或导入加密凭证。"
                icon="users"
              >
                <Button variant="primary" onClick={() => navigate("accounts")}>
                  添加账号
                </Button>
              </Empty>
            )}
          </Card>
          <Card
            title="机器人连接"
            description="将微信消息交给你熟悉的机器人框架"
            raw
            action={
              <Button
                small
                variant="link"
                icon="arrow"
                onClick={() => navigate("connections")}
              >
                连接配置
              </Button>
            }
          >
            <div className="card-body">
              <div className="flow">
                <div className="flow-node">
                  <span className={`flow-icon ${hasOnline ? "active" : ""}`}>
                    <Icon name="users" />
                  </span>
                  微信账号
                </div>
                <Icon name="arrow" className="arrow" />
                <div className="flow-node">
                  <span className="flow-icon active">
                    <Cat />
                  </span>
                  WispCat
                </div>
                <Icon name="arrow" className="arrow" />
                <div className="flow-node">
                  <span className={`flow-icon ${connected ? "active" : ""}`}>
                    <Icon name="plug" />
                  </span>
                  机器人框架
                </div>
              </div>
            </div>
            {hasConnection ? (
              s.accounts.flatMap((a) =>
                (a.connections || []).map((c, i) => (
                  <div className="connection-row" key={`${a.name}-${c.name}`}>
                    <span className={`framework-icon ${i % 2 ? "blue" : ""}`}>
                      {c.name.toLowerCase().includes("astrbot") ? "A" : "N"}
                    </span>
                    <div className="connection-info">
                      <h3>
                        {c.name}
                        <Badge>
                          {c.kind === "httpClient"
                            ? "HTTP 上报"
                            : "反向 WebSocket"}
                        </Badge>
                      </h3>
                      <p>
                        {a.name} ·{" "}
                        {c.error
                          ? "连接异常，请在连接页检查配置"
                          : "OneBot v11 动作与事件"}
                      </p>
                    </div>
                    <StateBadge state={c.state} />
                  </div>
                )),
              )
            ) : (
              <div className="card-body">
                <div className="feature-note">
                  <Icon name="plug" />
                  <span>
                    支持 AstrBot、NoneBot 和自定义 OneBot
                    程序。添加一个连接，即可开始接收微信事件。
                  </span>
                </div>
              </div>
            )}
          </Card>
        </div>
        <div className="stack">
          <section className="intro-card">
            <div className="eyebrow">HELLO, WISPCAT</div>
            <h2>让连接，轻一点。</h2>
            <p>
              微信通道交给 WispCat，
              <br />
              机器人逻辑交给你喜欢的框架。
            </p>
            <Button
              small
              variant="soft"
              icon="key"
              onClick={() => navigate("login")}
            >
              管理登录
            </Button>
            <Cat className="intro-cat" />
          </section>
          <Card
            title="接入指南"
            action={<Badge>{steps.filter((x) => x.done).length} / 3</Badge>}
          >
            {steps.map((step, i) => (
              <div className="step" key={step.title}>
                <span className={`step-number ${step.done ? "done" : ""}`}>
                  {step.done ? <Icon name="check" /> : i + 1}
                </span>
                <div className="step-content">
                  <h3>{step.title}</h3>
                  <p>{step.description}</p>
                </div>
                <button
                  className="icon-btn"
                  aria-label={step.title}
                  onClick={() => navigate(step.route)}
                >
                  <Icon name="chevron" />
                </button>
              </div>
            ))}
          </Card>
          <Card title="运行信息" icon="server">
            <StatusLine label="管理服务">
              <Badge tone="good" dot>
                已连接
              </Badge>
            </StatusLine>
            <StatusLine label="通信协议">OneBot v11</StatusLine>
            <StatusLine label="最近同步">
              {time(account?.status.last_sync_at)}
            </StatusLine>
            <StatusLine label="联系人">
              {count(account?.status.contact_count)}
            </StatusLine>
            <div className="feature-note">
              <Icon name="shield" />
              <span>
                管理令牌仅在当前页面内存中使用，不写入浏览器持久存储。
              </span>
            </div>
          </Card>
        </div>
      </div>
    </>
  );
}
