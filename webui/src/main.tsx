import React, {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FormEvent,
  type ReactNode,
} from "react";
import { createRoot } from "react-dom/client";
import { RuntimeClient, ApiError } from "./api.ts";
import { DemoClient } from "./demo.ts";
import { DashboardContext } from "./context.tsx";
import {
  Badge,
  Button,
  Cat,
  ErrorText,
  Field,
  Icon,
  Loading,
  Modal,
  Notice,
} from "./components.tsx";
import { Overview } from "./overview.tsx";
import { Accounts } from "./accounts.tsx";
import { Login } from "./login.tsx";
import { Connections } from "./connections.tsx";
import { Settings, Storage } from "./maintenance.tsx";
import type { Client, Route, Snapshot } from "./types.ts";
import "./style.css";

const navigation: {
  route: Route;
  label: string;
  icon: string;
  description: string;
}[] = [
  {
    route: "overview",
    label: "运行概览",
    icon: "grid",
    description: "一眼了解账号与机器人连接的当前状态。",
  },
  {
    route: "accounts",
    label: "账号管理",
    icon: "users",
    description: "管理独立账号实例，查看微信会话与同步状态。",
  },
  {
    route: "login",
    label: "登录管理",
    icon: "key",
    description: "选择登录方式，安全连接你的微信账号。",
  },
  {
    route: "connections",
    label: "机器人连接",
    icon: "plug",
    description: "让微信消息与机器人框架顺畅连接。",
  },
  {
    route: "storage",
    label: "存储维护",
    icon: "database",
    description: "查看存储用量，按需整理临时文件与媒体副本。",
  },
  {
    route: "settings",
    label: "系统设置",
    icon: "settings",
    description: "管理访问入口、监听配置与运行时设置。",
  },
];
const readRoute = (): Route =>
  navigation.some((n) => n.route === location.hash.slice(1))
    ? (location.hash.slice(1) as Route)
    : "overview";
function Brand() {
  return (
    <div className="brand">
      <span className="brand-mark">
        <Cat />
      </span>
      <div>
        <div className="brand-word">WispCat</div>
        <div className="brand-sub">WECHAT RUNTIME</div>
      </div>
    </div>
  );
}
function LoginScreen({ onLogin }: { onLogin: (client: Client) => void }) {
  const [error, setError] = useState(""),
    [busy, setBusy] = useState(false);
  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget,
      token = String(new FormData(form).get("token") || "").trim();
    const client = new RuntimeClient(token);
    setBusy(true);
    setError("");
    try {
      await client.request("/accounts");
      form.reset();
      onLogin(client);
    } catch (e) {
      client.clear();
      form.reset();
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="login-shell">
      <section className="login-art">
        <Brand />
        <div className="orbit" />
        <div className="login-hero">
          <Cat />
          <h1>
            微信与机器人之间，
            <br />
            一份轻巧的连接。
          </h1>
          <p>
            独立管理账号与消息通道，
            <br />
            让你熟悉的机器人框架，继续做擅长的事。
          </p>
        </div>
        <span className="login-copyright">
          WispCat · Independent WeChat Runtime
        </span>
      </section>
      <main className="login-main">
        <div className="login-box">
          <div className="eyebrow">WELCOME BACK</div>
          <h2>登录管理控制台</h2>
          <p>
            输入运行时的管理令牌，
            <br />
            查看账号、管理登录和配置机器人连接。
          </p>
          <form onSubmit={submit}>
            <Field label="管理令牌">
              <input
                type="password"
                name="token"
                required
                autoComplete="off"
                placeholder="输入 WISPCAT_ADMIN_TOKEN 的值"
                autoFocus
                disabled={busy}
              />
            </Field>
            <ErrorText error={error} />
            <Button
              type="submit"
              variant="primary"
              icon="arrow"
              disabled={busy}
            >
              {busy ? "正在验证…" : "进入控制台"}
            </Button>
          </form>
          <div className="feature-note">
            <Icon name="shield" />
            <span>
              令牌仅在当前页面内存中使用。请通过本机地址或可信的 SSH 隧道访问。
            </span>
          </div>
          <div className="login-demo">
            想先看看界面？
            <a href="?demo=1">
              进入演示模式 <span aria-hidden="true">↗</span>
            </a>
          </div>
        </div>
      </main>
    </div>
  );
}
function App() {
  const demo = new URLSearchParams(location.search).get("demo") === "1";
  const [client, setClient] = useState<Client | null>(() =>
    demo ? new DemoClient() : null,
  );
  const [dirty, setDirty] = useState(false);
  const markDirty = useCallback(() => setDirty(true), []);
  const clearDirty = useCallback(() => setDirty(false), []);
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null),
    [loadError, setLoadError] = useState(""),
    [stale, setStale] = useState(false),
    [revision, setRevision] = useState(0),
    [updated, setUpdated] = useState<Date>();
  const [route, setRoute] = useState<Route>(readRoute),
    [selected, setSelected] = useState(""),
    [mobile, setMobile] = useState(false),
    [theme, setTheme] = useState(() => {
      try {
        return localStorage.getItem("wispcat-theme") === "dark"
          ? "dark"
          : "light";
      } catch {
        return "light";
      }
    });
  const [toasts, setToasts] = useState<
      { id: number; message: string; error: boolean }[]
    >([]),
    toastCounter = useRef(0);
  const [confirmation, setConfirmation] = useState<{
      title: string;
      message: string;
      resolve: (value: boolean) => void;
    } | null>(null),
    [handover, setHandover] = useState<{ url: string; env: string } | null>(
      null,
    );
  const timers = useRef<ReturnType<typeof setTimeout>[]>([]);
  const toast = useCallback((message: string, error = false) => {
    const id = ++toastCounter.current;
    setToasts((old) => [...old.slice(-2), { id, message, error }]);
    timers.current.push(
      setTimeout(
        () => setToasts((old) => old.filter((x) => x.id !== id)),
        6500,
      ),
    );
  }, []);
  useEffect(
    () => () => {
      timers.current.forEach(clearTimeout);
    },
    [],
  );
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    try {
      localStorage.setItem("wispcat-theme", theme);
    } catch {
      /* Appearance storage is optional. */
    }
  }, [theme]);
  useEffect(() => {
    const changed = () => {
      setRoute(readRoute());
      setMobile(false);
    };
    window.addEventListener("hashchange", changed);
    return () => window.removeEventListener("hashchange", changed);
  }, []);
  const refresh = useCallback(() => {
    setDirty(false);
    setRevision((x) => x + 1);
  }, []);
  useEffect(() => {
    if (!dirty) return;
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [dirty]);
  const logout = useCallback(() => {
    client?.clear();
    setDirty(false);
    setClient(null);
    setSnapshot(null);
    setSelected("");
    setLoadError("");
    setHandover(null);
    setToasts([]);
  }, [client]);
  useEffect(() => {
    if (!client) return;
    let active = true;
    setLoadError("");
    Promise.all([
      client.request<Snapshot["accounts"]>("/accounts"),
      client.request<Snapshot["topology"]>("/topology"),
      client.request<Snapshot["management"]>("/management"),
      client.request<Snapshot["configuration"]>("/configuration"),
      client.request<Snapshot["listeners"]>("/listeners"),
      client.request<Snapshot["storage"]>("/storage"),
    ])
      .then(
        ([
          accounts,
          topology,
          management,
          configuration,
          listeners,
          storage,
        ]) => {
          if (!active) return;
          if (!configuration.saved || !topology.saved || !management.saved)
            throw new Error("配置当前不可读，请检查运行时的配置恢复状态。");
          setSnapshot({
            accounts,
            topology,
            management,
            configuration,
            listeners,
            storage,
          });
          setSelected((old) =>
            accounts.some((a) => a.name === old)
              ? old
              : accounts[0]?.name || "",
          );
          setUpdated(new Date());
          setStale(false);
        },
      )
      .catch((e) => {
        if (!active) return;
        if (e instanceof ApiError && e.status === 401) {
          logout();
          toast(e.message, true);
        } else setLoadError((e as Error).message);
      });
    return () => {
      active = false;
    };
  }, [client, revision]);
  useEffect(() => {
    if (!client || handover) return;
    let active = true,
      inFlight = false;
    const timer = setInterval(async () => {
      if (inFlight || document.hidden) return;
      inFlight = true;
      try {
        const accounts =
          await client.request<Snapshot["accounts"]>("/accounts");
        if (active) {
          setSnapshot((s) => (s ? { ...s, accounts } : s));
          setUpdated(new Date());
          setStale(false);
        }
      } catch (e) {
        if (active) {
          if (e instanceof ApiError && e.status === 401) {
            logout();
            toast(e.message, true);
          } else setStale(true);
        }
      } finally {
        inFlight = false;
      }
    }, 6000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [client, handover]);
  async function navigate(next: Route) {
    if (next === route) {
      setMobile(false);
      return;
    }
    if (
      dirty &&
      !(await confirm(
        "放弃未保存的修改？",
        "当前表单有未保存的输入，离开页面后将丢弃这些输入。",
      ))
    )
      return;
    setDirty(false);
    location.hash = next;
    setRoute(next);
    setMobile(false);
  }
  const confirm = useCallback(
    (title: string, message: string) =>
      new Promise<boolean>((resolve) =>
        setConfirmation({ title, message, resolve }),
      ),
    [],
  );
  async function select(name: string) {
    if (name === selected) return;
    if (
      dirty &&
      !(await confirm(
        "切换账号？",
        "当前表单有未保存的输入，切换后将丢弃这些输入。",
      ))
    )
      return;
    setDirty(false);
    setSelected(name);
  }
  async function requestRefresh() {
    if (
      dirty &&
      !(await confirm(
        "重新加载状态？",
        "当前表单有未保存的输入，重新加载后将丢弃这些输入。",
      ))
    )
      return;
    refresh();
  }
  async function requestLogout() {
    if (
      dirty &&
      !(await confirm(
        "退出管理控制台？",
        "退出后将丢弃当前表单中未保存的输入。",
      ))
    )
      return;
    logout();
  }
  function finishConfirm(value: boolean) {
    confirmation?.resolve(value);
    setConfirmation(null);
  }
  const page = navigation.find((n) => n.route === route)!;
  const context =
    client && snapshot
      ? {
          client,
          demo,
          snapshot,
          account: snapshot.accounts.find((a) => a.name === selected),
          selected,
          select,
          markDirty,
          clearDirty,
          navigate,
          refresh,
          revision,
          toast,
          confirm,
          handover: (url: string, env: string) => setHandover({ url, env }),
        }
      : null;
  const pages: Record<Route, ReactNode> = {
    overview: <Overview />,
    accounts: <Accounts />,
    login: <Login />,
    connections: <Connections />,
    storage: <Storage />,
    settings: <Settings />,
  };
  return (
    <>
      {!client ? (
        <LoginScreen onLogin={setClient} />
      ) : (
        <>
          <div
            className={`scrim ${mobile ? "show" : ""}`}
            onClick={() => setMobile(false)}
          />
          <aside
            className={`sidebar ${mobile ? "open" : ""}`}
            aria-label="主导航"
          >
            <Brand />
            <div className="nav-label">工作空间</div>
            <nav className="nav">
              {navigation.map((n) => (
                <a
                  key={n.route}
                  href={`#${n.route}`}
                  className={`nav-link ${route === n.route ? "active" : ""}`}
                  aria-current={route === n.route ? "page" : undefined}
                  onClick={(e) => {
                    e.preventDefault();
                    void navigate(n.route);
                  }}
                >
                  <Icon name={n.icon} />
                  {n.label}
                </a>
              ))}
            </nav>
            <div className="sidebar-bottom">
              <div className="sidebar-note">
                <h3>
                  <Icon name="shield" />
                  独立运行，清晰连接
                </h3>
                <p>
                  微信协议与账号由 WispCat 维护，
                  <br />
                  机器人逻辑在外部框架中运行。
                </p>
              </div>
              <div className="sidebar-foot">
                <span>
                  <span className="dot" />{" "}
                  {demo ? "本地演示" : stale ? "连接中断" : "管理控制台"}
                </span>
                <span>v0.1.0-alpha.2</span>
              </div>
            </div>
          </aside>
          <div className="shell">
            <header className="topbar">
              <div className="breadcrumb">
                <button
                  className="icon-btn mobile-menu"
                  aria-label="展开导航"
                  onClick={() => setMobile(true)}
                >
                  <Icon name="menu" />
                </button>
                <span>控制台</span>
                <Icon name="chevron" />
                <strong>{page.label}</strong>
              </div>
              <div className="topbar-actions">
                <Badge tone={demo ? "warn" : stale ? "error" : "good"} dot>
                  {demo ? "演示模式" : stale ? "服务不可达" : "运行时已连接"}
                </Badge>
                <button
                  className="icon-btn"
                  aria-label={
                    theme === "light" ? "切换深色模式" : "切换浅色模式"
                  }
                  title="切换外观"
                  onClick={() =>
                    setTheme((t) => (t === "light" ? "dark" : "light"))
                  }
                >
                  <Icon name={theme === "light" ? "moon" : "sun"} />
                </button>
                <span className="divider" />
                <div className="admin-label">
                  <span className="admin-avatar">WC</span>
                  <span>管理员</span>
                </div>
                <button
                  className="icon-btn"
                  aria-label="退出管理控制台"
                  title="退出登录"
                  onClick={requestLogout}
                >
                  <Icon name="logout" />
                </button>
              </div>
            </header>
            <main className="workspace">
              {demo && (
                <div className="demo-banner">
                  <Icon name="info" />
                  <span>
                    本地界面演示 ·
                    所有账号、连接与统计均为示例数据，操作不会连接微信。
                  </span>
                  <a href="/">退出演示</a>
                </div>
              )}
              <div className="page-heading">
                <div>
                  <div className="eyebrow">
                    {route === "overview"
                      ? "WORKSPACE OVERVIEW"
                      : "WISPCAT CONSOLE"}
                  </div>
                  <h1>{page.label}</h1>
                  <p>{page.description}</p>
                </div>
                <div className="heading-actions">
                  {snapshot &&
                    snapshot.accounts.length > 0 &&
                    route !== "overview" && (
                      <select
                        className="account-selector"
                        aria-label="当前账号"
                        value={selected}
                        onChange={(e) => void select(e.target.value)}
                      >
                        {snapshot.accounts.map((a) => (
                          <option key={a.name} value={a.name}>
                            {a.name}
                          </option>
                        ))}
                      </select>
                    )}
                  <Button icon="refresh" onClick={requestRefresh}>
                    刷新状态
                  </Button>
                </div>
              </div>
              {stale && (
                <div className="stale-strip" role="alert">
                  无法更新运行时状态，以下是最后一次读取的结果。请检查服务是否仍在运行。
                </div>
              )}
              {loadError && (
                <Notice tone="error" title="状态加载失败">
                  {loadError}
                  <Button small onClick={requestRefresh}>
                    重新加载
                  </Button>
                </Notice>
              )}
              {handover && (
                <Notice tone="warn" title="管理接口切换请求已接受">
                  请在新地址重新输入 {handover.env} 对应的管理令牌。
                  <br />
                  <a className="btn small" href={handover.url} rel="noopener">
                    在新地址登录 <Icon name="external" />
                  </a>
                </Notice>
              )}
              {context ? (
                <DashboardContext.Provider value={context}>
                  {pages[route]}
                </DashboardContext.Provider>
              ) : (
                !loadError && <Loading />
              )}
              <footer className="page-footer">
                <span>WispCat · 独立微信协议运行时</span>
                <span>
                  {updated
                    ? `最近更新 ${updated.toLocaleTimeString("zh-CN", { hour12: false })}`
                    : "等待运行时响应"}
                </span>
              </footer>
            </main>
          </div>
        </>
      )}
      {confirmation && (
        <Modal
          title={confirmation.title}
          onClose={() => finishConfirm(false)}
          footer={
            <>
              <Button onClick={() => finishConfirm(false)}>取消</Button>
              <Button variant="primary" onClick={() => finishConfirm(true)}>
                确认操作
              </Button>
            </>
          }
        >
          <p>{confirmation.message}</p>
        </Modal>
      )}
      <div className="toasts" role="status" aria-live="polite">
        {toasts.map((t) => (
          <div key={t.id} className={`toast ${t.error ? "error" : ""}`}>
            <Icon name={t.error ? "alert" : "check"} />
            <span>{t.message}</span>
            <button
              aria-label="关闭通知"
              onClick={() =>
                setToasts((old) => old.filter((x) => x.id !== t.id))
              }
            >
              <Icon name="close" />
            </button>
          </div>
        ))}
      </div>
    </>
  );
}
createRoot(document.getElementById("app")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
