import { useEffect, useRef, useState, type FormEvent } from "react";
import { useDashboard, useResource } from "./context.tsx";
import { accountPath, statusLabel, validateCredential } from "./model.ts";
import {
  Button,
  Card,
  Empty,
  ErrorText,
  Field,
  Icon,
  Notice,
  StateBadge,
  StatusLine,
} from "./components.tsx";
import type { LoginView } from "./types.ts";

export function Login() {
  const {
    account,
    client,
    revision,
    demo,
    toast,
    refresh,
    confirm,
    markDirty,
    clearDirty,
  } = useDashboard();
  const [tab, setTab] = useState<"file" | "qr">("file"),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [filename, setFilename] = useState(""),
    [image, setImage] = useState(""),
    [submitted, setSubmitted] = useState(false);
  const formRef = useRef<HTMLFormElement>(null);
  const name = account?.name || "";
  const resource = useResource<LoginView>(
    () =>
      name
        ? client.request<LoginView>(accountPath(name, "login"))
        : Promise.resolve({}),
    [client, name, revision],
  );
  const login = resource.data;
  useEffect(() => {
    if (!name || !submitted || account?.status.state === "connected") return;
    const id = setInterval(resource.reload, 2500);
    return () => clearInterval(id);
  }, [name, submitted, account?.status.state]);
  useEffect(() => {
    setError("");
    setSubmitted(false);
    setFilename("");
    formRef.current?.reset();
  }, [name]);
  useEffect(() => {
    let active = true,
      url = "";
    setImage("");
    if (name && login?.image_ready && login.id)
      client
        .image(
          accountPath(name, `login/image?id=${encodeURIComponent(login.id)}`),
        )
        .then((blob) => {
          url = URL.createObjectURL(blob);
          if (active) setImage(url);
          else URL.revokeObjectURL(url);
        })
        .catch((e) => {
          if (active) setError((e as Error).message);
        });
    return () => {
      active = false;
      if (url) URL.revokeObjectURL(url);
    };
  }, [client, name, login?.id, login?.image_ready]);
  if (!account)
    return (
      <Card title="登录微信">
        <Empty
          title="先添加一个账号"
          description="账号实例创建并应用后，即可在这里登录。"
          icon="users"
        />
      </Card>
    );
  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget,
      values = new FormData(form);
    const file = values.get("file"),
      pass = String(values.get("passphrase") || "");
    try {
      validateCredential(file instanceof File ? file : undefined, pass);
    } catch (e) {
      setError((e as Error).message);
      return;
    }
    setBusy(true);
    setError("");
    try {
      await client.request(accountPath(name, "login/import"), "POST", values);
      setSubmitted(true);
      toast("上传请求已接受，等待微信鉴权与首次同步。");
      refresh();
      resource.reload();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      // Never retain the file/password after an upload attempt, including errors.
      form.reset();
      clearDirty();
      setFilename("");
      setBusy(false);
    }
  }
  async function action(path: string) {
    if (
      (path === "online" || path === "login/start") &&
      !(await confirm(
        path === "online" ? "使用缓存连接微信" : "申请微信登录二维码",
        path === "online"
          ? "将使用此账号实例中已有的缓存连接微信。"
          : "将向微信申请新的登录二维码，请使用对应账号扫码并确认。",
      ))
    )
      return;
    setError("");
    setBusy(true);
    try {
      await client.request(accountPath(name, path), "POST", {});
      setSubmitted(path !== "login/cancel");
      toast(
        path === "login/cancel"
          ? "扫码已取消。"
          : "请求已接受，正在更新登录状态。",
      );
      resource.reload();
      refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="content-grid">
      <div className="stack">
        <Card
          title="登录微信"
          description={`当前账号：${account.name}`}
          action={<StateBadge state={account.status.state} />}
          raw
        >
          <div className="tabs" role="tablist" aria-label="登录方式">
            <button
              className={`tab ${tab === "file" ? "active" : ""}`}
              role="tab"
              aria-selected={tab === "file"}
              aria-controls="file-login"
              onClick={() => setTab("file")}
            >
              凭证文件登录
            </button>
            <button
              className={`tab ${tab === "qr" ? "active" : ""}`}
              role="tab"
              aria-selected={tab === "qr"}
              aria-controls="qr-login"
              onClick={() => setTab("qr")}
            >
              二维码登录
            </button>
          </div>
          <div className="card-body">
            <div className="login-state">
              <Icon
                name={account.status.state === "connected" ? "shield" : "key"}
              />
              <div>
                <h3>
                  {account.status.state === "connected"
                    ? "微信会话已连接"
                    : "连接你的微信账号"}
                </h3>
                <p>
                  {account.status.state === "connected"
                    ? "当前账号已完成鉴权与同步，可继续使用已有会话。"
                    : "扫码登录，或上传由 Mac 提取工具生成的加密凭证。"}
                </p>
              </div>
            </div>
            {submitted && account.status.state !== "connected" && (
              <Notice title="登录请求已提交">
                {statusLabel(login?.state)}
                。只有微信鉴权与首次同步完成后，账号才会显示为已连接。
              </Notice>
            )}
            <div id="file-login" role="tabpanel" hidden={tab !== "file"}>
              <form
                ref={formRef}
                onSubmit={submit}
                onChange={markDirty}
                className="upload-form"
              >
                <label className="upload-zone">
                  <Icon name={filename ? "file" : "upload"} />
                  <strong>{filename || "点击选择加密凭证文件"}</strong>
                  <span className="hint">.wxlogin 格式 · 最大 1 MiB</span>
                  <input
                    name="file"
                    type="file"
                    accept=".wxlogin"
                    required
                    aria-label="加密凭证文件"
                    disabled={busy}
                    onChange={(e) =>
                      setFilename(e.target.files?.[0]?.name || "")
                    }
                  />
                </label>
                <Field
                  label="凭证导出密码"
                  hint="输入在 Mac 上导出文件时设置的密码，至少 16 字节。"
                >
                  <input
                    name="passphrase"
                    type="password"
                    autoComplete="off"
                    required
                    placeholder="输入导出密码"
                    disabled={busy}
                  />
                </Field>
                <div className="form-actions">
                  <span className="hint">
                    文件与密码不会写入浏览器持久存储。
                  </span>
                  <Button
                    type="submit"
                    variant="primary"
                    icon="upload"
                    disabled={busy}
                  >
                    {busy ? "正在上传…" : "上传凭证登录"}
                  </Button>
                </div>
              </form>
            </div>
            <div
              id="qr-login"
              role="tabpanel"
              hidden={tab !== "qr"}
              className="qr-body"
            >
              <div className="qr-box">
                {image ? (
                  <img src={image} alt="微信登录二维码" />
                ) : (
                  <Icon name="qr" />
                )}
              </div>
              <p>
                {image
                  ? "请使用对应微信账号扫码，并在手机上确认。"
                  : demo
                    ? "演示模式不申请真实微信二维码。"
                    : "点击申请二维码，等待微信返回后扫码登录。"}
              </p>
              <Button
                variant="primary"
                icon="qr"
                onClick={() => action("login/start")}
                disabled={busy}
              >
                申请二维码
              </Button>
              {submitted && (
                <Button onClick={() => action("login/cancel")} disabled={busy}>
                  取消扫码
                </Button>
              )}
              <Button variant="link" icon="refresh" onClick={resource.reload}>
                刷新二维码状态
              </Button>
            </div>
            <ErrorText error={error || resource.error} />
            {login?.error_category && (
              <ErrorText error="本次登录未完成，请检查凭证或重新申请二维码。" />
            )}
          </div>
        </Card>
        <Notice title="选择对应的微信账号">
          不同微信账号应使用不同实例。同一账号重新导入时会继续使用原缓存命名空间、消息编号和历史。
        </Notice>
      </div>
      <div className="stack">
        <Card title="在 Mac 上准备凭证" icon="file">
          <div className="instruction">
            <div className="instruction-item">
              <div>
                <h3>运行凭证提取工具</h3>
                <p>使用已核实的微信 4.1.13（269602）安装，生成加密登录文件。</p>
                <code className="code-line">
                  wispcat-credentials --app /Applications/WeChat.app --out
                  wispcat-login.wxlogin
                </code>
              </div>
            </div>
            <div className="instruction-item">
              <div>
                <h3>设置导出密码</h3>
                <p>
                  启动工具前设置 WISPCAT_CREDENTIAL_PASSPHRASE，密码至少 16
                  字节。
                </p>
              </div>
            </div>
            <div className="instruction-item">
              <div>
                <h3>在此上传并等待连接</h3>
                <p>
                  文件和密码通过当前管理连接提交，完成鉴权与首次同步后生效。
                </p>
              </div>
            </div>
          </div>
          <div className="feature-note">
            <Icon name="shield" />
            <span>
              凭证文件包含登录所需的敏感材料，请勿公开分享；远程访问请使用 SSH
              转发。
            </span>
          </div>
        </Card>
        <Card title="已有登录缓存" description="运行时可直接恢复自己的加密缓存">
          <StatusLine label="当前会话">
            <StateBadge state={account.status.session_state} />
          </StatusLine>
          <StatusLine label="缓存来源">
            {account.status.restored_from_checkpoint
              ? "已恢复本机缓存"
              : "尚未恢复缓存"}
          </StatusLine>
          <div className="form-actions">
            <Button
              icon="refresh"
              onClick={() => action("online")}
              disabled={busy}
            >
              从缓存恢复连接
            </Button>
          </div>
        </Card>
      </div>
    </div>
  );
}
