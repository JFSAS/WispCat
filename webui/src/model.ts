import type { ConnectionPlan, Reverse, Post } from "./types.ts";

export const escapeName = (name: string) => encodeURIComponent(name);
export const accountPath = (name: string, action: string) =>
  `/accounts/${escapeName(name)}/${action}`;
export const count = (n: unknown) =>
  typeof n === "number" && Number.isFinite(n) ? n.toLocaleString("zh-CN") : "—";
export function bytes(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0)
    return "—";
  if (value === 0) return "0 B";
  const unit = Math.min(3, Math.floor(Math.log(value) / Math.log(1024)));
  return `${(value / 1024 ** unit).toLocaleString("en-US", { maximumFractionDigits: unit ? 1 : 0 })} ${["B", "KiB", "MiB", "GiB"][unit]}`;
}
export function time(value: unknown): string {
  if (typeof value !== "number" || value <= 0) return "暂无记录";
  return new Date(value * 1000).toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}
export function statusLabel(state?: string): string {
  return (
    (
      {
        connected: "已连接",
        offline: "离线",
        logging_in: "登录中",
        syncing: "同步中",
        recovering: "恢复中",
        connecting: "连接中",
        reconnecting: "重连中",
        disconnected: "未连接",
        waiting_account: "等待账号登录",
        waiting_confirmation: "待确认",
        authenticating: "鉴权中",
        degraded: "连接异常",
        idle: "未开始",
        failed: "失败",
        error: "异常",
        scope_changed: "账号已变更",
        waiting: "等待扫码",
        waiting_scan: "等待扫码",
        scanned: "待确认",
        waiting_confirm: "待确认",
        requesting: "申请中",
        expired: "已过期",
        cancelled: "已取消",
        confirmed: "已确认",
        authenticated: "已鉴权",
        validated: "已验证",
        unverified: "未验证",
        listening: "监听中",
        stopped: "已停止",
      } as Record<string, string>
    )[state || ""] || (state ? "处理中" : "未开始")
  );
}
export const statusTone = (state?: string): "" | "good" | "warn" | "error" =>
  ["connected", "validated", "authenticated", "listening"].includes(state || "")
    ? "good"
    : ["failed", "error", "scope_changed", "degraded"].includes(state || "")
      ? "error"
      : [
            "logging_in",
            "syncing",
            "recovering",
            "connecting",
            "reconnecting",
            "waiting_account",
            "waiting_confirmation",
            "authenticating",
            "requesting",
            "waiting_scan",
            "waiting_confirm",
            "scanned",
          ].includes(state || "")
        ? "warn"
        : "";
export function validateListen(value: string): string {
  const match = /^(127(?:\.\d{1,3}){3}|\[::1\]):(\d{1,5})$/.exec(value);
  if (
    !match ||
    Number(match[2]) > 65535 ||
    (match[1] !== "[::1]" && match[1].split(".").some((x) => Number(x) > 255))
  )
    throw new Error(
      "请输入回环监听地址，例如 127.0.0.1:5700，端口范围为 0–65535。",
    );
  return value;
}
export function validateEnv(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value))
    throw new Error("请输入有效的环境变量名，例如 WISPCAT_ONEBOT_TOKEN。");
  return value;
}
export function validateName(value: string): string {
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(value))
    throw new Error("名称须为 1–64 位英文字母、数字、下划线或短横线。");
  return value;
}
export type ConnectionDraft = {
  kind: "reverse" | "http_post";
  name: string;
  url: string;
  token_env: string;
  role: string;
};
export function connectionFromDraft(draft: ConnectionDraft): Reverse | Post {
  validateName(draft.name);
  validateEnv(draft.token_env);
  let url: URL;
  try {
    url = new URL(draft.url);
  } catch {
    throw new Error("请输入完整的连接地址。");
  }
  if (
    !(
      draft.kind === "reverse" ? ["ws:", "wss:"] : ["http:", "https:"]
    ).includes(url.protocol) ||
    !url.host ||
    url.username ||
    url.password ||
    url.hash
  )
    throw new Error("地址协议不匹配，且不能包含用户名、密码或片段。");
  if (draft.kind === "reverse") {
    if (!["Universal", "API", "Event"].includes(draft.role))
      throw new Error("请选择有效的连接角色。");
    return {
      name: draft.name,
      url: draft.url,
      token_env: draft.token_env,
      role: draft.role,
    };
  }
  return { name: draft.name, url: draft.url, secret_env: draft.token_env };
}
export function updateConnection(
  plan: ConnectionPlan,
  draft: ConnectionDraft,
  original?: { kind: "reverse" | "http_post"; index: number },
): ConnectionPlan {
  const next = structuredClone(plan);
  next.reverse ||= [];
  next.http_post ||= [];
  if (original) {
    if (!next[original.kind]![original.index])
      throw new Error("连接配置已变更，请刷新后重新编辑。");
    next[original.kind]!.splice(original.index, 1);
  }
  if ([...next.reverse, ...next.http_post].some((c) => c.name === draft.name))
    throw new Error("连接名称不能重复。");
  const value = connectionFromDraft(draft);
  if (draft.kind === "reverse")
    next.reverse.splice(
      original?.kind === "reverse" ? original.index : next.reverse.length,
      0,
      value as Reverse,
    );
  else
    next.http_post.splice(
      original?.kind === "http_post" ? original.index : next.http_post.length,
      0,
      value as Post,
    );
  if (next.reverse.length > 16 || next.http_post.length > 16)
    throw new Error("每种连接最多配置 16 个。");
  return next;
}
export function safeManagementURL(address: string): string | null {
  try {
    validateListen(address);
    const url = new URL(`http://${address}/`);
    if (url.port === "0") return null;
    return url.href;
  } catch {
    return null;
  }
}
export function validateCredential(file: File | undefined, passphrase: string) {
  if (
    !file ||
    !file.name.toLowerCase().endsWith(".wxlogin") ||
    file.size > 1048576 ||
    file.size === 0
  )
    throw new Error("请选择不超过 1 MiB 的 .wxlogin 加密凭证文件。");
  const length = new TextEncoder().encode(passphrase).length;
  if (length < 16 || length > 4096)
    throw new Error("导出密码须为 16–4096 字节。");
}
