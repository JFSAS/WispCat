import type { Client } from "./types.ts";
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  constructor(status: number, code: string) {
    super(errorMessage(status, code));
    this.status = status;
    this.code = code;
  }
}
export function errorMessage(status: number, code: string): string {
  if (status === 401) return "管理令牌无效或已更新，请重新登录。";
  if (code.includes("revision_conflict") || code.endsWith("_save_failed"))
    return "保存失败：配置可能已变更，或引用的令牌环境变量不可用。请检查设置，刷新后重试。";
  const messages: Record<string, string> = {
    credential_file_invalid_or_password_incorrect:
      "凭证文件无效或导出密码不正确。",
    credential_login_unavailable:
      "当前账号暂时无法导入凭证，请确认账号身份或稍后重试。",
    native_transport_unavailable: "当前平台不支持原生微信传输。",
    listener_bind_failed: "监听端口被占用，请使用其他端口。",
    account_operation_failed: "操作未被接受，请检查账号当前状态。",
    management_token_unavailable:
      "管理令牌环境变量不可用，请在进程环境中配置后重试。",
    topology_token_unavailable: "令牌环境变量不可用，请先在进程环境中配置。",
    management_config_invalid: "管理地址或令牌环境变量不正确。",
    configuration_revision_conflict: "配置已被其他窗口修改，请刷新后重新编辑。",
    configuration_recovery_pending: "配置保存尚未完成，请先恢复配置保存。",
    authentication_required: "请输入管理令牌。",
    host_not_allowed: "管理地址不匹配，请通过与服务监听一致的地址和端口访问。",
    origin_not_allowed: "当前页面来源未获允许，请从管理服务自身的地址访问。",
  };
  return (
    messages[code] ||
    (status >= 500
      ? "运行时暂时不可用，请稍后刷新。"
      : "操作未完成，请检查配置和账号状态后重试。")
  );
}
export class RuntimeClient implements Client {
  #token: string;
  #fetch: typeof fetch;
  constructor(token: string, transport: typeof fetch = fetch) {
    this.#token = token;
    // WebKit requires the native fetch receiver to remain the window.
    this.#fetch = transport.bind(globalThis);
  }
  clear() {
    this.#token = "";
  }
  async raw(path: string, method = "GET", body?: unknown): Promise<Response> {
    if (
      !path.startsWith("/") ||
      path.startsWith("//") ||
      /[\u0000-\u0020\u007f\\]/.test(path)
    )
      throw new Error("请求地址无效。");
    if (!this.#token) throw new ApiError(401, "authentication_required");
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.#token}`,
    };
    const multipart = body instanceof FormData;
    if (body !== undefined && !multipart)
      headers["Content-Type"] = "application/json";
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);
    try {
      const response = await this.#fetch(path, {
        method,
        headers,
        body:
          body === undefined
            ? undefined
            : multipart
              ? body
              : JSON.stringify(body),
        signal: controller.signal,
        credentials: "omit",
        cache: "no-store",
        redirect: "error",
      });
      if (!response.ok) {
        const text = await response.text();
        let code = text.trim();
        try {
          const value = JSON.parse(text);
          code = value.category || value.error || code;
        } catch {
          /* Plain HTTP errors carry stable categories. */
        }
        throw new ApiError(response.status, code);
      }
      return response;
    } catch (error) {
      if (error instanceof ApiError) throw error;
      throw new Error(
        "无法完成请求，请检查管理服务是否在线；写操作超时不代表没有生效，请先刷新状态。",
      );
    } finally {
      clearTimeout(timeout);
    }
  }
  async request<T>(path: string, method = "GET", body?: unknown): Promise<T> {
    return (await this.raw(path, method, body)).json() as Promise<T>;
  }
  async image(path: string) {
    return (await this.raw(path)).blob();
  }
}
