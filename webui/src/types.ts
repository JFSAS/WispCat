export interface ConnectionStatus {
  name: string;
  kind: string;
  state: string;
  error?: string;
  reconnects?: number;
  since?: number;
  cursor?: number;
}
export interface RuntimeStatus {
  state: string;
  mode: string;
  detail?: string;
  error_category?: string;
  session_state?: string;
  last_sync_at?: number;
  authenticated_at?: number;
  restored_from_checkpoint?: boolean;
  contact_count?: number;
  sync_calls?: number;
  send_calls?: number;
  login_source?: string;
  metrics?: { unique_messages?: number };
  connection?: { source?: string; stage?: string };
}
export interface RuntimeAccount {
  name: string;
  status: RuntimeStatus;
  connections: ConnectionStatus[];
  onebot_address: string;
}
export interface Reverse {
  name: string;
  url: string;
  token_env: string;
  role?: string;
}
export interface Post {
  name: string;
  url: string;
  secret_env: string;
}
export interface AccountConfig {
  name: string;
  instance: string;
  mode: string;
  listen: string;
  token_env: string;
  reverse?: Reverse[] | null;
  http_post?: Post[] | null;
}
export interface ManagementPlan {
  version: number;
  listen: string;
  token_env: string;
}
export interface ConfigurationPlan {
  version: number;
  management: ManagementPlan;
  accounts: AccountConfig[];
}
export interface ConfigurationView {
  saved: ConfigurationPlan;
  revision: string;
  persistence: string;
  error?: string;
}
export interface TopologyView {
  saved: { version: number; accounts: AccountConfig[] };
  revision: string;
  applied: boolean;
  error?: string;
  retiring?: { name: string; state: string }[];
}
export interface ManagementView {
  saved: ManagementPlan;
  revision: string;
  applied: boolean;
  error?: string;
  active?: { address: string };
  listener?: { address: string; state: string };
  operation?: { state: string; error?: string };
}
export interface ListenersView {
  revision: string;
  applied: boolean;
  error?: string;
}
export interface ConnectionPlan {
  reverse: Reverse[] | null;
  http_post: Post[] | null;
}
export interface ConnectionView {
  saved: ConnectionPlan;
  revision: string;
  applied: boolean;
  error?: string;
}
export interface ListenerPlan {
  listen: string;
  token_env: string;
}
export interface ListenerView {
  saved: ListenerPlan;
  revision: string;
  applied: boolean;
  error?: string;
  listener?: { address: string; state: string };
}
export interface LoginView {
  id?: string;
  state?: string;
  image_ready?: boolean;
  error_category?: string;
  deadline?: number;
}
export type Stats = Record<string, unknown>;
export interface Snapshot {
  accounts: RuntimeAccount[];
  topology: TopologyView;
  management: ManagementView;
  configuration: ConfigurationView;
  listeners: ListenersView;
  storage: Stats;
}
export type Route =
  "overview" | "accounts" | "login" | "connections" | "storage" | "settings";
export interface Client {
  request<T>(path: string, method?: string, body?: unknown): Promise<T>;
  image(path: string): Promise<Blob>;
  clear(): void;
}
