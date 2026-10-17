import {
  createContext,
  useContext,
  useEffect,
  useState,
  type DependencyList,
} from "react";
import type { Client, Snapshot, RuntimeAccount, Route } from "./types.ts";
export interface Dashboard {
  client: Client;
  demo: boolean;
  snapshot: Snapshot;
  account: RuntimeAccount | undefined;
  selected: string;
  select: (name: string) => void;
  navigate: (route: Route) => void;
  refresh: () => void;
  markDirty: () => void;
  clearDirty: () => void;
  revision: number;
  toast: (message: string, error?: boolean) => void;
  confirm: (title: string, message: string) => Promise<boolean>;
  handover: (address: string, tokenEnv: string) => void;
}
export const DashboardContext = createContext<Dashboard | null>(null);
export function useDashboard() {
  const value = useContext(DashboardContext);
  if (!value) throw new Error("dashboard context missing");
  return value;
}
export function useResource<T>(
  load: () => Promise<T>,
  deps: DependencyList,
  enabled = true,
) {
  const [data, setData] = useState<T>(),
    [error, setError] = useState(""),
    [serial, setSerial] = useState(0);
  useEffect(() => {
    let active = true;
    setData(undefined);
    setError("");
    if (!enabled) return;
    load()
      .then((value) => {
        if (active) setData(value);
      })
      .catch((e) => {
        if (active) setError(e instanceof Error ? e.message : "读取状态失败。");
      });
    return () => {
      active = false;
    };
    // Each caller explicitly supplies its request identity, account and refresh revision.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, serial, enabled]);
  return { data, error, reload: () => setSerial((x) => x + 1) };
}
