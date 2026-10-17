import {
  cloneElement,
  type ReactElement,
  useEffect,
  useId,
  useRef,
  type ReactNode,
  type ButtonHTMLAttributes,
} from "react";
import { createPortal } from "react-dom";
import { paths } from "./icon-paths.js";
import { statusLabel, statusTone } from "./model.ts";

export function Icon({
  name,
  className = "",
}: {
  name: string;
  className?: string;
}) {
  // Only the fixed, repository-owned path dictionary is inserted here.
  return (
    <svg
      className={`icon ${className}`}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.65"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      dangerouslySetInnerHTML={{
        __html: paths[name as keyof typeof paths] || paths.info,
      }}
    />
  );
}
export function Cat({ className = "" }: { className?: string }) {
  return (
    <svg
      className={`cat-mark ${className}`}
      viewBox="0 0 48 48"
      fill="none"
      aria-hidden="true"
    >
      <path
        d="M10 23 8 8l13 8a25 25 0 0 1 6 0l13-8-2 15c5 13-3 20-14 20S5 36 10 23Z"
        fill="currentColor"
      />
      <path
        d="M17 28h1m12 0h1m-10 5 3 2 3-2"
        stroke="var(--cat-face,#fff)"
        strokeWidth="3"
        strokeLinecap="round"
      />
    </svg>
  );
}
export function Button({
  children,
  variant = "",
  small = false,
  icon,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: string;
  small?: boolean;
  icon?: string;
}) {
  return (
    <button
      type="button"
      className={`btn ${variant} ${small ? "small" : ""}`}
      {...props}
    >
      {icon && <Icon name={icon} />} {children}
    </button>
  );
}
export function Badge({
  children,
  tone = "",
  dot = false,
}: {
  children: ReactNode;
  tone?: string;
  dot?: boolean;
}) {
  return (
    <span className={`badge ${tone}`}>
      {dot && <span className="dot" />}
      {children}
    </span>
  );
}
export function StateBadge({ state }: { state?: string }) {
  return (
    <Badge tone={statusTone(state)} dot>
      {statusLabel(state)}
    </Badge>
  );
}
export function Card({
  title,
  description,
  action,
  children,
  footer,
  raw = false,
  icon,
}: {
  title: string;
  description?: string;
  action?: ReactNode;
  children: ReactNode;
  footer?: ReactNode;
  raw?: boolean;
  icon?: string;
}) {
  return (
    <section className="card">
      <div className="card-head">
        <div>
          <h2>
            {icon && <Icon name={icon} />} {title}
          </h2>
          {description && <p>{description}</p>}
        </div>
        {action}
      </div>
      {raw ? children : <div className="card-body">{children}</div>}
      {footer && <div className="card-foot">{footer}</div>}
    </section>
  );
}
export function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: ReactNode;
}) {
  const id = useId();
  const control = children as ReactElement<{
    id?: string;
    "aria-describedby"?: string;
  }>;
  return (
    <div className="field">
      <label htmlFor={id}>{label}</label>
      {cloneElement(control, {
        id,
        "aria-describedby": hint ? `${id}-hint` : undefined,
      })}
      {hint && (
        <small className="hint" id={`${id}-hint`}>
          {hint}
        </small>
      )}
    </div>
  );
}
export function Notice({
  children,
  tone = "",
  title,
}: {
  children?: ReactNode;
  tone?: string;
  title?: string;
}) {
  return (
    <div
      className={`notice ${tone}`}
      role={tone === "error" ? "alert" : undefined}
    >
      <Icon name={tone === "warn" || tone === "error" ? "alert" : "info"} />
      <div>
        {title && <strong>{title}</strong>}
        {children && <p>{children}</p>}
      </div>
    </div>
  );
}
export function ErrorText({ error }: { error?: string | null }) {
  return error ? (
    <p className="error-text" role="alert">
      {error}
    </p>
  ) : null;
}
export function Empty({
  title,
  description,
  icon = "folder",
  children,
}: {
  title: string;
  description: string;
  icon?: string;
  children?: ReactNode;
}) {
  return (
    <div className="empty">
      <Icon name={icon} />
      <h3>{title}</h3>
      <p>{description}</p>
      {children}
    </div>
  );
}
export function Loading() {
  return (
    <div className="skeleton" role="status">
      <span className="loader" />
      正在读取运行时状态…
    </div>
  );
}
export function Stat({
  label,
  value,
  unit,
  note,
  icon,
  tone = "",
  positive = false,
}: {
  label: string;
  value: ReactNode;
  unit?: string;
  note: string;
  icon: string;
  tone?: string;
  positive?: boolean;
}) {
  return (
    <div className="stat-card">
      <div className="stat-top">
        {label}
        <span className={`stat-icon ${tone}`}>
          <Icon name={icon} />
        </span>
      </div>
      <div
        className={`stat-number ${typeof value === "string" && value.length > 5 ? "word" : ""}`}
      >
        {value}
        {unit && <span className="unit">{unit}</span>}
      </div>
      <div className={`stat-caption ${positive ? "positive" : ""}`}>
        {positive && <Icon name="check" />}
        {note}
      </div>
    </div>
  );
}
export function StatusLine({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="status-line">
      <span>{label}</span>
      <span>{children}</span>
    </div>
  );
}
export function Avatar({
  online = false,
  purple = false,
}: {
  online?: boolean;
  purple?: boolean;
}) {
  return (
    <span className={`avatar ${purple ? "purple" : ""}`}>
      <Cat />
      <i className={`presence ${online ? "" : "off"}`} />
    </span>
  );
}
export function Modal({
  title,
  children,
  footer,
  onClose,
}: {
  title: string;
  children: ReactNode;
  footer?: ReactNode;
  onClose: () => void;
}) {
  const ref = useRef<HTMLDialogElement>(null),
    titleId = useId();
  useEffect(() => {
    const dialog = ref.current;
    dialog?.showModal();
    return () => dialog?.close();
  }, []);
  return createPortal(
    <dialog
      ref={ref}
      aria-labelledby={titleId}
      onCancel={(e) => {
        e.preventDefault();
        onClose();
      }}
    >
      <div className="dialog-head">
        <h2 id={titleId}>{title}</h2>
        <button className="icon-btn" aria-label="关闭对话框" onClick={onClose}>
          <Icon name="close" />
        </button>
      </div>
      <div className="dialog-body">{children}</div>
      {footer && <div className="dialog-foot">{footer}</div>}
    </dialog>,
    document.body,
  );
}
