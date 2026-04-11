/** 圆形旋转指示器，0.8s linear infinite（样式见 index.css） */
export function UiSpinner({ tone = "muted" }) {
  return (
    <span
      className={tone === "primary" ? "ui-spinner ui-spinner--primary" : "ui-spinner ui-spinner--muted"}
      aria-hidden
    />
  );
}
