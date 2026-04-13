import { createPortal } from "react-dom";
import { useEffect, useRef } from "react";
import { Cpu, Database, Eraser, Globe, Loader2, Play, Radar, Sparkles, Square, Upload } from "lucide-react";
import { UiSpinner } from "./UiSpinner";

function poolStatusDot(status) {
  const s = (status || "").toLowerCase();
  if (s === "idle") return "bg-emerald-400 shadow-[0_0_10px_rgba(52,211,153,0.75)]";
  if (s === "used") return "bg-sky-400 shadow-[0_0_10px_rgba(56,189,248,0.65)]";
  return "bg-rose-400 shadow-[0_0_12px_rgba(251,113,133,0.7)]";
}

function poolStatusLabel(status) {
  const s = (status || "").toLowerCase();
  if (s === "idle") return "闲置";
  if (s === "used") return "已分配";
  return "失效";
}

function countryFlagIconUrl(code) {
  const c = String(code || "").trim().toUpperCase();
  if (c.length !== 2 || !/^[A-Z]{2}$/.test(c)) return "";
  const hexA = (0x1f1e6 + (c.charCodeAt(0) - 65)).toString(16);
  const hexB = (0x1f1e6 + (c.charCodeAt(1) - 65)).toString(16);
  return `https://cdnjs.cloudflare.com/ajax/libs/twemoji/14.0.2/svg/${hexA}-${hexB}.svg`;
}

function poolCheckStatusUi(checkStatus) {
  const s = String(checkStatus || "unknown").toLowerCase();
  if (s === "ok")
    return {
      label: "正常",
      dot: "bg-emerald-400 shadow-[0_0_10px_rgba(52,211,153,0.8)]",
      cls: "text-emerald-300/95",
    };
  if (s === "dead")
    return {
      label: "失败",
      dot: "bg-rose-500 shadow-[0_0_10px_rgba(248,113,113,0.75)]",
      cls: "text-rose-300/95",
    };
  return {
    label: "未检测",
    dot: "bg-amber-400/90 shadow-[0_0_8px_rgba(251,191,36,0.35)]",
    cls: "text-slate-300/90",
  };
}

function useModalEscape(open, onClose) {
  useEffect(() => {
    if (!open) return undefined;
    const onKey = (e) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [open, onClose]);
}

/** 代理池 · 闲置代理管理 */
export function ProxyPoolGlassModal({
  open,
  onClose,
  items,
  loading,
  onReload,
  onImportClick,
  importBusy,
  dedupeBusy,
  onDedupe,
  importNotice = null,
}) {
  const panelRef = useRef(null);
  useModalEscape(open, onClose);

  if (!open || typeof document === "undefined") return null;

  return createPortal(
    <>
      <button
        type="button"
        aria-label="关闭"
        className="proxy-glass-overlay"
        onClick={onClose}
      />
      <div
        ref={panelRef}
        className="proxy-glass-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="proxy-pool-title"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <div className="proxy-glass-dialog-glow" aria-hidden />
        <div className="relative z-[1] flex max-h-[min(78vh,680px)] flex-col p-6 sm:p-7">
          <header className="mb-5 shrink-0 border-b border-white/[0.08] pb-4">
            <div className="flex items-start gap-3">
              <span className="grid h-11 w-11 shrink-0 place-items-center rounded-xl border border-cyan-400/25 bg-cyan-500/10 text-cyan-300 shadow-[0_0_20px_rgba(34,211,238,0.2)]">
                <Database className="h-5 w-5" aria-hidden />
              </span>
              <div className="min-w-0">
                <h2 id="proxy-pool-title" className="text-lg font-bold tracking-tight text-slate-50">
                  代理池
                </h2>
                <p className="mt-1 text-xs font-medium text-slate-500">闲置代理管理</p>
              </div>
            </div>
          </header>

          <div className="mb-4 flex shrink-0 flex-wrap gap-2.5">
            <button
              type="button"
              disabled={importBusy}
              onClick={onImportClick}
              className="proxy-glass-btn proxy-glass-btn--secondary inline-flex items-center justify-center gap-2 px-4 py-2.5 text-sm font-semibold disabled:cursor-not-allowed disabled:opacity-45"
            >
              {importBusy ? <Loader2 className="h-4 w-4 animate-spin" aria-hidden /> : <Upload className="h-4 w-4" aria-hidden />}
              导入代理
            </button>
            <button
              type="button"
              disabled={dedupeBusy || loading}
              onClick={onDedupe}
              className="proxy-glass-btn proxy-glass-btn--secondary inline-flex items-center justify-center gap-2 px-4 py-2.5 text-sm font-semibold disabled:cursor-not-allowed disabled:opacity-45"
            >
              {dedupeBusy ? <Loader2 className="h-4 w-4 animate-spin" aria-hidden /> : <Eraser className="h-4 w-4" aria-hidden />}
              清洗去重
            </button>
            <button
              type="button"
              disabled={loading}
              onClick={onReload}
              className="proxy-glass-btn proxy-glass-btn--ghost ml-auto text-sm font-medium text-slate-400 hover:text-cyan-200"
            >
              刷新列表
            </button>
          </div>
          <p className="mb-3 text-[10px] leading-relaxed text-slate-500">
            导入支持 <span className="font-mono text-slate-400">.txt</span> /{' '}
            <span className="font-mono text-slate-400">.text</span>：每行一条{' '}
            <span className="font-mono text-cyan-400/85">host:port@用户名:密码</span>
            ；空行与 <span className="font-mono">#</span> 注释行会跳过。同一 host:port@用户名 下密码不同会作为多条代理入库（如轮换会话）。
            亦支持原有 <span className="font-mono text-slate-400">.json</span>。
          </p>

          {importNotice?.text ? (
            <p
              role="status"
              className={`mb-3 rounded-[10px] border px-3 py-2 text-xs font-medium leading-snug ${
                importNotice.variant === "err"
                  ? "border-rose-400/35 bg-rose-500/[0.12] text-rose-100"
                  : "border-emerald-400/30 bg-emerald-500/[0.1] text-emerald-100"
              }`}
            >
              {importNotice.text}
            </p>
          ) : null}

          <div className="min-h-0 flex-1 overflow-hidden rounded-[12px] border border-white/[0.07] bg-black/20 shadow-inner">
            {loading ? (
              <div className="flex flex-col items-center justify-center gap-3 py-16 text-slate-500">
                <UiSpinner tone="primary" />
                <span className="text-xs">加载代理池…</span>
              </div>
            ) : (
              <ul className="proxy-glass-pool-scroll max-h-[min(48vh,420px)] space-y-1 overflow-y-auto p-2">
                {(items || []).length === 0 ? (
                  <li className="px-3 py-10 text-center text-sm text-slate-500">暂无代理记录，请先导入</li>
                ) : (
                  (items || []).map((row) => {
                    const chk = poolCheckStatusUi(row.check_status);
                    return (
                      <li key={row.id}>
                        <div className="proxy-glass-pool-row group flex items-start gap-3 rounded-[10px] px-3 py-2.5">
                          <span className={`mt-1.5 h-2 w-2 shrink-0 rounded-full ${poolStatusDot(row.status)}`} aria-hidden />
                          <div className="min-w-0 flex-1">
                            <p className="truncate font-mono text-[13px] text-slate-200">{row.address}</p>
                            <p className="mt-0.5 text-[11px] font-medium text-slate-500">
                              {poolStatusLabel(row.status)}
                              {row.assigned_account_id != null ? (
                                <span className="ml-2 text-sky-400/80">· 已关联账号</span>
                              ) : null}
                            </p>
                            <p className="mt-1 flex flex-wrap items-center gap-x-1.5 gap-y-1 font-mono text-[10px] leading-relaxed text-slate-500">
                              <span className="inline-flex items-center gap-1 text-[#60a5fa] [text-shadow:0_0_8px_rgba(96,165,250,0.25)]">
                                <Globe className="h-3 w-3 shrink-0 text-[#4fd1c5]" aria-hidden strokeWidth={2.25} />
                                {row.check_ip || "—"}
                              </span>
                              <span className="text-slate-600">·</span>
                              <span className="inline-flex items-center gap-1">
                                {countryFlagIconUrl(row.country_code) ? (
                                  <img
                                    src={countryFlagIconUrl(row.country_code)}
                                    alt={`${String(row.country_code || "").toUpperCase()} flag`}
                                    className="h-3.5 w-3.5 rounded-[1px]"
                                    loading="lazy"
                                  />
                                ) : (
                                  <span aria-hidden>🌐</span>
                                )}{" "}
                                {row.check_country || "—"}
                              </span>
                              <span className="text-slate-600">·</span>
                              <span>{row.check_city || "—"}</span>
                              <span className="text-slate-600">·</span>
                              <span className={`inline-flex items-center gap-1.5 font-sans font-semibold ${chk.cls}`}>
                                <span className={`h-1.5 w-1.5 shrink-0 rounded-full ${chk.dot}`} aria-hidden />
                                {chk.label}
                              </span>
                            </p>
                          </div>
                        </div>
                      </li>
                    );
                  })
                )}
              </ul>
            )}
          </div>

          <footer className="mt-5 flex shrink-0 justify-end border-t border-white/[0.06] pt-4">
            <button type="button" onClick={onClose} className="proxy-glass-btn proxy-glass-btn--ghost px-4 py-2 text-sm text-slate-400">
              关闭
            </button>
          </footer>
        </div>
      </div>
    </>,
    document.body,
  );
}

/** 批量检测代理 · 实时日志 */
export function ProxyCheckGlassModal({
  open,
  onClose,
  running,
  logs,
  logEndRef,
  cancelRequested = false,
  isStopping = false,
  onCancelCheck,
}) {
  useModalEscape(open, onClose);

  if (!open || typeof document === "undefined") return null;

  const text = Array.isArray(logs) && logs.length ? logs.join("\n") : running ? "等待日志…" : "无日志";
  const sub = isStopping
    ? "正在停止任务…"
    : !running && cancelRequested
      ? "检测已由用户停止"
      : running
        ? "正在检测（并发 ≤5）…"
        : "本轮检测已结束";

  return createPortal(
    <>
      <button type="button" aria-label="关闭" className="proxy-glass-overlay" onClick={onClose} />
      <div
        className="proxy-glass-dialog proxy-glass-dialog--wide"
        role="dialog"
        aria-modal="true"
        aria-labelledby="proxy-check-title"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <div className="proxy-glass-dialog-glow" aria-hidden />
        <div className="relative z-[1] flex max-h-[min(88vh,760px)] flex-col p-6 sm:p-7">
          <header className="mb-5 shrink-0 border-b border-white/[0.08] pb-4">
            <div className="flex items-start gap-3">
              <span className="grid h-11 w-11 shrink-0 place-items-center rounded-xl border border-emerald-400/30 bg-emerald-500/10 text-emerald-200 shadow-[0_0_22px_rgba(52,211,153,0.22)]">
                <Radar className="h-5 w-5" aria-hidden />
              </span>
              <div className="min-w-0">
                <h2 id="proxy-check-title" className="text-lg font-bold tracking-tight text-slate-50">
                  代理出口检测
                </h2>
                <p className="mt-1 text-xs font-medium text-slate-500">{sub}</p>
              </div>
            </div>
          </header>

          <div className="min-h-0 flex-1">
            <p className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500">实时日志</p>
            <div className="proxy-glass-log rounded-[12px] border border-white/[0.08] bg-black/35 p-3 font-log text-[11px] leading-relaxed text-slate-300 shadow-inner">
              <pre className="proxy-glass-pool-scroll max-h-[min(40vh,360px)] overflow-y-auto whitespace-pre-wrap break-words pr-1 text-slate-400">
                {text}
                <span ref={logEndRef} />
              </pre>
            </div>
          </div>

          <footer className="mt-5 flex shrink-0 flex-wrap items-center justify-end gap-2 border-t border-white/[0.06] pt-4">
            {typeof onCancelCheck === "function" && running ? (
              <button
                type="button"
                disabled={isStopping}
                onClick={() => onCancelCheck()}
                className={`proxy-glass-btn inline-flex items-center justify-center gap-2 px-4 py-2 text-sm font-semibold disabled:cursor-not-allowed ${
                  isStopping
                    ? "border-slate-500/35 bg-slate-600/25 text-slate-300 shadow-none disabled:opacity-90"
                    : "proxy-glass-btn--stop disabled:opacity-40"
                }`}
              >
                {isStopping ? (
                  <>
                    <UiSpinner tone="muted" />
                    正在停止…
                  </>
                ) : (
                  <>
                    <Square className="h-3.5 w-3.5" aria-hidden />
                    取消检测
                  </>
                )}
              </button>
            ) : null}
            <button type="button" onClick={onClose} className="proxy-glass-btn proxy-glass-btn--ghost px-4 py-2 text-sm text-slate-400">
              {!running && cancelRequested ? "关闭（已停止）" : "关闭"}
            </button>
          </footer>
        </div>
      </div>
    </>,
    document.body,
  );
}

/** 代理匹配引擎 · 自动调度 */
export function ProxyMatchGlassModal({
  open,
  onClose,
  matchUnbound,
  setMatchUnbound,
  matchDeadProxy,
  setMatchDeadProxy,
  onStartMatch,
  onStopMatch,
  matchRunning,
  logs,
  logEndRef,
}) {
  useModalEscape(open, onClose);

  if (!open || typeof document === "undefined") return null;

  return createPortal(
    <>
      <button
        type="button"
        aria-label="关闭"
        className="proxy-glass-overlay"
        onClick={() => {
          if (!matchRunning) onClose();
        }}
      />
      <div
        className="proxy-glass-dialog proxy-glass-dialog--wide"
        role="dialog"
        aria-modal="true"
        aria-labelledby="proxy-match-title"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <div className="proxy-glass-dialog-glow" aria-hidden />
        <div className="relative z-[1] flex max-h-[min(88vh,760px)] flex-col p-6 sm:p-7">
          <header className="mb-5 shrink-0 border-b border-white/[0.08] pb-4">
            <div className="flex items-start gap-3">
              <span className="grid h-11 w-11 shrink-0 place-items-center rounded-xl border border-violet-400/30 bg-violet-500/10 text-violet-200 shadow-[0_0_22px_rgba(139,92,246,0.25)]">
                <Cpu className="h-5 w-5" aria-hidden />
              </span>
              <div className="min-w-0">
                <h2 id="proxy-match-title" className="flex flex-wrap items-center gap-2 text-lg font-bold tracking-tight text-slate-50">
                  代理匹配引擎
                  <Sparkles className="h-4 w-4 text-amber-300/90" aria-hidden />
                </h2>
                <p className="mt-1 text-xs font-medium text-slate-500">自动调度执行</p>
              </div>
            </div>
          </header>

          <div className="mb-4 shrink-0 space-y-3 rounded-[12px] border border-white/[0.07] bg-black/15 p-4">
            <label className="proxy-glass-check flex cursor-pointer items-center gap-3 rounded-[10px] border border-transparent px-2 py-2 transition hover:border-white/[0.08] hover:bg-white/[0.04]">
              <input
                type="checkbox"
                className="proxy-glass-checkbox"
                checked={matchUnbound}
                onChange={(e) => setMatchUnbound(e.target.checked)}
                disabled={matchRunning}
              />
              <span className="text-sm font-medium text-slate-200">未绑定账号</span>
            </label>
            <label className="proxy-glass-check flex cursor-pointer items-center gap-3 rounded-[10px] border border-transparent px-2 py-2 transition hover:border-white/[0.08] hover:bg-white/[0.04]">
              <input
                type="checkbox"
                className="proxy-glass-checkbox"
                checked={matchDeadProxy}
                onChange={(e) => setMatchDeadProxy(e.target.checked)}
                disabled={matchRunning}
              />
              <span className="text-sm font-medium text-slate-200">代理失效账号</span>
            </label>
          </div>

          <div className="mb-4 flex shrink-0 flex-wrap gap-3">
            <button
              type="button"
              disabled={matchRunning || (!matchUnbound && !matchDeadProxy)}
              onClick={onStartMatch}
              className="proxy-glass-btn proxy-glass-btn--primary inline-flex min-w-[8.5rem] items-center justify-center gap-2 px-5 py-2.5 text-sm font-bold disabled:cursor-not-allowed disabled:opacity-45"
            >
              {matchRunning ? (
                <>
                  <UiSpinner tone="primary" />
                  执行中…
                </>
              ) : (
                <>
                  <Play className="h-4 w-4" aria-hidden />
                  开始匹配
                </>
              )}
            </button>
            <button
              type="button"
              disabled={!matchRunning}
              onClick={onStopMatch}
              className="proxy-glass-btn proxy-glass-btn--stop inline-flex items-center justify-center gap-2 px-5 py-2.5 text-sm font-bold disabled:cursor-not-allowed disabled:opacity-35"
            >
              <Square className="h-3.5 w-3.5" aria-hidden />
              停止
            </button>
          </div>

          <div className="min-h-0 flex-1">
            <p className="mb-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500">实时日志</p>
            <div className="proxy-glass-log rounded-[12px] border border-white/[0.08] bg-black/35 p-3 font-log text-[11px] leading-relaxed text-slate-300 shadow-inner">
              <pre className="proxy-glass-pool-scroll max-h-[min(32vh,280px)] overflow-y-auto whitespace-pre-wrap break-words pr-1 text-slate-400">
                {logs || "等待开始匹配…"}
                <span ref={logEndRef} />
              </pre>
            </div>
          </div>

          <footer className="mt-5 flex shrink-0 justify-end border-t border-white/[0.06] pt-4">
            <button
              type="button"
              onClick={onClose}
              disabled={matchRunning}
              className="proxy-glass-btn proxy-glass-btn--ghost px-4 py-2 text-sm text-slate-400 disabled:opacity-40"
            >
              关闭
            </button>
          </footer>
        </div>
      </div>
    </>,
    document.body,
  );
}
