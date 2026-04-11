import { useCallback, useEffect, useMemo, useRef, useState } from "react";

/**
 * 玻璃拟态自定义下拉（无原生 select）
 * default：200ms 展开；task：控制台主题 + 更平滑 cubic-bezier
 */
export function GlassDropdown({
  value,
  onChange,
  options,
  placeholder = "请选择…",
  searchable = false,
  className = "",
  triggerClassName = "",
  disabled = false,
  variant = "default",
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const rootRef = useRef(null);
  const isTask = variant === "task";

  const filtered = useMemo(() => {
    if (!searchable || !query.trim()) return options;
    const q = query.trim().toLowerCase();
    return options.filter((o) => String(o.label).toLowerCase().includes(q));
  }, [options, query, searchable]);

  const selectedOpt = useMemo(() => options.find((o) => o.value === value), [options, value]);
  const displayLabel = selectedOpt?.label ?? (value !== "" && value != null ? String(value) : null);

  const close = useCallback(() => {
    setOpen(false);
    setQuery("");
  }, []);

  useEffect(() => {
    if (!open) return;
    const onDoc = (e) => {
      if (rootRef.current && !rootRef.current.contains(e.target)) close();
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open, close]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e) => {
      if (e.key === "Escape") close();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [open, close]);

  const triggerBaseDefault =
    "flex w-full items-center justify-between gap-2 rounded-xl border border-white/[0.08] bg-[rgba(255,255,255,0.04)] px-2.5 py-2 text-left text-sm text-slate-100 shadow-[0_4px_24px_rgba(0,0,0,0.25)] backdrop-blur-[16px] outline-none transition-all duration-200 hover:border-white/[0.12] focus-visible:ring-2 focus-visible:ring-cyan-400/30 disabled:cursor-not-allowed disabled:opacity-50";

  return (
    <div ref={rootRef} className={`relative min-w-0 ${className}`}>
      <button
        type="button"
        disabled={disabled}
        aria-expanded={open}
        aria-haspopup="listbox"
        onClick={() => !disabled && setOpen((v) => !v)}
        className={
          isTask
            ? `glass-dd-task-trigger ${triggerClassName}`
            : `${triggerBaseDefault} ${triggerClassName}`
        }
      >
        <span className={`min-w-0 flex-1 truncate ${displayLabel ? "" : "text-slate-500"}`}>
          {displayLabel || placeholder}
        </span>
        <svg
          className={`h-4 w-4 shrink-0 transition-transform duration-300 ease-out ${open ? "rotate-180" : ""} ${isTask ? "text-[#00AFFF]" : "text-slate-500"}`}
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          aria-hidden
        >
          <path d="M6 9l6 6 6-6" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </button>

      <div
        role="listbox"
        aria-hidden={!open}
        className={
          isTask
            ? `glass-dd-task-panel ${open ? "glass-dd-task-panel--open" : "glass-dd-task-panel--closed"}`
            : `absolute left-0 right-0 top-[calc(100%+4px)] z-[100] origin-top overflow-hidden rounded-xl border border-white/[0.1] bg-[rgba(14,20,28,0.94)] shadow-[0_20px_60px_rgba(0,0,0,0.55),0_0_40px_rgba(0,255,180,0.06)] backdrop-blur-[20px] transition-[opacity,transform] duration-200 ease-out ${
                open
                  ? "pointer-events-auto translate-y-0 scale-100 opacity-100"
                  : "pointer-events-none -translate-y-[10px] scale-[0.98] opacity-0"
              }`
        }
      >
        {searchable ? (
          <div className="border-b border-white/[0.06] p-2" onMouseDown={(e) => e.preventDefault()}>
            <input
              type="search"
              autoComplete="off"
              placeholder="搜索…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className={
                isTask
                  ? "glass-dd-task-search"
                  : "w-full rounded-lg border border-white/[0.08] bg-[rgba(255,255,255,0.05)] px-2.5 py-1.5 text-xs text-slate-100 outline-none backdrop-blur-[12px] placeholder:text-slate-500 focus:border-cyan-400/35 focus:ring-2 focus:ring-cyan-400/15"
              }
            />
          </div>
        ) : null}
        <ul className="growth-scroll max-h-60 overflow-y-auto py-1">
          {filtered.length === 0 ? (
            <li className="px-3 py-2.5 text-center text-xs text-slate-500">无匹配项</li>
          ) : (
            filtered.map((opt) => {
              const isActive = opt.value === value;
              return (
                <li key={String(opt.value)}>
                  <button
                    type="button"
                    role="option"
                    aria-selected={isActive}
                    onClick={() => {
                      onChange(opt.value);
                      close();
                    }}
                    className={
                      isTask
                        ? `glass-dd-task-option ${isActive ? "glass-dd-task-option--active" : ""}`
                        : `flex w-full items-center px-3 py-2.5 text-left text-sm transition duration-200 ease-out will-change-transform hover:translate-x-1 ${
                            isActive
                              ? "bg-gradient-to-r from-emerald-500/20 to-cyan-500/15 font-medium text-emerald-200"
                              : "text-slate-300 hover:bg-white/[0.06]"
                          }`
                    }
                  >
                    <span className="min-w-0 flex-1 truncate">{opt.label}</span>
                    {isActive ? (
                      <svg
                        className={`ml-2 h-4 w-4 shrink-0 ${isTask ? "text-[#00AFFF]" : "text-[#00ff87]"}`}
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2.5"
                      >
                        <path d="M20 6L9 17l-5-5" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    ) : null}
                  </button>
                </li>
              );
            })
          )}
        </ul>
      </div>
    </div>
  );
}
