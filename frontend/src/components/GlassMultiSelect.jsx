import { useCallback, useEffect, useMemo, useRef, useState } from "react";

/**
 * 玻璃风多选 + 搜索（勾选不关闭面板，适合目标群组多选）
 */
export function GlassMultiSelect({
  values,
  onChange,
  options,
  placeholder = "请选择…",
  className = "",
  triggerClassName = "",
  disabled = false,
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const rootRef = useRef(null);

  const setSel = useMemo(() => new Set(values || []), [values]);

  const filtered = useMemo(() => {
    if (!query.trim()) return options;
    const q = query.trim().toLowerCase();
    return options.filter((o) => String(o.label).toLowerCase().includes(q));
  }, [options, query]);

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

  const toggle = (v) => {
    const next = new Set(setSel);
    if (next.has(v)) next.delete(v);
    else next.add(v);
    onChange(Array.from(next));
  };

  const summary =
    values && values.length
      ? `已选 ${values.length} 个${values.length <= 2 ? ` · ${values.join("、")}` : ""}`
      : null;

  const triggerBase =
    "flex w-full items-center justify-between gap-2 rounded-xl border border-white/[0.08] bg-[rgba(255,255,255,0.04)] px-2.5 py-2 text-left text-sm text-slate-100 shadow-[0_4px_24px_rgba(0,0,0,0.25)] backdrop-blur-[16px] outline-none transition-all duration-200 hover:border-white/[0.12] focus-visible:ring-2 focus-visible:ring-fuchsia-400/30 disabled:cursor-not-allowed disabled:opacity-50";

  return (
    <div ref={rootRef} className={`relative min-w-0 ${className}`}>
      <button
        type="button"
        disabled={disabled}
        aria-expanded={open}
        aria-haspopup="listbox"
        onClick={() => !disabled && setOpen((v) => !v)}
        className={`${triggerBase} ${triggerClassName}`}
      >
        <span className={`min-w-0 flex-1 truncate ${summary ? "" : "text-slate-500"}`}>
          {summary || placeholder}
        </span>
        <svg
          className={`h-4 w-4 shrink-0 text-fuchsia-300/80 transition-transform duration-300 ease-out ${open ? "rotate-180" : ""}`}
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
        className={`absolute left-0 right-0 top-[calc(100%+4px)] z-[100] origin-top overflow-hidden rounded-xl border border-fuchsia-500/15 bg-[rgba(14,12,24,0.94)] shadow-[0_20px_60px_rgba(0,0,0,0.55),0_0_48px_rgba(192,38,211,0.12)] backdrop-blur-[20px] transition-[opacity,transform] duration-200 ease-out ${
          open
            ? "pointer-events-auto translate-y-0 scale-100 opacity-100"
            : "pointer-events-none -translate-y-[10px] scale-[0.98] opacity-0"
        }`}
      >
        <div className="border-b border-white/[0.06] p-2" onMouseDown={(e) => e.preventDefault()}>
          <input
            type="search"
            autoComplete="off"
            placeholder="搜索群组…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="w-full rounded-lg border border-white/[0.08] bg-[rgba(255,255,255,0.05)] px-2.5 py-1.5 text-xs text-slate-100 outline-none backdrop-blur-[12px] placeholder:text-slate-500 focus:border-fuchsia-400/35 focus:ring-2 focus:ring-fuchsia-400/15"
          />
        </div>
        <ul className="growth-scroll max-h-60 overflow-y-auto py-1">
          {filtered.length === 0 ? (
            <li className="px-3 py-2.5 text-center text-xs text-slate-500">无匹配项</li>
          ) : (
            filtered.map((opt) => {
              const checked = setSel.has(opt.value);
              return (
                <li key={String(opt.value)}>
                  <button
                    type="button"
                    role="option"
                    aria-selected={checked}
                    onClick={() => toggle(opt.value)}
                    className={`flex w-full items-center gap-2 px-3 py-2.5 text-left text-sm transition duration-200 ease-out hover:translate-x-0.5 ${
                      checked
                        ? "bg-gradient-to-r from-fuchsia-500/20 to-violet-500/15 font-medium text-fuchsia-100"
                        : "text-slate-300 hover:bg-white/[0.06]"
                    }`}
                  >
                    <span
                      className={`grid h-4 w-4 shrink-0 place-items-center rounded border text-[10px] ${
                        checked
                          ? "border-fuchsia-400/50 bg-fuchsia-500/25 text-fuchsia-200 shadow-[0_0_10px_rgba(217,70,239,0.35)]"
                          : "border-white/20 bg-white/[0.04] text-transparent"
                      }`}
                    >
                      ✓
                    </span>
                    <span className="min-w-0 flex-1 truncate">{opt.label}</span>
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
