import { useMemo, useState } from "react";

/**
 * 群组互动 · 高级多选面板（玻璃风、搜索、全选、已选数量）
 */
export function EngagementGroupPanel({ options, values, onChange, disabled = false }) {
  const [query, setQuery] = useState("");

  const filtered = useMemo(() => {
    if (!query.trim()) return options;
    const q = query.trim().toLowerCase();
    return options.filter((o) => String(o.label).toLowerCase().includes(q));
  }, [options, query]);

  const setSel = useMemo(() => new Set(values || []), [values]);

  const toggle = (v) => {
    const next = new Set(setSel);
    if (next.has(v)) next.delete(v);
    else next.add(v);
    onChange(Array.from(next));
  };

  const selectAll = () => {
    const all = new Set(setSel);
    options.forEach((o) => all.add(o.value));
    onChange(Array.from(all));
  };

  const selectAllFiltered = () => {
    const next = new Set(setSel);
    filtered.forEach((o) => next.add(o.value));
    onChange(Array.from(next));
  };

  const clearAll = () => onChange([]);

  const n = values?.length || 0;

  return (
    <div className="engagement-group-panel rounded-2xl border border-cyan-400/12 bg-[rgba(8,12,22,0.55)] p-3 shadow-[0_8px_40px_rgba(0,0,0,0.45),0_0_40px_rgba(34,211,238,0.06)] backdrop-blur-[20px] sm:p-4">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-white/[0.06] pb-3">
        <div className="flex items-center gap-2">
          <span className="rounded-lg border border-violet-400/25 bg-violet-500/10 px-2.5 py-1 text-[11px] font-bold tabular-nums text-violet-200/95 shadow-[0_0_16px_rgba(139,92,246,0.2)]">
            已选 <span className="text-cyan-200">{n}</span>
          </span>
          <span className="text-[10px] font-medium uppercase tracking-wider text-slate-500">/ {options.length}</span>
        </div>
        <div className="flex flex-wrap gap-1.5">
          <button
            type="button"
            disabled={disabled}
            onClick={selectAll}
            className="rounded-lg border border-white/[0.1] bg-white/[0.04] px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wide text-slate-300 transition hover:border-cyan-400/30 hover:bg-gradient-to-r hover:from-cyan-500/15 hover:to-violet-500/12 hover:text-white disabled:opacity-40"
          >
            全选
          </button>
          <button
            type="button"
            disabled={disabled || !filtered.length}
            onClick={selectAllFiltered}
            className="rounded-lg border border-white/[0.1] bg-white/[0.04] px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wide text-slate-300 transition hover:border-cyan-400/30 hover:bg-gradient-to-r hover:from-cyan-500/15 hover:to-violet-500/12 hover:text-white disabled:opacity-40"
          >
            全选结果
          </button>
          <button
            type="button"
            disabled={disabled || n === 0}
            onClick={clearAll}
            className="rounded-lg border border-rose-400/20 bg-rose-500/[0.07] px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wide text-rose-200/90 transition hover:border-rose-400/35 hover:shadow-[0_0_14px_rgba(251,113,133,0.2)] disabled:opacity-40"
          >
            清空
          </button>
        </div>
      </div>

      <div className="py-2.5">
        <input
          type="search"
          autoComplete="off"
          disabled={disabled}
          placeholder="搜索群组名称或 @…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="w-full rounded-xl border border-white/[0.08] bg-[rgba(0,0,0,0.35)] px-3 py-2 text-sm text-slate-100 shadow-inner outline-none backdrop-blur-md transition placeholder:text-slate-500 focus:border-cyan-400/35 focus:ring-2 focus:ring-cyan-400/12"
        />
      </div>

      <div className="engagement-group-panel-list growth-scroll max-h-[min(280px,42vh)] overflow-y-auto rounded-xl border border-white/[0.06] bg-[rgba(0,0,0,0.25)] p-1.5 backdrop-blur-md">
        {filtered.length === 0 ? (
          <p className="py-10 text-center text-xs text-slate-500">无匹配项</p>
        ) : (
          <ul className="space-y-0.5">
            {filtered.map((opt) => {
              const checked = setSel.has(opt.value);
              return (
                <li key={String(opt.value)}>
                  <button
                    type="button"
                    disabled={disabled}
                    onClick={() => toggle(opt.value)}
                    className={`group/opt flex w-full items-center gap-3 rounded-lg px-2.5 py-2 text-left text-sm transition duration-200 ease-out ${
                      checked
                        ? "border border-cyan-400/20 bg-gradient-to-r from-cyan-500/18 via-violet-500/12 to-transparent shadow-[0_0_20px_rgba(34,211,238,0.12)]"
                        : "border border-transparent hover:border-cyan-400/15 hover:bg-gradient-to-r hover:from-cyan-500/10 hover:to-violet-500/5"
                    }`}
                  >
                    <span
                      className={`grid h-[18px] w-[18px] shrink-0 place-items-center rounded border text-[10px] font-bold transition ${
                        checked
                          ? "border-cyan-400/50 bg-cyan-500/25 text-cyan-100 shadow-[0_0_12px_rgba(34,211,238,0.35)]"
                          : "border-white/20 bg-white/[0.04] text-transparent group-hover/opt:border-cyan-400/30"
                      }`}
                      aria-hidden
                    >
                      ✓
                    </span>
                    <span className="min-w-0 flex-1 truncate text-slate-200 group-hover/opt:text-white">{opt.label}</span>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </div>
  );
}
