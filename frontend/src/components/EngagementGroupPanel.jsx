import { useEffect, useMemo, useState } from "react";

/**
 * 群组互动 · 高级多选面板（玻璃风、搜索、全选、已选数量）
 */
export function EngagementGroupPanel({
  options,
  values,
  onChange,
  onDeleteSelected,
  onUpdateGroup,
  deleting = false,
  disabled = false,
}) {
  const [query, setQuery] = useState("");
  const [editKey, setEditKey] = useState("");
  const [editDrafts, setEditDrafts] = useState({});
  const [savingEditMap, setSavingEditMap] = useState({});

  const filtered = useMemo(() => {
    if (!query.trim()) return options;
    const q = query.trim().toLowerCase();
    return options.filter((o) =>
      [o.label, o.value, o.title, o.remark]
        .map((v) => String(v || "").toLowerCase())
        .some((v) => v.includes(q)),
    );
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

  const unselectAllFiltered = () => {
    const next = new Set(setSel);
    filtered.forEach((o) => next.delete(o.value));
    onChange(Array.from(next));
  };

  const n = values?.length || 0;

  useEffect(() => {
    const next = {};
    (options || []).forEach((opt) => {
      next[String(opt.value)] = {
        username: String(opt.value || ""),
        title: String(opt.title || ""),
        remark: String(opt.remark || ""),
      };
    });
    setEditDrafts(next);
  }, [options]);

  const setEditDraft = (value, field, text) => {
    const key = String(value);
    setEditDrafts((prev) => ({
      ...prev,
      [key]: {
        username: String(prev[key]?.username ?? ""),
        title: String(prev[key]?.title ?? ""),
        remark: String(prev[key]?.remark ?? ""),
        [field]: text,
      },
    }));
  };

  const saveGroupEdit = async (opt) => {
    if (!onUpdateGroup || !opt?.id) return;
    const key = String(opt.value);
    const draft = editDrafts[key] || {};
    const payload = {
      username: String(draft.username || "").trim(),
      title: String(draft.title || "").trim(),
      remark: String(draft.remark || ""),
    };
    if (!payload.username) return;
    setSavingEditMap((prev) => ({ ...prev, [key]: true }));
    try {
      await onUpdateGroup(opt.id, payload);
      setEditKey("");
    } finally {
      setSavingEditMap((prev) => ({ ...prev, [key]: false }));
    }
  };

  return (
    <div className="engagement-group-panel relative overflow-hidden rounded-2xl border border-cyan-400/18 bg-[rgba(8,12,22,0.58)] p-3 shadow-[0_8px_40px_rgba(0,0,0,0.45),0_0_44px_rgba(34,211,238,0.08)] backdrop-blur-[20px] sm:p-4">
      <div className="engagement-panel-aurora engagement-panel-aurora-a" aria-hidden />
      <div className="engagement-panel-aurora engagement-panel-aurora-b" aria-hidden />
      <div className="engagement-panel-grid-glow" aria-hidden />
      <div className="relative z-[1]">
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
            disabled={disabled || filtered.length === 0 || n === 0}
            onClick={unselectAllFiltered}
            className="rounded-lg border border-white/[0.1] bg-white/[0.04] px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wide text-slate-300 transition hover:border-cyan-400/30 hover:bg-gradient-to-r hover:from-cyan-500/15 hover:to-violet-500/12 hover:text-white disabled:opacity-40"
          >
            取消全选
          </button>
          <button
            type="button"
            disabled={disabled || n === 0 || deleting}
            onClick={onDeleteSelected}
            className="rounded-lg border border-rose-400/20 bg-rose-500/[0.07] px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wide text-rose-200/90 transition hover:border-rose-400/35 hover:shadow-[0_0_14px_rgba(251,113,133,0.2)] disabled:opacity-40"
          >
            {deleting ? "删除中" : "删除"}
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

      <div className="engagement-group-panel-list growth-scroll max-h-[calc(100vh-300px)] max-h-[calc(100dvh-300px)] overflow-y-auto rounded-xl border border-white/[0.06] bg-[rgba(0,0,0,0.25)] p-1.5 backdrop-blur-md">
        {filtered.length === 0 ? (
          <p className="py-10 text-center text-xs text-slate-500">无匹配项</p>
        ) : (
          <ul className="space-y-2">
            {filtered.map((opt) => {
              const checked = setSel.has(opt.value);
              const key = String(opt.value);
              const isEditing = editKey === key;
              const savingEdit = Boolean(savingEditMap[key]);
              const draft = editDrafts[key] || { username: "", title: "", remark: "" };
              return (
                <li key={String(opt.value)}>
                  <div
                    className={`rounded-xl border p-2.5 transition ${
                      checked
                        ? "border-cyan-400/22 bg-gradient-to-r from-cyan-500/16 via-violet-500/12 to-transparent shadow-[0_0_20px_rgba(34,211,238,0.12)]"
                        : "border-white/[0.08] bg-white/[0.02] hover:border-cyan-400/18 hover:bg-gradient-to-r hover:from-cyan-500/8 hover:to-violet-500/5"
                    }`}
                  >
                    {!isEditing ? (
                      <div className="flex items-center gap-2">
                        <button
                          type="button"
                          disabled={disabled}
                          onClick={() => toggle(opt.value)}
                          className="group/opt flex min-w-0 flex-1 items-center gap-3 rounded-lg px-1 py-1 text-left text-sm"
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
                        <span
                          className="inline-flex max-w-[36%] items-center gap-1 truncate rounded-md border border-emerald-400/25 bg-emerald-500/10 px-2 py-1 text-[11px] text-emerald-100/90"
                          title={`备注：${opt.remark || "无"}`}
                        >
                          <span aria-hidden>📝</span>
                          <span className="truncate">{opt.remark ? opt.remark : "无备注"}</span>
                        </span>
                        <button
                          type="button"
                          disabled={disabled || !onUpdateGroup}
                          onClick={() => setEditKey(key)}
                          className="rounded-lg border border-cyan-400/30 bg-cyan-500/12 px-2.5 py-1.5 text-[11px] font-semibold text-cyan-100 transition hover:border-cyan-300/45 hover:bg-cyan-500/22 disabled:opacity-40"
                        >
                          编辑
                        </button>
                      </div>
                    ) : (
                      <div className="mt-2 grid gap-2">
                        <input
                          type="text"
                          value={draft.username}
                          disabled={disabled || savingEdit}
                          onChange={(e) => setEditDraft(opt.value, "username", e.target.value)}
                          placeholder="群组ID（如 @name 或 -100...）"
                          className="w-full rounded-lg border border-cyan-400/22 bg-cyan-500/[0.08] px-2.5 py-1.5 text-xs text-cyan-100 outline-none transition placeholder:text-cyan-200/45 focus:border-cyan-300/45 focus:ring-2 focus:ring-cyan-300/10 disabled:opacity-40"
                        />
                        <input
                          type="text"
                          value={draft.title}
                          disabled={disabled || savingEdit}
                          onChange={(e) => setEditDraft(opt.value, "title", e.target.value)}
                          placeholder="群组名称"
                          className="w-full rounded-lg border border-violet-400/22 bg-violet-500/[0.08] px-2.5 py-1.5 text-xs text-violet-100 outline-none transition placeholder:text-violet-200/45 focus:border-violet-300/45 focus:ring-2 focus:ring-violet-300/10 disabled:opacity-40"
                        />
                        <input
                          type="text"
                          value={draft.remark}
                          disabled={disabled || savingEdit}
                          onChange={(e) => setEditDraft(opt.value, "remark", e.target.value)}
                          placeholder="备注"
                          className="w-full rounded-lg border border-emerald-400/22 bg-emerald-500/[0.08] px-2.5 py-1.5 text-xs text-emerald-100 outline-none transition placeholder:text-emerald-200/45 focus:border-emerald-300/45 focus:ring-2 focus:ring-emerald-300/10 disabled:opacity-40"
                        />
                        <div className="flex justify-end gap-2">
                          <button
                            type="button"
                            disabled={disabled || savingEdit}
                            onClick={() => setEditKey("")}
                            className="rounded-lg border border-white/15 bg-white/5 px-2.5 py-1.5 text-[11px] font-semibold text-slate-200 transition hover:bg-white/10 disabled:opacity-40"
                          >
                            取消
                          </button>
                          <button
                            type="button"
                            disabled={disabled || savingEdit || !onUpdateGroup}
                            onClick={() => saveGroupEdit(opt)}
                            className="rounded-lg border border-emerald-400/30 bg-emerald-500/15 px-2.5 py-1.5 text-[11px] font-semibold text-emerald-100 transition hover:border-emerald-300/45 hover:bg-emerald-500/25 disabled:opacity-40"
                          >
                            {savingEdit ? "保存中" : "提交修改"}
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </div>
      </div>
    </div>
  );
}
