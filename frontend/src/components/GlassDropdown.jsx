import { createPortal } from "react-dom";
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";

const DROPDOWN_Z = 2000;

function computePanelStyle(triggerEl, searchable) {
  if (!triggerEl) return null;
  const r = triggerEl.getBoundingClientRect();
  const gap = 6;
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  const minW = 200;
  const width = Math.min(Math.max(r.width, minW), vw - 16);
  let left = r.left;
  if (left + width > vw - 8) left = vw - width - 8;
  if (left < 8) left = 8;

  const spaceBelow = vh - r.bottom - gap - 8;
  const spaceAbove = r.top - gap - 8;
  const searchH = searchable ? 48 : 0;
  const listRoomBelow = spaceBelow - searchH;
  const listRoomAbove = spaceAbove - searchH;
  let openBelow = listRoomBelow >= 120 || listRoomBelow >= listRoomAbove;
  if (listRoomBelow < 100 && listRoomAbove > listRoomBelow) openBelow = false;

  const room = openBelow ? listRoomBelow : listRoomAbove;
  const listMax = Math.min(280, Math.max(80, room));
  const panelMax = listMax + searchH;

  let top;
  let bottom;
  if (openBelow) {
    top = r.bottom + gap;
  } else {
    bottom = vh - r.top + gap;
  }

  return { top, bottom, left, width, panelMax };
}

/**
 * 玻璃拟态自定义下拉：面板通过 portal + position:fixed 渲染，避免被父级 overflow 裁切
 */
export function GlassDropdown({
  value,
  onChange,
  options,
  placeholder = "请选择…",
  searchable = false,
  className = "",
  triggerClassName = "",
  /** 显示在选中项文案前，例如「状态：」 */
  triggerPrefix = "",
  /** 追加到菜单面板容器（如 rounded-[10px]） */
  menuClassName = "",
  disabled = false,
  variant = "default",
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const rootRef = useRef(null);
  const triggerRef = useRef(null);
  const panelRef = useRef(null);
  const [panelStyle, setPanelStyle] = useState(null);
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

  const updatePosition = useCallback(() => {
    const el = triggerRef.current;
    if (!el || !open) return;
    const next = computePanelStyle(el, searchable);
    setPanelStyle(next);
  }, [open, searchable]);

  useLayoutEffect(() => {
    if (!open) {
      setPanelStyle(null);
      return;
    }
    updatePosition();
  }, [open, updatePosition]);

  useEffect(() => {
    if (!open) return;
    const onScroll = () => updatePosition();
    const onResize = () => updatePosition();
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", onResize);
    };
  }, [open, updatePosition]);

  useEffect(() => {
    if (!open) return;
    const onDoc = (e) => {
      const t = e.target;
      if (rootRef.current?.contains(t)) return;
      if (panelRef.current?.contains(t)) return;
      close();
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

  const fixedBase =
    "fixed rounded-xl border border-white/[0.1] bg-[rgba(14,20,28,0.92)] shadow-[0_20px_60px_rgba(0,0,0,0.55),0_0_40px_rgba(0,255,180,0.06)] backdrop-blur-[22px]";

  const taskFixedBase =
    "fixed rounded-xl border border-[rgba(0,175,255,0.18)] bg-[rgba(12,18,28,0.96)] shadow-[0_24px_64px_rgba(0,0,0,0.55),0_0_40px_rgba(0,175,255,0.08),0_0_48px_rgba(122,92,255,0.06)] backdrop-blur-[20px]";

  const dropdownFixedStyle =
    panelStyle && open
      ? {
          zIndex: DROPDOWN_Z,
          left: panelStyle.left,
          width: panelStyle.width,
          maxHeight: panelStyle.panelMax,
          ...(panelStyle.top != null ? { top: panelStyle.top } : {}),
          ...(panelStyle.bottom != null ? { bottom: panelStyle.bottom } : {}),
        }
      : undefined;

  return (
    <div ref={rootRef} className={`relative min-w-0 ${className}`}>
      <button
        ref={triggerRef}
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
        <span className={`min-w-0 flex-1 truncate ${displayLabel || triggerPrefix ? "" : "text-slate-500"}`}>
          {triggerPrefix ? (
            <span className="text-slate-500">{triggerPrefix}</span>
          ) : null}
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

      {typeof document !== "undefined" &&
        open &&
        panelStyle &&
        createPortal(
          <div
            ref={panelRef}
            role="listbox"
            aria-hidden={false}
            style={dropdownFixedStyle}
            className={
              isTask
                ? `${taskFixedBase} flex flex-col overflow-hidden`
                : `${fixedBase} flex flex-col overflow-hidden ${menuClassName}`.trim()
            }
          >
            {searchable ? (
              <div className="shrink-0 border-b border-white/[0.06] p-2" onMouseDown={(e) => e.preventDefault()}>
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
            <ul className="growth-scroll min-h-0 flex-1 overflow-y-auto overflow-x-hidden py-1">
              {filtered.length === 0 ? (
                <li className="px-3 py-2.5 text-center text-xs text-slate-500">无匹配项</li>
              ) : (
                filtered.map((opt) => {
                  const isActive = opt.value === value;
                  const customOpt =
                    opt.itemInactiveClass != null ||
                    opt.itemActiveClass != null ||
                    opt.itemBaseClass != null;
                  const optRowBase =
                    opt.itemBaseClass ??
                    "flex w-full items-center px-3 py-2.5 text-left text-sm transition duration-200 ease-out rounded-[10px] mx-1";
                  const defaultInactive = "text-slate-300 hover:bg-white/[0.06]";
                  const defaultActive =
                    "bg-gradient-to-r from-emerald-500/20 to-cyan-500/15 font-medium text-emerald-200";
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
                            : customOpt
                              ? `${optRowBase} ${
                                  isActive
                                    ? opt.itemActiveClass ?? defaultActive
                                    : opt.itemInactiveClass ?? defaultInactive
                                }`
                              : `flex w-full items-center px-3 py-2.5 text-left text-sm transition duration-200 ease-out will-change-transform hover:translate-x-1 ${
                                  isActive ? defaultActive : defaultInactive
                                }`
                        }
                      >
                        <span className="min-w-0 flex-1 truncate">{opt.label}</span>
                        {isActive ? (
                          <svg
                            className={`ml-2 h-4 w-4 shrink-0 ${isTask ? "text-[#00AFFF]" : "text-cyan-300/90"}`}
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
          </div>,
          document.body,
        )}
    </div>
  );
}
