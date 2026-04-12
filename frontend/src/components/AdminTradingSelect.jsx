import { createPortal } from "react-dom";
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { Check, ChevronDown } from "lucide-react";

const PANEL_Z = 2100;

function computePanelStyle(triggerEl) {
  if (!triggerEl) return null;
  const r = triggerEl.getBoundingClientRect();
  const gap = 6;
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  const minW = Math.max(r.width, 200);
  const width = Math.min(minW, vw - 16);
  let left = r.left;
  if (left + width > vw - 8) left = vw - width - 8;
  if (left < 8) left = 8;

  const spaceBelow = vh - r.bottom - gap - 8;
  const spaceAbove = r.top - gap - 8;
  const openBelow = spaceBelow >= 100 || spaceBelow >= spaceAbove;
  const room = openBelow ? spaceBelow : spaceAbove;
  const panelMax = Math.min(320, Math.max(120, room));

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
 * 用户管理页：交易系统风浮层选择器（排序 / 权限等）
 * options: { value, label, TrailingIcon? } — 无 TrailingIcon 时选中项右侧显示 Check
 */
export function AdminTradingSelect({
  value,
  onChange,
  options = [],
  placeholder = "请选择…",
  triggerPrefix = "",
  disabled = false,
  className = "",
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef(null);
  const triggerRef = useRef(null);
  const panelRef = useRef(null);
  const [panelStyle, setPanelStyle] = useState(null);

  const selectedOpt = useMemo(() => options.find((o) => o.value === value), [options, value]);
  const displayLabel = selectedOpt?.label ?? (value !== "" && value != null ? String(value) : null);

  const close = useCallback(() => setOpen(false), []);

  const updatePosition = useCallback(() => {
    const el = triggerRef.current;
    if (!el || !open) return;
    setPanelStyle(computePanelStyle(el));
  }, [open]);

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

  const fixedStyle =
    panelStyle && open
      ? {
          zIndex: PANEL_Z,
          left: panelStyle.left,
          width: panelStyle.width,
          maxHeight: panelStyle.panelMax,
          ...(panelStyle.top != null ? { top: panelStyle.top } : {}),
          ...(panelStyle.bottom != null ? { bottom: panelStyle.bottom } : {}),
        }
      : undefined;

  const pick = (v) => {
    onChange(v);
    close();
  };

  return (
    <div ref={rootRef} className={`relative min-w-0 ${className}`}>
      <button
        ref={triggerRef}
        type="button"
        disabled={disabled}
        aria-expanded={open}
        aria-haspopup="listbox"
        onClick={() => !disabled && setOpen((x) => !x)}
        className="admin-trading-select-trigger w-full text-left outline-none focus-visible:ring-2 focus-visible:ring-cyan-400/35 disabled:cursor-not-allowed disabled:opacity-45"
      >
        <span className="min-w-0 flex-1 truncate">
          {triggerPrefix ? <span className="text-slate-500/90">{triggerPrefix}</span> : null}
          <span className={displayLabel ? "text-[#cfe3ff]" : "text-slate-500"}>
            {displayLabel || placeholder}
          </span>
        </span>
        <ChevronDown
          className={`h-4 w-4 shrink-0 text-slate-400 transition-transform duration-200 ${open ? "rotate-180" : ""}`}
          strokeWidth={2}
          aria-hidden
        />
      </button>

      {typeof document !== "undefined" &&
        open &&
        panelStyle &&
        createPortal(
          <div
            ref={panelRef}
            role="listbox"
            style={fixedStyle}
            className="admin-trading-select-dropdown fixed flex flex-col overflow-y-auto overflow-x-hidden"
          >
            {options.map((opt) => {
              const active = opt.value === value;
              const T = opt.TrailingIcon;
              return (
                <button
                  key={String(opt.value)}
                  type="button"
                  role="option"
                  aria-selected={active}
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() => pick(opt.value)}
                  className={`admin-trading-select-item ${active ? "admin-trading-select-item--active" : ""}`}
                >
                  <span className="min-w-0 flex-1 truncate text-left">{opt.label}</span>
                  <span className="flex h-5 w-5 shrink-0 items-center justify-center text-slate-400">
                    {T ? (
                      <T className="h-3.5 w-3.5 opacity-80" strokeWidth={2.25} aria-hidden />
                    ) : active ? (
                      <Check className="h-3.5 w-3.5 text-sky-300/90" strokeWidth={2.5} aria-hidden />
                    ) : null}
                  </span>
                </button>
              );
            })}
          </div>,
          document.body,
        )}
    </div>
  );
}
