import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Activity,
  AlertCircle,
  BarChart3,
  CalendarClock,
  CheckCircle,
  Download,
  Globe,
  Info,
  Layers,
  Loader,
  MessageCircle,
  Network,
  Repeat2,
  Server,
  Shield,
  Sparkles,
  Upload,
  TrendingUp,
  UserCheck,
  UserCircle,
  UserCog,
  UserSearch,
  Users,
  Users2,
  XCircle,
} from "lucide-react";
import { api, downloadScraperFile, downloadScraperTaskById } from "./api";
import { GlassDropdown } from "./components/GlassDropdown";
import { EngagementGroupPanel } from "./components/EngagementGroupPanel";
import { UiSpinner } from "./components/UiSpinner";

const menus = ["用户增长", "账号检测", "目标群组", "群组互动", "代理监控", "用户采集", "消息Copy", "用户管理"];

/** 玻璃基底（无纯白/纯黑底板） */
const CARD_GLASS_CORE =
  "rounded-2xl bg-[rgba(255,255,255,0.03)] backdrop-blur-[20px] transition-all duration-[250ms] ease-out hover:-translate-y-0.5";

/** 模块分色光晕：增长绿 / 日志蓝 / 风控紫红 / 采集青绿 / 警告琥珀 / 默认薄荷 */
const MODULE_CARD = {
  default:
    "border border-white/[0.08] p-5 shadow-[0_8px_40px_rgba(0,0,0,0.42),0_0_40px_rgba(0,255,200,0.08)] hover:border-white/[0.14] hover:shadow-[0_20px_56px_rgba(0,0,0,0.48),0_0_52px_rgba(0,255,200,0.14)]",
  growth:
    "border border-emerald-400/14 p-5 shadow-[0_8px_40px_rgba(0,0,0,0.42),0_0_42px_rgba(34,197,94,0.11)] hover:border-emerald-400/28 hover:shadow-[0_20px_56px_rgba(0,0,0,0.45),0_0_56px_rgba(52,211,153,0.16)]",
  log: "border border-blue-400/14 p-5 shadow-[0_8px_40px_rgba(0,0,0,0.42),0_0_42px_rgba(59,130,246,0.12)] hover:border-blue-400/26 hover:shadow-[0_20px_56px_rgba(0,0,0,0.45),0_0_56px_rgba(96,165,250,0.15)]",
  risk:
    "border border-fuchsia-500/16 p-5 shadow-[0_8px_40px_rgba(0,0,0,0.42),0_0_48px_rgba(192,38,211,0.1),0_0_60px_rgba(244,63,94,0.07)] hover:border-fuchsia-400/32 hover:shadow-[0_20px_56px_rgba(0,0,0,0.45),0_0_64px_rgba(217,70,239,0.14)]",
  scraper:
    "border border-teal-400/15 p-5 shadow-[0_8px_40px_rgba(0,0,0,0.4),0_0_44px_rgba(0,255,200,0.09),0_0_64px_rgba(34,211,238,0.07)] hover:border-cyan-400/28 hover:shadow-[0_20px_56px_rgba(0,0,0,0.42),0_0_58px_rgba(45,212,191,0.15)]",
  warn: "border border-amber-400/15 p-5 shadow-[0_8px_40px_rgba(0,0,0,0.42),0_0_42px_rgba(251,146,60,0.1)] hover:border-amber-400/28 hover:shadow-[0_20px_56px_rgba(0,0,0,0.45),0_0_52px_rgba(251,191,36,0.12)]",
};

function cardShellClass(accent = "default") {
  return `${CARD_GLASS_CORE} ${MODULE_CARD[accent] ?? MODULE_CARD.default}`;
}

const CARD_SHELL = cardShellClass("default");

const MODAL_SHELL =
  "rounded-2xl border border-blue-400/18 bg-[rgba(12,20,36,0.88)] shadow-[0_24px_80px_rgba(0,0,0,0.55),0_0_60px_rgba(59,130,246,0.14)] backdrop-blur-[22px]";

const INPUT_FIELD =
  "rounded-xl border border-white/[0.08] bg-[rgba(255,255,255,0.04)] px-3 py-2 text-sm text-slate-100 outline-none backdrop-blur-[16px] transition placeholder:text-slate-500 hover:border-white/[0.12] focus:border-cyan-400/40 focus:ring-2 focus:ring-cyan-400/15";

const BTN_SECONDARY =
  "rounded-xl border border-white/[0.1] bg-[rgba(255,255,255,0.05)] px-4 py-2 text-sm font-medium text-slate-200 shadow-[0_4px_24px_rgba(0,0,0,0.3)] backdrop-blur-[16px] transition-all duration-200 hover:-translate-y-0.5 hover:border-white/[0.14] hover:bg-[rgba(255,255,255,0.08)] hover:text-white hover:shadow-[0_8px_32px_rgba(96,239,255,0.12)] active:translate-y-0";

const BTN_PRIMARY =
  "rounded-xl bg-[linear-gradient(135deg,#00ff87,#60efff)] px-4 py-2 text-sm font-semibold text-slate-900 shadow-[0_0_20px_rgba(0,255,150,0.3),0_6px_20px_rgba(0,0,0,0.25)] transition-all duration-200 hover:-translate-y-0.5 hover:shadow-[0_0_36px_rgba(0,255,180,0.48),0_10px_28px_rgba(96,239,255,0.28)] active:translate-y-0 active:shadow-[0_0_16px_rgba(0,255,150,0.28)]";

/** 内嵌终端：用户增长队列 = 绿色光，实时日志 = 蓝色光 */
const GLASS_PANEL_GROWTH =
  "flex flex-col overflow-hidden rounded-2xl border border-emerald-400/14 bg-[rgba(255,255,255,0.03)] shadow-[0_8px_40px_rgba(0,0,0,0.42),0_0_40px_rgba(34,197,94,0.08)] backdrop-blur-[20px]";

const GLASS_PANEL_CHROME_GROWTH =
  "flex shrink-0 items-center gap-2 border-b border-emerald-400/10 bg-emerald-500/[0.05] px-3 py-2 backdrop-blur-[12px]";

const GLASS_PANEL_CHROME_LOG =
  "flex shrink-0 items-center gap-2 border-b border-blue-400/12 bg-blue-500/[0.06] px-3 py-2 backdrop-blur-[12px]";

/** 代理监控表格：蓝色信息光晕 */
const TABLE_WRAP =
  "overflow-x-auto overflow-hidden rounded-xl border border-blue-400/12 bg-[rgba(255,255,255,0.03)] shadow-[0_8px_40px_rgba(0,0,0,0.42),0_0_40px_rgba(59,130,246,0.09)] backdrop-blur-[20px]";

/** 用户采集：青绿双色环境光 */
const SCRAPER_PAGE =
  "relative overflow-hidden rounded-2xl border border-teal-400/18 bg-[rgba(255,255,255,0.025)] p-5 shadow-[0_8px_40px_rgba(0,0,0,0.38),0_0_48px_rgba(0,255,200,0.1),0_0_72px_rgba(34,211,238,0.07)] backdrop-blur-[16px] sm:p-7";
const SCRAPER_GLASS_CARD = cardShellClass("scraper");
const SCRAPER_FIELD = INPUT_FIELD;
const SCRAPER_BTN_GLOW =
  "inline-flex items-center justify-center gap-2 rounded-2xl bg-[linear-gradient(135deg,#00ff87,#60efff)] px-5 py-2.5 text-sm font-semibold text-slate-900 shadow-[0_0_20px_rgba(0,255,150,0.3),0_6px_24px_rgba(0,0,0,0.28)] transition-all duration-[250ms] ease-out hover:-translate-y-0.5 hover:shadow-[0_0_36px_rgba(0,255,180,0.45),0_10px_32px_rgba(96,239,255,0.22)] active:scale-[0.98] active:translate-y-0 disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0 disabled:hover:shadow-[0_0_18px_rgba(0,255,150,0.2)]";
const SCRAPER_BTN_GLOW_SM =
  "inline-flex items-center justify-center gap-2 rounded-2xl bg-[linear-gradient(135deg,#00ff87,#60efff)] px-3 py-2 text-xs font-semibold text-slate-900 shadow-[0_0_18px_rgba(0,255,150,0.28)] transition-all duration-[250ms] ease-out hover:-translate-y-0.5 hover:shadow-[0_0_30px_rgba(96,239,255,0.38)] active:scale-[0.98] disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0";
const SCRAPER_BTN_GLOW_BLOCK = `${SCRAPER_BTN_GLOW} w-full`;
const SCRAPER_HISTORY_CARD =
  "group rounded-2xl border border-teal-400/14 bg-[rgba(255,255,255,0.03)] p-4 shadow-[0_8px_32px_rgba(0,0,0,0.38),0_0_40px_rgba(0,255,200,0.08),0_0_56px_rgba(34,211,238,0.06)] backdrop-blur-[20px] transition-all duration-[250ms] ease-out will-change-transform hover:-translate-y-0.5 hover:border-cyan-400/25 hover:shadow-[0_18px_48px_rgba(0,0,0,0.42),0_0_52px_rgba(45,212,191,0.14)] active:scale-[0.99]";

/** 消息 Copy：紫罗兰科技光 */
const COPY_PAGE =
  "relative overflow-hidden rounded-2xl border border-violet-400/16 bg-[rgba(255,255,255,0.025)] p-5 shadow-[0_8px_40px_rgba(0,0,0,0.38),0_0_48px_rgba(139,92,246,0.1),0_0_64px_rgba(99,102,241,0.06)] backdrop-blur-[16px] sm:p-7";
const COPY_GLASS_CARD = `${CARD_GLASS_CORE} border border-violet-400/14 p-5 shadow-[0_8px_32px_rgba(0,0,0,0.38),0_0_40px_rgba(139,92,246,0.09)] backdrop-blur-[20px] transition-all duration-[250ms] ease-out hover:-translate-y-0.5 hover:border-violet-400/26`;
const COPY_FIELD = INPUT_FIELD;
const COPY_BTN_GLOW_SM =
  "inline-flex items-center justify-center gap-2 rounded-2xl bg-[linear-gradient(135deg,#a78bfa,#60efff)] px-3 py-2 text-xs font-semibold text-slate-900 shadow-[0_0_18px_rgba(167,139,250,0.35)] transition-all duration-[250ms] ease-out hover:-translate-y-0.5 hover:shadow-[0_0_28px_rgba(96,239,255,0.35)] active:scale-[0.98] disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0";

function mergeCopyTaskIntoList(prev, patch) {
  if (!patch || patch.id == null) return prev;
  const i = prev.findIndex((x) => x.id === patch.id);
  if (i < 0) return prev;
  const next = [...prev];
  next[i] = { ...next[i], ...patch };
  return next;
}

/** 服务端 status + 点击启动后的乐观 starting */
function resolveCopyDisplayStatus(task, optimisticStartIds) {
  const st = (task.status || "").toLowerCase();
  if (st === "starting") return "starting";
  if (optimisticStartIds[task.id] && st !== "running" && st !== "error" && st !== "paused") return "starting";
  if (st === "running" || st === "paused" || st === "error") return st;
  return "idle";
}

/** 最多保留日志条数（FIFO 丢弃最早） */
const MAX_LOG_ENTRIES = 500;
/** 距底部小于等于该像素视为「在底部」，恢复自动跟随滚动 */
const LOG_SCROLL_BOTTOM_THRESHOLD_PX = 56;

function capLogEntries(prev, entry) {
  const next = [...prev, entry];
  return next.length > MAX_LOG_ENTRIES ? next.slice(-MAX_LOG_ENTRIES) : next;
}

const MENU_ICONS = {
  用户增长: BarChart3,
  账号检测: UserCheck,
  目标群组: Users2,
  群组互动: MessageCircle,
  代理监控: Network,
  用户采集: UserSearch,
  消息Copy: Repeat2,
  用户管理: UserCog,
};

function SidebarMenuIcon({ name, className }) {
  const Icon = MENU_ICONS[name];
  if (!Icon) return null;
  return <Icon className={className} size={20} strokeWidth={2} aria-hidden />;
}

const TAB_HEADER_ICONS = {
  用户增长: BarChart3,
  账号检测: UserCheck,
  目标群组: Users2,
  群组互动: MessageCircle,
  代理监控: Network,
  用户采集: UserSearch,
  消息Copy: Repeat2,
  用户管理: UserCog,
};

const STAT_ICON_BOX = {
  growth: "bg-emerald-500/12 text-emerald-300 ring-1 ring-emerald-400/25 shadow-[0_0_20px_rgba(52,211,153,0.12)]",
  info: "bg-cyan-500/12 text-cyan-300 ring-1 ring-cyan-400/25 shadow-[0_0_20px_rgba(34,211,238,0.12)]",
  risk: "bg-rose-500/12 text-rose-300 ring-1 ring-rose-400/25 shadow-[0_0_20px_rgba(251,113,133,0.12)]",
};

/** 与卡片模块色一致：增长绿 / 日志蓝 / 风控紫红渐变数字 */
const STAT_NUM_CLASS = {
  growth: "stat-num-growth mt-1 text-3xl",
  info: "stat-num-log mt-1 text-3xl",
  risk: "stat-num-risk mt-1 text-3xl",
};

const STAT_NUM_CLASS_LG = {
  growth: "stat-num-growth mt-1 text-4xl",
  info: "stat-num-log mt-1 text-4xl",
  risk: "stat-num-risk mt-1 text-4xl",
};

const STAT_CARD_ACCENT = { growth: "growth", info: "log", risk: "risk" };

function StatTile({ title, value, icon: Icon, tone = "growth" }) {
  const accent = STAT_CARD_ACCENT[tone] || "default";
  return (
    <div className={`${cardShellClass(accent)} !p-4`}>
      <div className="flex items-start gap-3">
        <div className={`grid h-11 w-11 shrink-0 place-items-center rounded-xl ${STAT_ICON_BOX[tone]}`}>
          <Icon size={22} strokeWidth={2} aria-hidden />
        </div>
        <div className="min-w-0 flex-1">
          <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">{title}</p>
          <p className={STAT_NUM_CLASS[tone] || STAT_NUM_CLASS.growth}>{value}</p>
        </div>
      </div>
    </div>
  );
}

function StatTileLg({ title, value, icon: Icon, tone = "growth" }) {
  const accent = STAT_CARD_ACCENT[tone] || "default";
  return (
    <div className={`${cardShellClass(accent)} !p-4`}>
      <div className="flex items-start gap-3">
        <div className={`grid h-12 w-12 shrink-0 place-items-center rounded-xl ${STAT_ICON_BOX[tone]}`}>
          <Icon size={24} strokeWidth={2} aria-hidden />
        </div>
        <div className="min-w-0 flex-1">
          <p className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">{title}</p>
          <p className={STAT_NUM_CLASS_LG[tone] || STAT_NUM_CLASS_LG.growth}>{value}</p>
        </div>
      </div>
    </div>
  );
}

/** 圆环扇区路径：角度自正上方起顺时针（度） */
function donutSegmentPath(cx, cy, rInner, rOuter, deg0, deg1) {
  const rad = Math.PI / 180;
  const p = (r, d) => {
    const a = (d - 90) * rad;
    return { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
  };
  const o0 = p(rOuter, deg0);
  const o1 = p(rOuter, deg1);
  const i1 = p(rInner, deg1);
  const i0 = p(rInner, deg0);
  const sweep = deg1 - deg0;
  const large = sweep > 180 ? 1 : 0;
  return `M ${o0.x} ${o0.y} A ${rOuter} ${rOuter} 0 ${large} 1 ${o1.x} ${o1.y} L ${i1.x} ${i1.y} A ${rInner} ${rInner} 0 ${large} 0 ${i0.x} ${i0.y} Z`;
}

/**
 * 用户增长 · 左侧账号池分布（纯展示：Donut + 横向比例条）
 * 数据：total_accounts / active_accounts / limited_accounts / risk_accounts
 */
function AccountPoolNeonDistributionPanel({
  total_accounts,
  active_accounts,
  limited_accounts,
  risk_accounts,
}) {
  const [hoverKey, setHoverKey] = useState(null);
  const total = total_accounts;
  const active = active_accounts;
  const limited = limited_accounts;
  const risk = risk_accounts;

  const pct = (n) => (total > 0 ? (n / total) * 100 : 0);
  const pctStr = (n) => (total > 0 ? ((n / total) * 100).toFixed(1) : "0.0");

  const segments = useMemo(() => {
    if (total <= 0) return [];
    let a = 0;
    const out = [];
    const push = (key, count, fill, glow) => {
      if (count <= 0) return;
      const span = (count / total) * 360;
      const d0 = a;
      const d1 = a + span;
      out.push({
        key,
        d: donutSegmentPath(50, 50, 28, 42, d0, d1),
        fill,
        glow,
        count,
      });
      a = d1;
    };
    push("active", active, "url(#poolGradActive)", "rgba(52,211,153,0.55)");
    push("limited", limited, "url(#poolGradLimited)", "rgba(250,204,21,0.5)");
    push("risk", risk, "url(#poolGradRisk)", "rgba(251,113,133,0.55)");
    return out;
  }, [total, active, limited, risk]);

  const rows = [
    {
      key: "active",
      zh: "可用",
      en: "AVAILABLE",
      value: active,
      pct: pct(active),
      barClass: "bg-gradient-to-r from-emerald-400 via-teal-300 to-cyan-400",
      barGlow: "shadow-[0_0_12px_rgba(52,211,153,0.45)]",
      numClass: "text-emerald-300",
    },
    {
      key: "limited",
      zh: "受限",
      en: "LIMITED",
      value: limited,
      pct: pct(limited),
      barClass: "bg-gradient-to-r from-amber-400 via-yellow-300 to-orange-400",
      barGlow: "shadow-[0_0_12px_rgba(250,204,21,0.4)]",
      numClass: "text-amber-300",
    },
    {
      key: "risk",
      zh: "风控",
      en: "RISK",
      value: risk,
      pct: pct(risk),
      barClass: "bg-gradient-to-r from-rose-500 via-fuchsia-500 to-violet-500",
      barGlow: "shadow-[0_0_12px_rgba(251,113,133,0.45)]",
      numClass: "text-rose-300",
    },
  ];

  return (
    <div
      className="relative flex h-[240px] max-h-[260px] min-h-[200px] w-full shrink-0 flex-col overflow-hidden rounded-2xl border border-cyan-500/15 bg-[linear-gradient(145deg,rgba(6,10,20,0.92)_0%,rgba(8,14,28,0.88)_45%,rgba(10,8,22,0.9)_100%)] shadow-[0_0_1px_rgba(0,255,200,0.12),0_8px_40px_rgba(0,0,0,0.5),inset_0_1px_0_rgba(255,255,255,0.06)] backdrop-blur-[20px]"
      aria-label="账号池分布"
    >
      <div
        className="pointer-events-none absolute -left-16 top-1/2 h-48 w-48 -translate-y-1/2 rounded-full bg-emerald-500/10 blur-[64px]"
        aria-hidden
      />
      <div
        className="pointer-events-none absolute -right-12 -top-8 h-40 w-40 rounded-full bg-fuchsia-500/10 blur-[56px]"
        aria-hidden
      />
      <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-cyan-400/35 to-transparent" aria-hidden />

      <div className="relative z-[1] flex min-h-0 flex-1 flex-col gap-2 px-3 py-2.5 sm:px-3.5">
        <div className="flex shrink-0 items-center justify-between gap-2 border-b border-white/[0.06] pb-1.5">
          <span className="font-log text-[10px] font-bold uppercase tracking-[0.22em] text-cyan-400/75">
            pool.distribution
          </span>
          <span className="font-log text-[9px] tabular-nums text-slate-500">LIVE · DB</span>
        </div>

        <div className="flex min-h-0 flex-1 items-stretch gap-3">
          <div className="relative w-[118px] shrink-0 sm:w-[128px]">
            <svg
              viewBox="0 0 100 100"
              className="h-full w-full max-h-[132px] overflow-visible drop-shadow-[0_0_20px_rgba(34,211,238,0.15)]"
              role="img"
              aria-label="账号状态圆环"
            >
              <defs>
                <linearGradient id="poolGradActive" x1="0%" y1="0%" x2="100%" y2="100%">
                  <stop offset="0%" stopColor="#34d399" />
                  <stop offset="100%" stopColor="#2dd4bf" />
                </linearGradient>
                <linearGradient id="poolGradLimited" x1="0%" y1="0%" x2="100%" y2="100%">
                  <stop offset="0%" stopColor="#fbbf24" />
                  <stop offset="100%" stopColor="#f97316" />
                </linearGradient>
                <linearGradient id="poolGradRisk" x1="0%" y1="0%" x2="100%" y2="100%">
                  <stop offset="0%" stopColor="#fb7185" />
                  <stop offset="100%" stopColor="#c084fc" />
                </linearGradient>
              </defs>
              {total <= 0 ? (
                <circle
                  cx="50"
                  cy="50"
                  r="35"
                  fill="none"
                  stroke="rgb(51 65 85 / 0.85)"
                  strokeWidth="14"
                  className="transition-all duration-[600ms] ease-in-out"
                />
              ) : (
                segments.map((s) => {
                  const dim = hoverKey && hoverKey !== s.key;
                  return (
                    <path
                      key={s.key}
                      d={s.d}
                      fill={s.fill}
                      stroke="rgba(255,255,255,0.12)"
                      strokeWidth="0.35"
                      className="cursor-pointer transition-all duration-[600ms] ease-in-out"
                      style={{
                        opacity: dim ? 0.32 : 1,
                        filter: hoverKey === s.key ? `drop-shadow(0 0 10px ${s.glow})` : "none",
                      }}
                      onMouseEnter={() => setHoverKey(s.key)}
                      onMouseLeave={() => setHoverKey(null)}
                    />
                  );
                })
              )}
            </svg>
            <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center pt-0.5">
              <span className="font-log text-[9px] font-bold uppercase tracking-[0.28em] text-cyan-500/80">TOTAL</span>
              <span className="mt-0.5 bg-gradient-to-b from-white to-slate-300 bg-clip-text text-2xl font-bold tabular-nums text-transparent drop-shadow-[0_0_12px_rgba(255,255,255,0.15)]">
                {total}
              </span>
            </div>
            {hoverKey && total > 0 ? (
              <div className="pointer-events-none absolute bottom-full left-1/2 z-10 mb-1.5 w-max -translate-x-1/2 rounded-lg border border-cyan-400/30 bg-[#050a14]/95 px-2.5 py-1.5 text-center shadow-[0_0_20px_rgba(34,211,238,0.25)] backdrop-blur-md">
                <p className="text-[10px] font-semibold text-slate-200">
                  {hoverKey === "active" ? "可用" : hoverKey === "limited" ? "受限" : "风控"}
                </p>
                <p className="mt-0.5 font-mono text-[11px] tabular-nums text-cyan-300/95">
                  {hoverKey === "active" ? active : hoverKey === "limited" ? limited : risk} ·{" "}
                  {pctStr(hoverKey === "active" ? active : hoverKey === "limited" ? limited : risk)}%
                </p>
              </div>
            ) : null}
          </div>

          <div className="flex min-w-0 flex-1 flex-col justify-center gap-2.5 py-0.5">
            {rows.map((r) => {
              const dim = hoverKey && hoverKey !== r.key;
              return (
                <div
                  key={r.key}
                  className="group/row cursor-default transition-opacity duration-[600ms] ease-in-out"
                  style={{ opacity: dim ? 0.38 : 1 }}
                  onMouseEnter={() => setHoverKey(r.key)}
                  onMouseLeave={() => setHoverKey(null)}
                  title={`${r.zh} 占比 ${r.pct.toFixed(1)}%`}
                >
                  <div className="mb-1 flex items-baseline justify-between gap-2">
                    <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">{r.en}</span>
                    <span className={`font-mono text-sm font-bold tabular-nums ${r.numClass}`}>{r.value}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="w-9 shrink-0 text-[11px] font-medium text-slate-400">{r.zh}</span>
                    <div className="relative h-2 min-w-0 flex-1 overflow-hidden rounded-full bg-white/[0.06] ring-1 ring-white/[0.05]">
                      <div
                        className={`h-full rounded-full transition-all duration-[600ms] ease-in-out ${r.barClass} ${r.barGlow}`}
                        style={{ width: `${Math.min(100, r.pct)}%` }}
                      />
                    </div>
                  </div>
                  <p className="invisible mt-0.5 text-[9px] text-slate-500 group-hover/row:visible">
                    占比 {r.pct.toFixed(1)}%
                  </p>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

function formatGrowthGroupHandle(raw) {
  if (raw == null || !String(raw).trim()) return "—";
  const u = String(raw).trim();
  return u.startsWith("@") ? u : `@${u}`;
}

/** 从进度日志解析 [i/n] 与粗略成功/失败条数（仅展示，不参与业务） */
function parseGrowthTaskProgressLines(lines) {
  const arr = Array.isArray(lines) ? lines : [];
  let qi = 0;
  let qn = 0;
  const reBracket = /\[(\d+)\/(\d+)\]/g;
  for (const line of arr) {
    const s = String(line);
    reBracket.lastIndex = 0;
    let m;
    while ((m = reBracket.exec(s)) !== null) {
      qi = Number(m[1]) || 0;
      qn = Number(m[2]) || 0;
    }
  }
  let progressPct = null;
  if (qn > 0) progressPct = Math.min(100, Math.round((qi / qn) * 100));
  let success = 0;
  let failed = 0;
  for (const line of arr) {
    const s = String(line);
    if (/拉入群组.+成功/.test(s)) success += 1;
    if (/用户 .+ 失败[:：]/.test(s)) failed += 1;
  }
  return { queueIndex: qi, queueTotal: qn, progressPct, success, failed };
}

/** 用户增长 · 执行状态（纯展示，数据来自轮询快照与表单上下文） */
function GrowthExecutionStatusModule({ selectedGroup, snapshot, taskRunning, taskHighlight }) {
  const groupDisplay = formatGrowthGroupHandle(snapshot?.groupRaw ?? selectedGroup);
  const phoneDisplay =
    snapshot?.phoneDisplay ??
    taskHighlight?.connecting ??
    taskHighlight?.active ??
    taskHighlight?.previous ??
    "—";
  const uiStatus = snapshot?.uiStatus ?? "WAITING";
  const taskKind = snapshot?.taskKind ?? "拉人";
  const indeterminate = Boolean(snapshot?.indeterminate);
  const progressPct = snapshot?.progressPct;
  const success = snapshot?.success ?? 0;
  const failed = snapshot?.failed ?? 0;
  const errorHint = snapshot?.errorHint;

  const chipClass =
    uiStatus === "ERROR"
      ? "border-rose-400/50 bg-rose-500/[0.12] text-rose-100 shadow-[0_0_14px_rgba(251,113,133,0.35)]"
      : uiStatus === "RUNNING"
        ? "border-cyan-400/45 bg-cyan-500/[0.1] text-cyan-100 shadow-[0_0_16px_rgba(34,211,238,0.3)]"
        : "border-violet-400/30 bg-violet-500/[0.08] text-slate-200 shadow-[0_0_12px_rgba(139,92,246,0.18)]";

  const frameGlow =
    uiStatus === "ERROR"
      ? "shadow-[0_0_28px_rgba(251,113,133,0.14),inset_0_0_0_1px_rgba(251,113,133,0.22)]"
      : uiStatus === "RUNNING"
        ? "shadow-[0_0_28px_rgba(34,211,238,0.12),inset_0_0_0_1px_rgba(34,211,238,0.2)]"
        : "shadow-[inset_0_0_0_1px_rgba(139,92,246,0.14)]";

  const pctLabel =
    indeterminate || progressPct == null ? "—" : `${Math.min(100, Math.max(0, progressPct))}%`;

  return (
    <section
      className={`relative flex h-[150px] min-h-[140px] max-h-[170px] min-w-0 shrink-0 grow-0 flex-col overflow-hidden rounded-xl border border-white/[0.09] bg-[linear-gradient(160deg,rgba(6,10,18,0.92)_0%,rgba(10,14,24,0.88)_100%)] px-2.5 py-2 backdrop-blur-[18px] transition-[box-shadow,border-color] duration-500 ease-out ${frameGlow}`}
      aria-label="执行状态"
    >
      <div
        className="pointer-events-none absolute -right-8 top-0 h-24 w-24 rounded-full bg-cyan-500/10 blur-3xl"
        aria-hidden
      />
      <div className="relative z-[1] flex h-full min-h-0 flex-col justify-between gap-1">
        <div className="shrink-0 space-y-1">
          <div className="flex items-center justify-between gap-2 border-b border-white/[0.06] pb-1">
            <div className="flex min-w-0 items-center gap-1.5">
              <Activity className="h-3.5 w-3.5 shrink-0 text-cyan-400/80" aria-hidden />
              <span className="truncate font-log text-[10px] font-bold uppercase tracking-[0.2em] text-slate-400">
                execution.status
              </span>
            </div>
            <span
              className={`shrink-0 rounded-md border px-2 py-0.5 font-log text-[9px] font-bold uppercase tracking-widest transition-all duration-500 ease-out ${chipClass}`}
            >
              {uiStatus}
            </span>
          </div>

          <div className="grid grid-cols-2 gap-x-2 gap-y-0.5 text-[10px] leading-tight">
            <div className="text-slate-500">账号</div>
            <div className="truncate text-right font-mono text-slate-200 tabular-nums" title={phoneDisplay}>
              {phoneDisplay}
            </div>
            <div className="text-slate-500">任务</div>
            <div className="truncate text-right font-medium text-slate-300">{taskKind}</div>
            <div className="text-slate-500">群组</div>
            <div className="truncate text-right font-mono text-cyan-300/90" title={groupDisplay}>
              {groupDisplay}
            </div>
          </div>
        </div>

        <div className="shrink-0 space-y-1">
          <div>
            <div className="mb-0.5 flex items-center justify-between gap-2">
              <span className="text-[9px] font-semibold uppercase tracking-wider text-slate-500">progress</span>
              <span className="font-mono text-[10px] tabular-nums text-cyan-300/90">{pctLabel}</span>
            </div>
            <div className="relative h-2 overflow-hidden rounded-full bg-white/[0.07] ring-1 ring-white/[0.06]">
              {indeterminate ? (
                <div className="growth-exec-progress-indeterminate" />
              ) : (
                <div
                  className="h-full rounded-full bg-gradient-to-r from-cyan-500 via-sky-400 to-violet-500 shadow-[0_0_14px_rgba(34,211,238,0.45)] transition-[width] duration-700 ease-out"
                  style={{ width: `${progressPct != null ? Math.min(100, Math.max(0, progressPct)) : 0}%` }}
                />
              )}
            </div>
          </div>

          <div className="flex items-center justify-between gap-2 border-t border-white/[0.05] pt-0.5 font-mono text-[10px] tabular-nums">
            <span className="text-emerald-400/90">OK · {success}</span>
            <span className="text-rose-400/90">FAIL · {failed}</span>
          </div>
          {uiStatus === "ERROR" && errorHint ? (
            <p className="line-clamp-2 text-[9px] leading-snug text-rose-300/90" title={errorHint}>
              {errorHint}
            </p>
          ) : null}
        </div>
      </div>
    </section>
  );
}

/** 账号检测列表行：玻璃卡 + 头像脉冲光圈 + 标签可发光 */
function AccountInspectRow({ phoneLine, subLine, badge, variant, right }) {
  const rowMod = variant === "active" ? "active" : variant === "limited" ? "limited" : "banned";
  const wrapMod =
    variant === "active" ? "account-inspect-avatar-wrap--active" : variant === "limited" ? "account-inspect-avatar-wrap--limited" : "account-inspect-avatar-wrap--banned";
  const innerMod =
    variant === "active"
      ? "account-inspect-avatar-inner--active"
      : variant === "limited"
        ? "account-inspect-avatar-inner--limited"
        : "account-inspect-avatar-inner--banned";
  return (
    <div className={`account-inspect-row account-inspect-row--${rowMod}`}>
      <div className={`account-inspect-avatar-wrap ${wrapMod}`}>
        <div className={`account-inspect-avatar-inner ${innerMod}`}>
          <UserCircle size={20} strokeWidth={1.75} aria-hidden />
        </div>
      </div>
      <div className="min-w-0 flex-1">
        <div className="font-medium text-slate-100">{phoneLine}</div>
        {subLine ? <div className="mt-0.5 text-xs text-slate-500">{subLine}</div> : null}
        <div className="mt-2 flex flex-wrap items-center justify-between gap-2">
          {badge}
          {right}
        </div>
      </div>
    </div>
  );
}

/** 账号检测 · 状态列容器（渐变玻璃 + 头部统计） */
function AccountMonitorColumn({ variant, title, titleEn, count, countClassName, children }) {
  const colClass =
    variant === "active"
      ? "account-console-column account-console-column--active"
      : variant === "limited"
        ? "account-console-column account-console-column--limited"
        : "account-console-column account-console-column--risk";
  return (
    <section className={colClass}>
      <div className="account-console-column-head">
        <div className="min-w-0">
          <h3 className="text-base font-semibold tracking-tight text-slate-100">{title}</h3>
          <p className="mt-0.5 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">{titleEn}</p>
        </div>
        <span className={`shrink-0 tabular-nums leading-none ${countClassName}`}>{count}</span>
      </div>
      <div className="account-console-column-body growth-scroll max-h-[500px] space-y-2.5 overflow-y-auto pr-0.5">{children}</div>
    </section>
  );
}

/** 任务控制 · READY / RUNNING / COMPLETED 状态条 */
function TaskControlStatusBar({ phase }) {
  const Item = ({ match, en, zh }) => {
    const on = phase === match;
    return (
      <span className={`task-status-chip ${on ? `task-status-on-${match}` : "task-status-chip--dim"}`}>
        <span className="font-log text-[9px] font-bold tracking-widest">{en}</span>
        <span className="text-[10px] font-medium leading-tight">{zh}</span>
      </span>
    );
  };
  return (
    <div className="flex flex-wrap items-center gap-2 sm:justify-end" role="status" aria-live="polite">
      <Item match="ready" en="READY" zh="就绪" />
      <Item match="running" en="RUNNING" zh="执行中" />
      <Item match="completed" en="COMPLETED" zh="已完成" />
    </div>
  );
}

function Card({ title, children, right, className = "", accent = "default", shellClass = null }) {
  const shell = shellClass ?? cardShellClass(accent);
  return (
    <div className={`${shell} ${className}`}>
      <div className="mb-4 flex items-center justify-between gap-3">
        <h3 className="text-[15px] font-semibold tracking-tight text-slate-100">{title}</h3>
        {right}
      </div>
      {children}
    </div>
  );
}

function Badge({ status, glow = false }) {
  const s = (status || "").toLowerCase();
  const cls =
    s.includes("正常") || s === "normal" || s === "active" || s.includes("可用")
      ? "border-emerald-400/35 bg-emerald-500/10 text-emerald-200"
      : s.includes("风控") || s === "banned" || s.includes("疑似") || s.includes("长期受限")
        ? "border-rose-400/35 bg-rose-500/10 text-rose-200"
        : "border-amber-400/35 bg-amber-500/10 text-amber-200";
  let glowCls = "";
  if (glow) {
    if (s.includes("正常") || s === "normal" || s === "active" || s.includes("可用")) glowCls = "badge-glow-available";
    else if (s.includes("风控") || s === "banned" || s.includes("疑似") || s.includes("长期受限")) glowCls = "badge-glow-risk";
    else glowCls = "badge-glow-warn";
  }
  return <span className={`rounded-full border px-2.5 py-0.5 text-[11px] font-medium ${cls} ${glowCls}`}>{status}</span>;
}

/** 目标群组页 · 视觉焦点 Hero（多层光、中心伪元素光核、光圈 + 图标） */
function GroupsHeroCard({ group }) {
  const today = Number(group.today_added) || 0;
  const total = Number(group.total_added) || 0;
  const members = Number(group.members_count) || 0;
  const yest = Number(group.yesterday_added) || 0;
  const yestLeave = Number(group.yesterday_leave_count ?? group.yesterday_left) || 0;
  const netGrowth =
    group.net_growth !== undefined && group.net_growth !== null
      ? Number(group.net_growth)
      : yest - yestLeave;
  const leaveHeroAnomaly = yestLeave > 30;
  const title = group.title || group.username;
  const handle = group.display_handle || group.username;
  const isTodayLeader = today > 0;
  return (
    <section className="group-hero-card w-full" aria-label="重点群组">
      <div className="relative z-[1] flex flex-col gap-8 p-6 md:flex-row md:items-center md:justify-between md:gap-12 md:px-10 md:py-9">
        <div className="flex min-w-0 flex-1 flex-col gap-6 sm:flex-row sm:items-center">
          <div className="relative mx-auto grid h-[132px] w-[132px] shrink-0 place-items-center sm:mx-0" aria-hidden>
            <div className="pointer-events-none absolute inset-0 grid place-items-center">
              <div className="group-hero-orbit h-[118px] w-[118px] rounded-full border border-dashed border-emerald-400/35 opacity-60" />
            </div>
            <div
              className="pointer-events-none absolute inset-[14px] rounded-full border border-cyan-400/25 group-hero-halo"
              style={{ animationDelay: "0.6s" }}
            />
            <div className="pointer-events-none absolute inset-[26px] rounded-full border border-white/12 opacity-90" />
            <div className="relative z-[2] grid h-[52px] w-[52px] place-items-center rounded-full bg-gradient-to-br from-emerald-400/45 to-cyan-500/35 shadow-[0_0_36px_rgba(0,255,200,0.45)] ring-1 ring-white/25">
              <Users2 className="h-6 w-6 text-white drop-shadow-[0_0_10px_rgba(255,255,255,0.45)]" strokeWidth={2} />
            </div>
          </div>
          <div className="min-w-0 flex-1 text-center sm:text-left">
            <div className="mb-2 flex flex-wrap items-center justify-center gap-2 sm:justify-start">
              <span className="inline-flex items-center gap-1.5">
                <Sparkles
                  className="h-3.5 w-3.5 shrink-0 animate-pulse text-cyan-400 drop-shadow-[0_0_10px_rgba(34,211,238,0.55)]"
                  aria-hidden
                />
                <span className="group-hero-badge">{isTodayLeader ? "今日增长最快" : "推荐群组"}</span>
              </span>
              <span className="rounded-full border border-white/10 bg-white/[0.04] px-2 py-0.5 text-[10px] font-medium text-slate-400">
                {isTodayLeader ? "按今日拉人" : "按累计拉人"}
              </span>
            </div>
            <h2 className="truncate text-xl font-bold tracking-tight text-white md:text-2xl">{title}</h2>
            <p className="mt-1 truncate font-log text-sm text-slate-400">{handle}</p>
            <div className="mt-4 flex flex-wrap items-center justify-center gap-4 sm:justify-start">
              <div className="flex items-center gap-1.5 text-xs text-slate-500">
                <Users className="h-3.5 w-3.5 text-emerald-400/80" aria-hidden />
                <span>成员 {members.toLocaleString("zh-CN")}</span>
              </div>
              <div className="flex items-center gap-1.5 text-xs text-slate-500">
                <BarChart3 className="h-3.5 w-3.5 text-sky-400/80" aria-hidden />
                <span>累计拉人 {total.toLocaleString("zh-CN")}</span>
              </div>
              <div className="flex items-center gap-1.5 text-xs text-slate-500">
                <TrendingUp className="h-3.5 w-3.5 text-teal-400/80" aria-hidden />
                <span>昨日拉人 {yest.toLocaleString("zh-CN")}</span>
              </div>
              <div
                className={`flex items-center gap-1.5 text-xs ${
                  leaveHeroAnomaly ? "text-rose-300 drop-shadow-[0_0_10px_rgba(251,113,133,0.45)]" : "text-slate-500"
                }`}
              >
                <span aria-hidden>{leaveHeroAnomaly ? "⚠️" : "↩"}</span>
                <span>昨日离群 {yestLeave.toLocaleString("zh-CN")}</span>
                {leaveHeroAnomaly ? (
                  <span className="rounded-full border border-rose-400/40 bg-rose-950/40 px-1.5 py-0.5 text-[9px] font-bold text-rose-200">
                    异常
                  </span>
                ) : null}
              </div>
              <div
                className={`flex items-center gap-1.5 text-xs font-semibold tabular-nums ${
                  netGrowth > 0
                    ? "text-emerald-300 drop-shadow-[0_0_8px_rgba(52,211,153,0.35)]"
                    : netGrowth < 0
                      ? "text-rose-300 drop-shadow-[0_0_8px_rgba(251,113,133,0.3)]"
                      : "text-slate-500"
                }`}
              >
                <span>净增长 {netGrowth > 0 ? "+" : ""}{netGrowth.toLocaleString("zh-CN")}</span>
              </div>
            </div>
          </div>
        </div>
        <div className="flex shrink-0 flex-col items-center gap-1 border-t border-white/[0.08] pt-6 md:border-l md:border-t-0 md:pl-10 md:pt-0">
          <span className="text-[11px] font-medium uppercase tracking-wider text-slate-500">今日拉人</span>
          <p className="group-hero-metric-xl tabular-nums">{today.toLocaleString("zh-CN")}</p>
          <span className="text-xs text-slate-500">核心增长指标</span>
        </div>
      </div>
    </section>
  );
}

function targetGroupAvatarInitial(title) {
  const t = (title || "").trim();
  if (!t) return "G";
  const ch = t[0];
  return /[a-zA-Z0-9]/.test(ch) ? ch.toUpperCase() : ch;
}

/** 目标群组 · Web3 / 金融 Dashboard 风格卡片 */
function TargetGroupDashboardCard({ group, onUpdateDailyLimit }) {
  const g = group;
  const title = g.title || g.username;
  const handleRaw = g.display_handle || g.username;
  const handle = handleRaw.startsWith("@") ? handleRaw : `@${String(handleRaw).replace(/^@/, "")}`;
  const members = Number(g.members_count) || 0;
  const total = Number(g.total_added) || 0;
  const today = Number(g.today_added) || 0;
  const yest = Number(g.yesterday_added) || 0;
  const yesterdayLeave = Number(g.yesterday_leave_count ?? g.yesterday_left) || 0;
  const netGrowth =
    g.net_growth !== undefined && g.net_growth !== null ? Number(g.net_growth) : yest - yesterdayLeave;
  const leaveAnomaly = yesterdayLeave > 30;
  const limited = g.status === "limited";
  const initial = targetGroupAvatarInitial(title);

  const netValueClass =
    netGrowth > 0 ? "tg-dash-net-value--pos" : netGrowth < 0 ? "tg-dash-net-value--neg" : "tg-dash-net-value--zero";

  return (
    <article className="tg-dash-group-card">
      <div className="flex gap-3.5">
        <div className="tg-dash-group-avatar" aria-hidden>
          <span className="tg-dash-group-avatar-inner">
            <span className="tg-dash-group-avatar-letter">{initial}</span>
          </span>
        </div>
        <div className="min-w-0 flex-1 pt-0.5">
          <h3 className="truncate text-[15px] font-bold tracking-tight text-slate-50">{title}</h3>
          <p className="mt-0.5 truncate text-[12px] font-medium text-slate-500/75">{handle}</p>
        </div>
      </div>

      {/* 顶部：当前人数 · 累计拉人 */}
      <div className="mt-5 grid grid-cols-2 gap-3">
        <div className="rounded-xl border border-cyan-400/10 bg-[rgba(0,0,0,0.22)] px-3 py-3 backdrop-blur-sm">
          <div className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-slate-500/90">
            <span aria-hidden>👥</span>
            <span>当前人数</span>
          </div>
          <p className="stat-num-dash-members mt-1.5 text-2xl tabular-nums leading-none sm:text-[1.65rem]">
            {members.toLocaleString("zh-CN")}
          </p>
        </div>
        <div className="rounded-xl border border-violet-400/10 bg-[rgba(0,0,0,0.22)] px-3 py-3 backdrop-blur-sm">
          <div className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-slate-500/90">
            <span aria-hidden>📈</span>
            <span>累计拉人</span>
          </div>
          <p className="stat-num-dash-pull mt-1.5 text-xl font-bold tabular-nums leading-none sm:text-2xl">
            {total.toLocaleString("zh-CN")}
          </p>
        </div>
      </div>

      {/* 拉人 · 离群：无边框数据流（横向 label + value + 渐变分割线） */}
      <div className="tg-dash-metrics-stream mt-4">
        <div className="tg-dash-gradient-rule-h" aria-hidden />
        <p className="tg-dash-stream-head">拉人 · 离群</p>
        <div className="flex min-h-[3.25rem] w-full items-stretch">
          <div className="tg-dash-metric-item flex min-w-0 flex-1 flex-col justify-center gap-1 px-1 sm:px-2">
            <span className="tg-dash-stream-label">今日拉人</span>
            <span className="tg-dash-stream-value">{today.toLocaleString("zh-CN")}</span>
          </div>
          <div className="tg-dash-gradient-rule-v shrink-0" aria-hidden />
          <div className="tg-dash-metric-item flex min-w-0 flex-1 flex-col justify-center gap-1 px-1 sm:px-2">
            <span className="tg-dash-stream-label">昨日拉人</span>
            <span className="tg-dash-stream-value">{yest.toLocaleString("zh-CN")}</span>
          </div>
          <div className="tg-dash-gradient-rule-v shrink-0" aria-hidden />
          <div
            className={`tg-dash-metric-item relative flex min-w-0 flex-1 flex-col justify-center gap-1 px-1 sm:px-2 ${
              leaveAnomaly ? "tg-dash-metric-item--anomaly" : ""
            }`}
          >
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="tg-dash-stream-label">昨日离群</span>
              {leaveAnomaly ? (
                <span className="tg-dash-anomaly-badge" title="昨日离群人数大于 30">
                  ⚠️ 异常
                </span>
              ) : null}
            </div>
            <span
              className={
                leaveAnomaly ? "tg-dash-stream-value tg-dash-stream-value--anomaly" : "tg-dash-stream-value"
              }
            >
              {yesterdayLeave.toLocaleString("zh-CN")}
            </span>
          </div>
        </div>
      </div>

      {/* 净增长：单独一行强调（无边框内嵌卡） */}
      <div className="tg-dash-net-row tg-dash-net-row--emphasis">
        <div className="tg-dash-gradient-rule-h" aria-hidden />
        <div className="flex flex-col gap-2 pt-1 sm:flex-row sm:items-end sm:justify-between sm:gap-4">
          <span className="tg-dash-net-label">净增长（昨日拉人 − 昨日离群）</span>
          <p className={`${netValueClass} tg-dash-net-value-display`}>
            {netGrowth > 0 ? "+" : ""}
            {netGrowth.toLocaleString("zh-CN")}
          </p>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-end justify-between gap-3 border-t border-white/[0.06] pt-4">
        <label className="min-w-[120px] flex-1">
          <span className="mb-1.5 block text-[10px] font-semibold uppercase tracking-wider text-slate-500/75">
            单日限制
          </span>
          <input
            type="number"
            min={1}
            className="tg-dash-group-input w-full"
            defaultValue={g.daily_limit || 30}
            onBlur={(e) => onUpdateDailyLimit(g.id, e.target.value)}
          />
        </label>
        <div className="flex shrink-0 flex-col items-end gap-1">
          <span className="text-[9px] font-semibold uppercase tracking-widest text-slate-600">状态</span>
          <span className={limited ? "tg-dash-status-pill tg-dash-status-pill--limited" : "tg-dash-status-pill tg-dash-status-pill--active"}>
            {limited ? "LIMITED" : "ACTIVE"}
          </span>
        </div>
      </div>
    </article>
  );
}

const ENGAGEMENT_LAYER_ZH = { account: "账号", group: "群组", system: "系统" };

function formatEngagementLogLine(entry) {
  const t = entry.t ?? "";
  const account = entry.account ?? "";
  const group = entry.group ?? "";
  const emoji = entry.emoji ?? "";
  const message = entry.message ?? "";
  const layer = entry.layer ?? "";
  const progress = entry.progress ?? "";
  const layerZh = ENGAGEMENT_LAYER_ZH[layer] || "";
  let head = "";
  if (layerZh || progress) {
    const bits = [layerZh, progress].filter(Boolean);
    head = bits.length ? `[${bits.join(" · ")}] ` : "";
  }
  const emojiPart = emoji ? `${emoji}\u00A0` : "";
  const mid = emoji ? `${emojiPart}${group}` : group;
  const base = `${t}  ${head}${account} ${mid}`;
  return message ? `${base} · ${message}` : base;
}

function engagementLiveLogTone(level) {
  if (level === "success") return "text-emerald-400 drop-shadow-[0_0_8px_rgba(52,211,153,0.22)]";
  if (level === "warn") return "text-amber-300 drop-shadow-[0_0_8px_rgba(251,191,36,0.18)]";
  if (level === "info") return "text-cyan-200/80 drop-shadow-[0_0_6px_rgba(34,211,238,0.14)]";
  return "text-rose-400 drop-shadow-[0_0_8px_rgba(251,113,133,0.2)]";
}

function displayPhone(account) {
  if (account?.formatted_phone) return String(account.formatted_phone).replace(/^#/, "+");
  const digits = String(account?.phone || "").replace(/\D/g, "");
  return digits ? `+${digits}` : "+unknown";
}

function normalizePhoneKey(phone) {
  return String(phone || "").replace(/\D/g, "");
}

/** 用户增长侧栏行：ACTIVE / LIMITED / RISK / 执行中高亮用 executing */
function growthQueueRowKind(a, taskHighlight) {
  const pk = normalizePhoneKey(a.phone);
  if (a._queueKind === "echo") {
    if (a.status === "risk_suspected") return "risk";
    return "limited";
  }
  const activePk = taskHighlight?.active ? normalizePhoneKey(taskHighlight.active) : "";
  const connPk = taskHighlight?.connecting ? normalizePhoneKey(taskHighlight.connecting) : "";
  if (pk && (pk === activePk || pk === connPk)) return "executing";
  return "active";
}

/** 根据全文推断 type：info | success | error | warn */
function inferLogType(text) {
  const s = String(text);
  if (/\[INFO\]/i.test(s)) return "info";
  if (/\[ERROR\]|ERROR\b|登录超时|Internal Server Error|500|sync failed|任务失败/i.test(s)) return "error";
  if (/\[WARN(ING)?\]|WARNING\b|告警|\[WARN\]/.test(s)) return "warn";
  if (/\[SUCCESS\]|SUCCESS\b|登录成功|任务已排队|同步成功|执行成功|成功拉入|✓/i.test(s)) return "success";
  return "info";
}

/** 行级图标：成功 / 错误 / 加载 / 警告 / 信息 */
function inferLogRowKind(message, type) {
  if (type === "error") return "error";
  if (type === "success") return "success";
  if (type === "warn") return "warn";
  const s = String(message);
  if (/正在|加载|排队|执行中|拉取|提交|Connecting|登录中|\.\.\.|pending|同步中|刷新中|上传中|处理中/i.test(s)) return "loading";
  return "info";
}

function LogLineRow({ time, message, type }) {
  const termCls =
    type === "error"
      ? "terminal-log-line--error"
      : type === "warn"
        ? "terminal-log-line--warn"
        : type === "success"
          ? "terminal-log-line--success"
          : "terminal-log-line--info";
  const kind = inferLogRowKind(message, type);
  let rowIcon = null;
  if (kind === "error") rowIcon = <XCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 opacity-90" aria-hidden />;
  else if (kind === "success") rowIcon = <CheckCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 opacity-90" aria-hidden />;
  else if (kind === "loading") rowIcon = <Loader className="mt-0.5 h-3.5 w-3.5 shrink-0 animate-spin opacity-80" aria-hidden />;
  else if (kind === "warn") rowIcon = <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 opacity-90" aria-hidden />;
  else rowIcon = <Info className="mt-0.5 h-3.5 w-3.5 shrink-0 opacity-80" aria-hidden />;
  return (
    <div
      className={`log-line terminal-log-line flex gap-2 border-b border-emerald-500/[0.07] py-2 font-log text-[11px] leading-6 ${termCls}`}
    >
      <span className="w-[76px] shrink-0 tabular-nums text-slate-500/80">{time}</span>
      {rowIcon}
      <span className="min-w-0 flex-1 whitespace-pre-wrap break-words">{message}</span>
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState("用户增长");
  const [auth, setAuth] = useState({ username: "user", password: "user123" });
  const [profile, setProfile] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [accounts, setAccounts] = useState({
    active: [],
    limited: [],
    banned: [],
    recent_sidebar_echo: [],
    activity_feed: [],
  });
  /** 驱动左侧队列中非 ACTIVE 提示行按 status_changed_at 在约 60s 内过期（无需等待手动刷新） */
  const [sidebarEchoTick, setSidebarEchoTick] = useState(0);
  const [taskHighlight, setTaskHighlight] = useState({ active: null, previous: null, connecting: null });
  const [groups, setGroups] = useState([]);
  const [proxyData, setProxyData] = useState({ summary: { total: 0, idle: 0, used: 0, dead: 0 }, items: [] });
  const [users, setUsers] = useState([]);
  const [accountPaths, setAccountPaths] = useState([]);
  const [showPathModal, setShowPathModal] = useState(false);
  const [newPath, setNewPath] = useState("");
  const [editingPathId, setEditingPathId] = useState(null);
  const [selectedGroup, setSelectedGroup] = useState("");
  const [forcedGroups, setForcedGroups] = useState([]);
  const [removedGroups, setRemovedGroups] = useState([]);
  const [forceCandidate, setForceCandidate] = useState("");
  const logIdRef = useRef(0);
  const [logs, setLogs] = useState(() => {
    const id = logIdRef.current++;
    return [
      {
        id,
        time: new Date().toLocaleTimeString("zh-CN", { hour12: false }),
        message: "dashboard initialized",
        type: "info",
      },
    ];
  });
  const [msg, setMsg] = useState("");
  const logRef = useRef(null);
  const stickToBottomRef = useRef(true);
  const [uploadFile, setUploadFile] = useState(null);
  const [form, setForm] = useState({ users: "" });
  const [lastGroupMetadataSync, setLastGroupMetadataSync] = useState(null);
  const [taskRunning, setTaskRunning] = useState(false);
  /** 任务控制面板：就绪 / 执行中 / 已完成（成功后可短暂显示 Completed） */
  const [taskPanelPhase, setTaskPanelPhase] = useState("ready");
  const taskPanelPhaseTimerRef = useRef(null);
  /** 用户增长任务 · 执行状态条（与 start_task 轮询同步更新，仅展示） */
  const [growthExecSnapshot, setGrowthExecSnapshot] = useState(null);

  const refreshLoadingRef = useRef(false);
  const [refreshLoading, setRefreshLoading] = useState(false);
  /** 区分顶栏「刷新数据」与「强制同步」文案 */
  const [refreshPhase, setRefreshPhase] = useState(null);

  const authLoadingRef = useRef(false);
  const [authLoading, setAuthLoading] = useState(false);

  const uploadLoadingRef = useRef(false);
  const [uploadLoading, setUploadLoading] = useState(false);

  const loadUsersLoadingRef = useRef(false);
  const [loadUsersLoading, setLoadUsersLoading] = useState(false);

  const pathSubmitRef = useRef(false);
  const [pathSubmitLoading, setPathSubmitLoading] = useState(false);

  const [scraperForm, setScraperForm] = useState({ group_id: "", days: 7, max_messages: 5000 });
  const scraperLoadingRef = useRef(false);
  const [scraperLoading, setScraperLoading] = useState(false);
  const [scraperResult, setScraperResult] = useState(null);
  const [scraperTasks, setScraperTasks] = useState([]);
  const [scraperHistoryLoading, setScraperHistoryLoading] = useState(false);
  const [scraperDownloadTaskId, setScraperDownloadTaskId] = useState(null);
  const [scraperResultDownloadLoading, setScraperResultDownloadLoading] = useState(false);
  const [scraperAccount, setScraperAccount] = useState(null);
  const [showScraperAccountModal, setShowScraperAccountModal] = useState(false);
  const [scraperBindPhone, setScraperBindPhone] = useState("");
  const [scraperBindCode, setScraperBindCode] = useState("");
  const [scraperPhoneCodeHash, setScraperPhoneCodeHash] = useState("");
  const scraperSendCodeRef = useRef(false);
  const [scraperSendCodeLoading, setScraperSendCodeLoading] = useState(false);
  const scraperBindLoginRef = useRef(false);
  const [scraperBindLoginLoading, setScraperBindLoginLoading] = useState(false);
  /** 弹窗内状态条：success | error | loading | info */
  const [scraperModalBanner, setScraperModalBanner] = useState(null);
  const [scraperNeedPassword, setScraperNeedPassword] = useState(false);
  const [scraperBindPassword, setScraperBindPassword] = useState("");

  const [copyBots, setCopyBots] = useState([]);
  const [copyTasks, setCopyTasks] = useState([]);
  const [copyLogs, setCopyLogs] = useState([]);
  const [copyBotForm, setCopyBotForm] = useState({ api_id: "", api_hash: "", bot_token: "" });
  const [copyTaskForm, setCopyTaskForm] = useState({ source_channel: "", target_channel: "", bot_id: "" });
  const copyBotSubmitRef = useRef(false);
  const copyTaskSubmitRef = useRef(false);
  const [copyBotSaving, setCopyBotSaving] = useState(false);
  const [copyTaskSaving, setCopyTaskSaving] = useState(false);
  const copySessionFileInputRef = useRef(null);
  const [copySessionImportBotId, setCopySessionImportBotId] = useState(null);
  const [uploadingSessionBotId, setUploadingSessionBotId] = useState(null);
  /** 点击启动后、服务端尚未返回 starting 之前的乐观状态 */
  const [copyStartOptimistic, setCopyStartOptimistic] = useState({});

  const [engagementSelectedGroups, setEngagementSelectedGroups] = useState([]);
  const [engagementScanLimit, setEngagementScanLimit] = useState(300);
  const [engagementSubmitting, setEngagementSubmitting] = useState(false);
  const [engagementJobId, setEngagementJobId] = useState(null);
  const [engagementLiveLogs, setEngagementLiveLogs] = useState([]);
  const engagementLogRef = useRef(null);
  const [engagementGroupResolution, setEngagementGroupResolution] = useState(null);
  const [engagementRegisterLoading, setEngagementRegisterLoading] = useState(false);

  const isAdmin = useMemo(() => profile?.role === "admin", [profile]);
  const availableAccounts = useMemo(() => accounts.active || [], [accounts]);

  useEffect(() => {
    const id = setInterval(() => setSidebarEchoTick((n) => n + 1), 2000);
    return () => clearInterval(id);
  }, []);

  const sidebarQueueAccounts = useMemo(() => {
    const ECHO_TTL_MS = 60_000;
    const act = accounts.active || [];
    const recent = accounts.recent_sidebar_echo || [];
    const now = Date.now();
    const phones = new Set(act.map((a) => a.phone));
    const freshEcho = recent.filter((a) => {
      if (!a.phone || phones.has(a.phone)) return false;
      const raw = a.status_changed_at;
      if (!raw) return false;
      const t = Date.parse(raw);
      if (Number.isNaN(t)) return false;
      return now - t <= ECHO_TTL_MS;
    });
    return [
      ...act.map((a) => ({ ...a, _queueKind: "active" })),
      ...freshEcho.map((a) => ({ ...a, _queueKind: "echo" })),
    ];
  }, [accounts.active, accounts.recent_sidebar_echo, sidebarEchoTick]);

  const growthAccountPoolStats = useMemo(() => {
    const active_accounts = accounts.active?.length || 0;
    const limited_accounts = accounts.limited?.length || 0;
    const risk_accounts = accounts.banned?.length || 0;
    return {
      total_accounts: active_accounts + limited_accounts + risk_accounts,
      active_accounts,
      limited_accounts,
      risk_accounts,
    };
  }, [accounts.active, accounts.limited, accounts.banned]);

  const hiddenGroups = useMemo(
    () => groups.filter((g) => !g.available && !removedGroups.includes(g.username)),
    [groups, removedGroups]
  );
  const availableGroups = useMemo(() => {
    const base = groups
      .filter((g) => (g.available || forcedGroups.includes(g.username)) && !removedGroups.includes(g.username))
      .map((g) => g.username);
    return Array.from(new Set(base));
  }, [groups, forcedGroups, removedGroups]);

  const selectedGroupDropdownOptions = useMemo(
    () => [
      { value: "", label: "选择群组…" },
      ...availableGroups.map((username) => {
        const gi = groups.find((x) => x.username === username);
        const label = gi
          ? `${gi.title || gi.username} (${gi.display_handle || gi.username})`
          : username;
        return { value: username, label };
      }),
    ],
    [availableGroups, groups],
  );

  const forceCandidateDropdownOptions = useMemo(
    () => [
      { value: "", label: "隐藏群组…" },
      ...hiddenGroups.map((g) => ({
        value: g.username,
        label: `${g.title || g.username} (${g.display_handle || g.username})`,
      })),
    ],
    [hiddenGroups],
  );

  const scraperDaysDropdownOptions = useMemo(
    () =>
      [1, 3, 7, 14, 30, 90].map((d) => ({
        value: String(d),
        label: `最近 ${d} 天`,
      })),
    [],
  );

  const userRoleDropdownOptions = useMemo(
    () => [
      { value: "user", label: "user" },
      { value: "admin", label: "admin" },
    ],
    [],
  );

  const engagementGroupOptions = useMemo(
    () =>
      (groups || []).map((g) => ({
        value: g.username,
        label: `${g.title || g.username} (${g.display_handle || g.username})`,
      })),
    [groups],
  );

  const engagementAccountPoolCount = useMemo(() => {
    const lim = (accounts.limited || []).filter((x) => x.status === "daily_limited").length;
    return (accounts.active?.length || 0) + lim;
  }, [accounts.active, accounts.limited]);

  useEffect(() => {
    const allowed = new Set(engagementGroupOptions.map((o) => o.value));
    setEngagementSelectedGroups((prev) => {
      const next = prev.filter((v) => allowed.has(v));
      return next.length === prev.length ? prev : next;
    });
  }, [engagementGroupOptions]);

  const appendLog = (message) => {
    const time = new Date().toLocaleTimeString("zh-CN", { hour12: false });
    const type = inferLogType(message);
    const id = logIdRef.current++;
    setLogs((prev) => capLogEntries(prev, { id, time, message, type }));
  };

  const pushLogLine = (line) => {
    const raw = String(line);
    const m = raw.match(/^\[(\d{1,2}:\d{2}:\d{2})\]\s*(.*)$/);
    if (m) {
      const id = logIdRef.current++;
      const time = m[1];
      const message = m[2];
      const type = inferLogType(raw);
      setLogs((prev) => capLogEntries(prev, { id, time, message, type }));
    } else {
      appendLog(raw);
    }
  };

  const handleLogScroll = useCallback(() => {
    const el = logRef.current;
    if (!el) return;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    stickToBottomRef.current = distanceFromBottom <= LOG_SCROLL_BOTTOM_THRESHOLD_PX;
  }, []);

  useEffect(() => {
    if (!stickToBottomRef.current) return;
    const el = logRef.current;
    if (!el) return;
    const idRaf = requestAnimationFrame(() => {
      el.scrollTop = el.scrollHeight;
    });
    return () => cancelAnimationFrame(idRaf);
  }, [logs]);

  useEffect(() => {
    const { active, previous, connecting } = taskHighlight;
    if (!active && !previous && !connecting) return undefined;
    const t = setTimeout(
      () => setTaskHighlight({ active: null, previous: null, connecting: null }),
      60000,
    );
    return () => clearTimeout(t);
  }, [taskHighlight.active, taskHighlight.previous, taskHighlight.connecting]);

  const loadCopyData = useCallback(async () => {
    try {
      const token = localStorage.getItem("token");
      if (!token) return;
      const [cb, ct] = await Promise.all([api.listCopyBots(), api.listCopyTasks()]);
      setCopyBots(cb.bots || []);
      setCopyTasks(ct.tasks || []);
    } catch {
      /* Copy 模块可选，失败不阻断主同步 */
    }
  }, []);

  const refreshBase = async (opts = {}) => {
    const { skipMetadataSync = false, forceMetadataSync = false } = opts;
    let syncOk = null;
    try {
      if (!skipMetadataSync) {
        try {
          const sr = await api.syncGroupMetadata({ force: forceMetadataSync });
          if (sr?.skipped && sr?.reason === "recently_synced") {
            appendLog("群组元数据：24 小时内已同步，跳过");
            syncOk = true;
          } else if (sr?.ok && !sr?.skipped) {
            appendLog(`群组元数据已同步（更新 ${sr.updated ?? 0} 条）`);
            (sr.logs || []).slice(-20).forEach((line) => appendLog(`tg-sync | ${line}`));
            syncOk = true;
          } else if (sr?.ok === false) {
            appendLog(
              `数据同步失败：${sr.message || "Telegram 账号不可用或未连接"}；任务列表、账号与群组等仍来自数据库`,
            );
            syncOk = false;
          }
        } catch (e) {
          const aborted =
            e?.name === "AbortError" ||
            String(e?.message || "")
              .toLowerCase()
              .includes("aborted");
          appendLog(
            aborted
              ? "数据同步失败：等待 Telegram 响应超时；任务列表、账号与群组等仍来自数据库"
              : `数据同步失败：${e.message || "网络或服务异常"}；任务列表、账号与群组等仍来自数据库`,
          );
          syncOk = false;
        }
      }
      const baseCalls = [api.listTasks(), api.listAccounts(), api.listGroups(), api.listAccountPaths()];
      baseCalls.push(api.listProxies());
      const results = await Promise.all(baseCalls);
      const [t, a, g, ap, p] = results;
      setTasks(t.tasks || []);
      setAccounts({
        active: a.active || [],
        limited: a.limited || [],
        banned: a.banned || [],
        recent_sidebar_echo: a.recent_sidebar_echo || a.recent_limited_sidebar || [],
        activity_feed: a.activity_feed || [],
      });
      setGroups(g.groups || []);
      setLastGroupMetadataSync(g.last_metadata_sync || null);
      setAccountPaths(ap.items || []);
      setProxyData({
        summary: p?.summary || { total: 0, idle: 0, used: 0, dead: 0 },
        items: p?.items || [],
      });
      await loadCopyData();
      appendLog("sync ok");
      return { syncOk };
    } catch (e) {
      setMsg(e.message);
      appendLog(`sync failed | ${e.message}`);
      return { syncOk };
    }
  };

  const triggerRefresh = async (opts = {}, intent = "header") => {
    if (refreshLoadingRef.current) return undefined;
    refreshLoadingRef.current = true;
    setRefreshPhase(intent);
    setRefreshLoading(true);
    try {
      return await refreshBase(opts);
    } finally {
      refreshLoadingRef.current = false;
      setRefreshLoading(false);
      setRefreshPhase(null);
    }
  };

  const onForceSyncGroups = async () => {
    try {
      const r = await triggerRefresh({ skipMetadataSync: false, forceMetadataSync: true }, "force");
      if (r === undefined) return;
      if (r.syncOk === false) {
        setMsg("数据同步失败，界面仍显示数据库中的群组与统计");
      } else {
        setMsg("已从 Telegram 强制同步群组信息");
      }
    } catch (e) {
      setMsg(e.message);
    }
  };

  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) return;
    api
      .me()
      .then((r) => {
        const u = r.user || { username: r.username, role: r.role };
        setProfile({ username: u.username, role: u.role });
      })
      .then(() => refreshBase())
      .catch(() => localStorage.removeItem("token"));
  }, []);

  useEffect(() => () => clearTimeout(taskPanelPhaseTimerRef.current), []);

  useEffect(() => {
    if (taskRunning || taskPanelPhase !== "ready") return undefined;
    const t = window.setTimeout(() => setGrowthExecSnapshot(null), 7000);
    return () => window.clearTimeout(t);
  }, [taskRunning, taskPanelPhase]);

  useEffect(() => {
    if (tab !== "消息Copy" || !profile) return undefined;
    let cancelled = false;
    const tick = async () => {
      try {
        const [lr, tr] = await Promise.all([api.copyLogs(300), api.listCopyTasks()]);
        if (cancelled) return;
        if (Array.isArray(lr.logs)) setCopyLogs(lr.logs);
        if (Array.isArray(tr.tasks)) setCopyTasks(tr.tasks);
      } catch {
        /* ignore */
      }
    };
    tick();
    const id = setInterval(tick, 1500);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [tab, profile]);

  useEffect(() => {
    setCopyStartOptimistic((prev) => {
      const next = { ...prev };
      let changed = false;
      for (const key of Object.keys(prev)) {
        const tid = Number(key);
        if (Number.isNaN(tid)) continue;
        const row = copyTasks.find((x) => x.id === tid);
        if (row && row.status !== "starting") {
          delete next[key];
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  }, [copyTasks]);

  const loadScraperAccount = useCallback(async () => {
    try {
      const r = await api.getScraperAccount();
      setScraperAccount(r);
    } catch {
      setScraperAccount({ status: "not_logged" });
    }
  }, []);

  const loadScraperTasks = useCallback(async () => {
    if (!profile) return;
    setScraperHistoryLoading(true);
    try {
      const list = await api.listScraperTasks();
      setScraperTasks(Array.isArray(list) ? list : []);
    } catch {
      setScraperTasks([]);
    } finally {
      setScraperHistoryLoading(false);
    }
  }, [profile]);

  useEffect(() => {
    if (tab !== "用户采集" || !profile) return;
    loadScraperAccount();
    loadScraperTasks();
  }, [tab, profile, loadScraperAccount, loadScraperTasks]);

  useEffect(() => {
    if (!engagementJobId) return undefined;
    let cancelled = false;
    const poll = async () => {
      try {
        const data = await api.interactionLive(engagementJobId);
        if (cancelled) return;
        setEngagementLiveLogs((data.logs || []).slice(-200));
        if (data.status === "completed" || data.status === "failed" || data.status === "stopped") {
          setEngagementSubmitting(false);
          setEngagementJobId(null);
        }
      } catch {
        if (!cancelled) {
          setEngagementSubmitting(false);
          setEngagementJobId(null);
        }
      }
    };
    poll();
    const iv = setInterval(poll, 550);
    return () => {
      cancelled = true;
      clearInterval(iv);
    };
  }, [engagementJobId]);

  useEffect(() => {
    const el = engagementLogRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [engagementLiveLogs]);

  const runEngagementTask = useCallback(
    async ({ validOnly }) => {
      if (!engagementSelectedGroups.length) {
        setMsg("请至少选择一个目标群组");
        return;
      }
      if (engagementAccountPoolCount < 1) {
        setMsg("当前没有可用或当日受限的账号（长期冷却中账号不参与互动）");
        return;
      }
      setEngagementLiveLogs([]);
      setEngagementJobId(null);
      setEngagementSubmitting(true);
      setEngagementGroupResolution(null);
      setMsg("");
      try {
        const r = await api.startInteractionTask({
          groups: engagementSelectedGroups,
          scan_limit: engagementScanLimit,
          valid_only: validOnly,
        });
        if (r && r.ok === false && r.code === "UNKNOWN_GROUPS") {
          setEngagementGroupResolution({
            valid: r.valid_groups || [],
            invalid: r.invalid_groups || [],
          });
          setEngagementSubmitting(false);
          return;
        }
        if (!r?.job_id) throw new Error("服务端未返回执行会话");
        setEngagementJobId(r.job_id);
      } catch (e) {
        setMsg(e.message);
        setEngagementSubmitting(false);
      }
    },
    [engagementAccountPoolCount, engagementScanLimit, engagementSelectedGroups],
  );

  const onStartEngagement = () => {
    runEngagementTask({ validOnly: false });
  };

  const onStopEngagement = async () => {
    try {
      await api.stopTask();
      setMsg("已发送停止请求");
    } catch (e) {
      setMsg(e.message);
    }
  };

  const onEngagementIgnoreUnknown = () => {
    const res = engagementGroupResolution;
    if (!res?.valid?.length) {
      setMsg("没有已在目标群组库中的项，无法继续");
      setEngagementGroupResolution(null);
      return;
    }
    runEngagementTask({ validOnly: true });
  };

  const onEngagementRegisterUnknown = async () => {
    const res = engagementGroupResolution;
    if (!res?.invalid?.length) return;
    setEngagementRegisterLoading(true);
    setMsg("");
    try {
      await api.registerInteractionTargetGroups(res.invalid);
      await refreshBase();
      setEngagementGroupResolution(null);
      setMsg("已写入目标群组库，正在启动任务…");
      await runEngagementTask({ validOnly: false });
    } catch (e) {
      setMsg(e.message);
      setEngagementSubmitting(false);
    } finally {
      setEngagementRegisterLoading(false);
    }
  };

  const login = async () => {
    if (authLoadingRef.current) return;
    authLoadingRef.current = true;
    setAuthLoading(true);
    try {
      const res = await api.login(auth.username, auth.password);
      localStorage.setItem("token", res.token);
      const u = res.user || { username: res.username, role: res.role };
      setProfile({ username: u.username, role: u.role });
      const { syncOk } = await refreshBase();
      if (u.role === "admin") {
        const list = await api.listUsers();
        setUsers(list.users || []);
      }
      if (syncOk === false) {
        setMsg(`登录成功：${u.username}（数据同步失败，当前为数据库缓存）`);
      } else {
        setMsg(`登录成功：${u.username}`);
      }
    } catch (e) {
      setMsg(e.message);
    } finally {
      authLoadingRef.current = false;
      setAuthLoading(false);
    }
  };

  const logout = async () => {
    try {
      await api.logout();
    } catch {
      /* 令牌已失效时仍执行本地登出 */
    }
    localStorage.removeItem("token");
    setProfile(null);
    setTab("用户增长");
    setCopyBots([]);
    setCopyTasks([]);
    setCopyLogs([]);
    setCopyStartOptimistic({});
    setTasks([]);
    setAccounts({ active: [], limited: [], banned: [], recent_sidebar_echo: [], activity_feed: [] });
    setTaskHighlight({ active: null, previous: null, connecting: null });
    setGroups([]);
    setUsers([]);
      setAccountPaths([]);
      setSelectedGroup("");
    setForcedGroups([]);
    setRemovedGroups([]);
    setEngagementLiveLogs([]);
    setEngagementJobId(null);
    setEngagementSelectedGroups([]);
    setEngagementGroupResolution(null);
    setEngagementRegisterLoading(false);
    setGrowthExecSnapshot(null);
  };

  const onUpload = async () => {
    if (!uploadFile) return setMsg("请选择 zip 文件");
    if (uploadLoadingRef.current) return;
    uploadLoadingRef.current = true;
    setUploadLoading(true);
    try {
      await api.uploadAccount(uploadFile);
      setMsg("上传成功");
      await refreshBase();
    } catch (e) {
      setMsg(e.message);
    } finally {
      uploadLoadingRef.current = false;
      setUploadLoading(false);
    }
  };

  const onStopRunningTask = async () => {
    try {
      await api.stopTask();
      appendLog("stop-task | 已发送停止请求");
    } catch (e) {
      setMsg(e.message);
    }
  };

  const onStartTask = async () => {
    const parsedUsers = form.users.split("\n").map((x) => x.trim()).filter(Boolean);
    if (!selectedGroup) {
      const message = "请先选择目标群组";
      setMsg(message);
      appendLog(`task blocked | ${message}`);
      return;
    }
    if (!parsedUsers.length) {
      const message = "请先填写用户列表（每行一个）";
      setMsg(message);
      appendLog(`task blocked | ${message}`);
      return;
    }
    setTaskRunning(true);
    setTaskPanelPhase("running");
    setGrowthExecSnapshot({
      uiStatus: "WAITING",
      phoneDisplay: "—",
      taskKind: "拉人",
      groupRaw: selectedGroup,
      indeterminate: true,
      progressPct: null,
      success: 0,
      failed: 0,
      errorHint: null,
    });
    setMsg("");
    appendLog(
      `开始执行用户增长 | 群组=${selectedGroup} | 用户数=${parsedUsers.length} | 正在提交后台任务…`,
    );
    try {
      const resp = await api.startTask({
        group: selectedGroup,
        users: parsedUsers,
      });
      const jobId = resp?.job_id;
      if (!jobId) {
        throw new Error("服务端未返回任务编号");
      }
      appendLog(
        `任务已排队 job=${jobId}，后台执行中；约每秒拉取进度，下方将实时显示 Telegram 步骤日志`,
      );
      let data = null;
      let streamed = 0;
      for (;;) {
        const st = await api.taskJobStatus(jobId);
        setTaskHighlight({
          active: st.highlight_active_phone ?? null,
          previous: st.highlight_previous_phone ?? null,
          connecting: st.highlight_connecting_phone ?? null,
        });
        const pl = st.progress_logs || [];
        const parsedProg = parseGrowthTaskProgressLines(pl);
        const conn = st.highlight_connecting_phone ?? null;
        const act = st.highlight_active_phone ?? null;
        const prev = st.highlight_previous_phone ?? null;
        const phoneLine = conn || act || prev || "—";
        const jobLive = st.status === "running" || st.status === "queued";
        let uiStatus = "RUNNING";
        if (st.status === "failed") uiStatus = "ERROR";
        else if (conn && !act) uiStatus = "WAITING";
        else if (st.status === "queued" && !conn && !act) uiStatus = "WAITING";
        const taskKindLine = act ? "拉人" : conn ? "登录" : "拉人";
        const hasQueue = parsedProg.queueTotal > 0;
        setGrowthExecSnapshot({
          uiStatus,
          phoneDisplay: phoneLine,
          taskKind: taskKindLine,
          groupRaw: st.group || selectedGroup,
          indeterminate: jobLive && !hasQueue,
          progressPct: hasQueue ? parsedProg.progressPct : jobLive ? null : parsedProg.progressPct,
          success: parsedProg.success,
          failed: parsedProg.failed,
          errorHint: st.error || null,
        });
        for (let i = streamed; i < pl.length; i++) {
          pushLogLine(pl[i]);
        }
        streamed = pl.length;
        if ((st.status === "completed" || st.status === "stopped") && st.data) {
          data = st.data;
          const dl = st.data.logs || [];
          for (let i = streamed; i < dl.length; i++) {
            pushLogLine(dl[i]);
          }
          break;
        }
        if (st.status === "failed") {
          throw new Error(st.error || "任务失败");
        }
        await new Promise((r) => setTimeout(r, 1000));
      }
      const summary = data.summary || { success: 0, skipped: 0, failed: 0 };
      setGrowthExecSnapshot({
        uiStatus: "WAITING",
        phoneDisplay: "—",
        taskKind: "拉人",
        groupRaw: selectedGroup,
        indeterminate: false,
        progressPct: 100,
        success: summary.success ?? 0,
        failed: summary.failed ?? 0,
        errorHint: null,
      });
      appendLog(`task finished | group=${selectedGroup} accounts_auto=${availableAccounts.length}`);
      appendLog(`result summary | success=${summary.success} skipped=${summary.skipped} failed=${summary.failed}`);
      const h = data.highlight || {};
      setTaskHighlight({
        active: h.active_phone ?? null,
        previous: h.previous_phone ?? null,
        connecting: null,
      });
      if (data.stopped) {
        appendLog("任务已停止（用户中断）");
        setMsg("任务已停止");
      } else if (summary.failed > 0) {
        setMsg(`任务执行完成：成功${summary.success}，跳过${summary.skipped}，失败${summary.failed}`);
      } else {
        setMsg(`任务执行完成：成功${summary.success}，跳过${summary.skipped}，失败0`);
      }
      if (taskPanelPhaseTimerRef.current) clearTimeout(taskPanelPhaseTimerRef.current);
      setTaskPanelPhase(data.stopped ? "ready" : "completed");
      if (!data.stopped) {
        taskPanelPhaseTimerRef.current = window.setTimeout(() => setTaskPanelPhase("ready"), 6000);
      }
      await refreshBase();
    } catch (e) {
      if (taskPanelPhaseTimerRef.current) clearTimeout(taskPanelPhaseTimerRef.current);
      setTaskPanelPhase("ready");
      setGrowthExecSnapshot((prev) => ({
        uiStatus: "ERROR",
        phoneDisplay: prev?.phoneDisplay ?? "—",
        taskKind: "拉人",
        groupRaw: selectedGroup,
        indeterminate: false,
        progressPct: prev?.progressPct ?? null,
        success: prev?.success ?? 0,
        failed: prev?.failed ?? 0,
        errorHint: e.message || "任务失败",
      }));
      appendLog(`任务失败 | ${e.message}`);
      setMsg(e.message);
    } finally {
      setTaskRunning(false);
    }
  };

  const onDeleteAccount = async (phone) => {
    try {
      await api.deleteAccount(phone);
      await refreshBase();
    } catch (e) {
      setMsg(e.message);
    }
  };

  const onForceAddGroup = () => {
    if (!forceCandidate) return;
    setForcedGroups((prev) => Array.from(new Set([...prev, forceCandidate])));
    setSelectedGroup(forceCandidate);
    setForceCandidate("");
  };

  const onRemoveGroup = () => {
    if (!selectedGroup) return;
    setRemovedGroups((prev) => Array.from(new Set([...prev, selectedGroup])));
    setForcedGroups((prev) => prev.filter((x) => x !== selectedGroup));
    setSelectedGroup("");
  };

  const onUpdateDailyLimit = async (groupId, value) => {
    await api.updateGroupLimit(groupId, value);
    await refreshBase();
  };

  const onAddOrUpdatePath = async () => {
    if (!newPath.trim()) return;
    if (pathSubmitRef.current) return;
    pathSubmitRef.current = true;
    setPathSubmitLoading(true);
    try {
      await api.addAccountPath(newPath.trim());
      setNewPath("");
      setEditingPathId(null);
      await refreshBase();
    } finally {
      pathSubmitRef.current = false;
      setPathSubmitLoading(false);
    }
  };

  const onDeletePath = async (id) => {
    await api.deleteAccountPath(id);
    if (editingPathId === id) {
      setEditingPathId(null);
      setNewPath("");
    }
    await refreshBase();
  };

  const onEditPath = (item) => {
    setEditingPathId(item.id);
    setNewPath(item.path);
  };

  const loadUsersData = async () => {
    const list = await api.listUsers();
    setUsers(list.users || []);
  };

  const onLoadUsers = async () => {
    if (loadUsersLoadingRef.current) return;
    loadUsersLoadingRef.current = true;
    setLoadUsersLoading(true);
    try {
      await loadUsersData();
    } finally {
      loadUsersLoadingRef.current = false;
      setLoadUsersLoading(false);
    }
  };

  const onMarkProxyDead = async (proxyId) => {
    await api.markProxyDead(proxyId);
    await refreshBase();
  };

  const onUnbindProxy = async (proxyId) => {
    await api.unbindProxy(proxyId);
    await refreshBase();
  };

  const onChangeRole = async (id, role) => {
    await api.updateUserRole(id, role);
    await loadUsersData();
  };

  const onCreateCopyBot = async () => {
    if (copyBotSubmitRef.current) return;
    const apiId = Number(copyBotForm.api_id);
    if (!apiId || !String(copyBotForm.api_hash || "").trim() || !String(copyBotForm.bot_token || "").trim()) {
      setMsg("请填写 api_id、api_hash、bot_token");
      return;
    }
    copyBotSubmitRef.current = true;
    setCopyBotSaving(true);
    setMsg("");
    try {
      await api.createCopyBot(copyBotForm);
      setCopyBotForm({ api_id: "", api_hash: "", bot_token: "" });
      await loadCopyData();
    } catch (e) {
      setMsg(e?.message || "添加机器人失败");
    } finally {
      copyBotSubmitRef.current = false;
      setCopyBotSaving(false);
    }
  };

  const onDeleteCopyBot = async (id) => {
    if (!window.confirm("删除机器人将同时删除其下所有转发任务，确定？")) return;
    setMsg("");
    try {
      await api.deleteCopyBot(id);
      await loadCopyData();
    } catch (e) {
      setMsg(e?.message || "删除失败");
    }
  };

  const onResetCopyBot = async (id) => {
    setMsg("");
    try {
      await api.resetCopyBot(id);
      await loadCopyData();
    } catch (e) {
      setMsg(e?.message || "重置失败");
    }
  };

  const onCreateCopyTask = async () => {
    if (copyTaskSubmitRef.current) return;
    const botId = Number(copyTaskForm.bot_id);
    if (!copyTaskForm.source_channel.trim() || !copyTaskForm.target_channel.trim() || !botId) {
      setMsg("请从机器人库选择 Bot，并填写来源 / 目标频道");
      return;
    }
    copyTaskSubmitRef.current = true;
    setCopyTaskSaving(true);
    setMsg("");
    try {
      await api.createCopyTask({
        source_channel: copyTaskForm.source_channel,
        target_channel: copyTaskForm.target_channel,
        bot_id: botId,
      });
      setCopyTaskForm((f) => ({ ...f, source_channel: "", target_channel: "" }));
      await loadCopyData();
    } catch (e) {
      setMsg(e?.message || "创建任务失败");
    } finally {
      copyTaskSubmitRef.current = false;
      setCopyTaskSaving(false);
    }
  };

  const onStartCopyTask = async (id) => {
    setCopyStartOptimistic((p) => ({ ...p, [id]: true }));
    setMsg("");
    try {
      const res = await api.startCopyTask(id);
      if (res?.task) {
        setCopyTasks((prev) => mergeCopyTaskIntoList(prev, res.task));
      } else {
        await loadCopyData();
      }
    } catch (e) {
      setCopyStartOptimistic((p) => {
        if (!p[id]) return p;
        const n = { ...p };
        delete n[id];
        return n;
      });
      setMsg(e?.message || "启动失败");
      await loadCopyData();
    }
  };

  const onPauseCopyTask = async (id) => {
    setMsg("");
    try {
      const res = await api.pauseCopyTask(id);
      setCopyStartOptimistic((p) => {
        if (!p[id]) return p;
        const n = { ...p };
        delete n[id];
        return n;
      });
      if (res?.task) {
        setCopyTasks((prev) => mergeCopyTaskIntoList(prev, res.task));
      } else {
        await loadCopyData();
      }
    } catch (e) {
      setMsg(e?.message || "暂停失败");
      await loadCopyData();
    }
  };

  const onDeleteCopyTask = async (id) => {
    if (!window.confirm("确定删除此转发任务？")) return;
    setMsg("");
    try {
      await api.deleteCopyTask(id);
      setCopyStartOptimistic((p) => {
        if (!p[id]) return p;
        const n = { ...p };
        delete n[id];
        return n;
      });
      await loadCopyData();
    } catch (e) {
      setMsg(e?.message || "删除失败");
    }
  };

  const triggerCopySessionImport = (botId) => {
    setCopySessionImportBotId(botId);
    copySessionFileInputRef.current?.click();
  };

  const onCopySessionFileSelected = async (e) => {
    const file = e.target.files?.[0];
    const bid = copySessionImportBotId;
    e.target.value = "";
    setCopySessionImportBotId(null);
    if (!file || bid == null) return;
    setUploadingSessionBotId(bid);
    setMsg("");
    try {
      await api.uploadCopyBotSession(bid, file);
      await loadCopyData();
      setMsg("session 已导入");
    } catch (err) {
      setMsg(err?.message || "导入失败");
    } finally {
      setUploadingSessionBotId(null);
    }
  };

  const onRunScraper = async () => {
    if (!scraperForm.group_id.trim() || scraperLoadingRef.current) return;
    scraperLoadingRef.current = true;
    setScraperLoading(true);
    setMsg("");
    try {
      const data = await api.runScraper(scraperForm);
      setScraperResult(data);
      await loadScraperTasks();
    } catch (e) {
      setScraperResult(null);
      setMsg(e?.message || "采集失败");
    } finally {
      scraperLoadingRef.current = false;
      setScraperLoading(false);
    }
  };

  const onDownloadScrape = async () => {
    if (scraperResultDownloadLoading) return;
    setMsg("");
    setScraperResultDownloadLoading(true);
    try {
      if (scraperResult?.task_id != null) {
        await downloadScraperTaskById(scraperResult.task_id);
        await loadScraperTasks();
        return;
      }
      const url = scraperResult?.file_url;
      if (!url) return;
      const fn = url.split("/").filter(Boolean).pop();
      if (!fn) return;
      await downloadScraperFile(fn);
    } catch (e) {
      setMsg(e?.message || "下载失败");
    } finally {
      setScraperResultDownloadLoading(false);
    }
  };

  const onDownloadScraperHistoryTask = async (taskId) => {
    if (scraperDownloadTaskId != null) return;
    setScraperDownloadTaskId(taskId);
    setMsg("");
    try {
      await downloadScraperTaskById(taskId);
      await loadScraperTasks();
    } catch (e) {
      setMsg(e?.message || "下载失败");
    } finally {
      setScraperDownloadTaskId(null);
    }
  };

  const closeScraperAccountModal = () => {
    setShowScraperAccountModal(false);
    setScraperModalBanner(null);
    setScraperNeedPassword(false);
    setScraperBindPassword("");
  };

  const openScraperAccountModal = () => {
    setScraperBindCode("");
    setScraperPhoneCodeHash("");
    setScraperBindPassword("");
    setScraperModalBanner(null);
    setScraperNeedPassword(false);
    setScraperBindPhone(scraperAccount?.phone || "");
    setShowScraperAccountModal(true);
  };

  const onScraperSendCode = async () => {
    if (!scraperBindPhone.trim() || scraperSendCodeRef.current) return;
    scraperSendCodeRef.current = true;
    setScraperSendCodeLoading(true);
    setScraperModalBanner({ kind: "loading", text: "正在发送验证码…" });
    try {
      const r = await api.sendScraperCode(scraperBindPhone.trim());
      if (r.ok) {
        setScraperPhoneCodeHash(r.phone_code_hash || "");
        if (r.phone) setScraperBindPhone(r.phone);
        setScraperModalBanner({ kind: "success", text: "验证码已发送" });
      } else {
        setScraperModalBanner({ kind: "error", text: r.error || "发送失败" });
      }
    } catch (e) {
      setScraperModalBanner({ kind: "error", text: e?.message || "发送失败" });
    } finally {
      scraperSendCodeRef.current = false;
      setScraperSendCodeLoading(false);
    }
  };

  const onScraperBindLogin = async () => {
    const phone = scraperBindPhone.trim();
    if (!phone || scraperBindLoginRef.current) return;
    if (scraperNeedPassword) {
      if (!scraperBindPassword.trim()) return;
    } else if (!scraperBindCode.trim() || !scraperPhoneCodeHash) return;

    scraperBindLoginRef.current = true;
    setScraperBindLoginLoading(true);
    setScraperModalBanner({ kind: "loading", text: "正在登录…" });
    try {
      const r = scraperNeedPassword
        ? await api.loginScraperAccount({
            phone,
            code: "",
            phone_code_hash: "",
            password: scraperBindPassword.trim(),
          })
        : await api.loginScraperAccount({
            phone,
            code: scraperBindCode.trim(),
            phone_code_hash: scraperPhoneCodeHash,
          });

      if (r.need_password) {
        setScraperNeedPassword(true);
        setScraperModalBanner({
          kind: "info",
          text: "该账号开启了二步验证，请输入密码",
        });
        return;
      }
      if (r.ok) {
        setScraperModalBanner({ kind: "success", text: "登录成功 ✅" });
        setTimeout(() => {
          closeScraperAccountModal();
          setScraperBindCode("");
          setScraperPhoneCodeHash("");
          loadScraperAccount();
        }, 1000);
        return;
      }
      const err = r.error || "登录失败";
      if (err.includes("验证码")) {
        setScraperModalBanner({ kind: "error", text: "验证码错误 ❌" });
      } else if (err.includes("密码")) {
        setScraperModalBanner({ kind: "error", text: "密码错误 ❌" });
      } else {
        setScraperModalBanner({ kind: "error", text: err });
      }
    } catch (e) {
      setScraperModalBanner({ kind: "error", text: e?.message || "登录失败" });
    } finally {
      scraperBindLoginRef.current = false;
      setScraperBindLoginLoading(false);
    }
  };

  const stats = useMemo(() => {
    const today = new Date().toISOString().slice(0, 10);
    const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
    const parseDay = (x) => (x || "").slice(0, 10);
    return {
      todayAdd: tasks.filter((t) => parseDay(t.created_at) === today).reduce((n, t) => n + (t.users?.length || 0), 0),
      yestAdd: tasks.filter((t) => parseDay(t.created_at) === yesterday).reduce((n, t) => n + (t.users?.length || 0), 0),
      total: tasks.reduce((n, t) => n + (t.users?.length || 0), 0),
      accounts: availableAccounts.length,
    };
  }, [tasks, availableAccounts]);

  const TabHeaderIcon = TAB_HEADER_ICONS[tab];

  const scraperTasksVisible = useMemo(
    () => scraperTasks.filter((item) => (Number(item.user_count) || 0) > 0),
    [scraperTasks],
  );

  /** 目标群组 Hero：今日拉人优先，否则按累计拉人、人数 */
  const featuredTargetGroup = useMemo(() => {
    const list = groups || [];
    if (!list.length) return null;
    return [...list].sort((a, b) => {
      const t = (Number(b.today_added) || 0) - (Number(a.today_added) || 0);
      if (t !== 0) return t;
      const u = (Number(b.total_added) || 0) - (Number(a.total_added) || 0);
      if (u !== 0) return u;
      return (Number(b.members_count) || 0) - (Number(a.members_count) || 0);
    })[0];
  }, [groups]);

  if (!profile) {
    return (
      <div className="relative flex min-h-[100dvh] flex-col items-center justify-center overflow-hidden bg-[#070b12] px-4 py-10 text-slate-200">
        <div
          className="pointer-events-none absolute left-[15%] top-0 h-64 w-64 -translate-y-1/2 rounded-full bg-cyan-500/12 blur-[100px]"
          aria-hidden
        />
        <div
          className="pointer-events-none absolute bottom-0 right-[10%] h-72 w-72 translate-y-1/3 rounded-full bg-emerald-500/10 blur-[110px]"
          aria-hidden
        />
        <div className="relative z-[1] w-full max-w-[400px]">
          <div className="mb-8 text-center">
            <div className="mx-auto mb-4 grid h-14 w-14 place-items-center rounded-2xl bg-gradient-to-br from-[#00ff87] to-[#60efff] text-lg font-bold text-slate-900 shadow-[0_0_32px_rgba(0,255,150,0.4)]">
              TG
            </div>
            <h1 className="text-xl font-semibold tracking-tight text-white">TG Pro</h1>
            <p className="mt-1 text-xs font-medium uppercase tracking-wider text-slate-500">请登录以继续</p>
          </div>
          <div className="rounded-2xl border border-white/[0.08] bg-[rgba(255,255,255,0.04)] p-6 shadow-[0_24px_80px_rgba(0,0,0,0.5)] backdrop-blur-[22px]">
            <div className="space-y-3">
              <label className="block text-[11px] font-semibold uppercase tracking-wider text-slate-500">用户名</label>
              <input
                className={INPUT_FIELD}
                autoComplete="username"
                value={auth.username}
                onChange={(e) => setAuth((v) => ({ ...v, username: e.target.value }))}
              />
              <label className="mt-4 block text-[11px] font-semibold uppercase tracking-wider text-slate-500">密码</label>
              <input
                type="password"
                className={INPUT_FIELD}
                autoComplete="current-password"
                value={auth.password}
                onChange={(e) => setAuth((v) => ({ ...v, password: e.target.value }))}
              />
              <button
                type="button"
                disabled={authLoading}
                className={`${BTN_PRIMARY} mt-5 w-full justify-center py-2.5 disabled:cursor-not-allowed disabled:opacity-60`}
                onClick={login}
              >
                {authLoading ? (
                  <>
                    <UiSpinner tone="primary" />
                    登录中…
                  </>
                ) : (
                  "登录"
                )}
              </button>
            </div>
            {msg ? (
              <p className="mt-4 text-center text-sm font-medium text-rose-400 drop-shadow-[0_0_8px_rgba(251,113,133,0.35)]">
                {msg}
              </p>
            ) : null}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex min-h-0 min-h-[100dvh] bg-transparent text-slate-200">
      <aside className="sticky top-0 flex h-screen w-[220px] shrink-0 flex-col border-r border-white/[0.06] bg-[rgba(10,15,20,0.8)] shadow-[4px_0_48px_rgba(0,0,0,0.5)] backdrop-blur-[20px]">
        <div className="border-b border-white/[0.06] px-4 py-5">
          <div className="flex items-center gap-3">
            <div className="grid h-10 w-10 place-items-center rounded-xl bg-gradient-to-br from-[#00ff87] to-[#60efff] text-sm font-bold text-slate-900 shadow-[0_0_24px_rgba(0,255,150,0.35)] transition duration-200 hover:scale-105 hover:shadow-[0_0_32px_rgba(96,239,255,0.45)]">
              TG
            </div>
            <div>
              <div className="text-sm font-semibold tracking-tight text-slate-100">TG Pro</div>
              <div className="text-[10px] font-medium uppercase tracking-wider text-slate-500">Growth Console</div>
            </div>
          </div>
        </div>
        <nav className="growth-scroll flex flex-1 flex-col gap-1 overflow-y-auto p-3">
          {menus
            .filter((m) => {
              if (m === "用户管理" || m === "代理监控") return isAdmin;
              return true;
            })
            .map((m) => (
              <button
                key={m}
                type="button"
                onClick={() => setTab(m)}
                className={`group flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-left text-sm font-medium transition-all duration-[250ms] ease-out ${
                  tab === m
                    ? "border border-white/[0.1] bg-gradient-to-r from-emerald-500/20 via-cyan-500/15 to-transparent text-white shadow-[0_0_28px_rgba(0,255,180,0.12)]"
                    : "border border-transparent text-slate-400 hover:border-white/[0.06] hover:bg-white/[0.04] hover:text-slate-200"
                }`}
              >
                <span
                  className={
                    tab === m
                      ? "text-[#5eead4]"
                      : "text-slate-500 transition group-hover:text-[#5eead4]"
                  }
                >
                  <SidebarMenuIcon name={m} className="h-5 w-5 shrink-0" />
                </span>
                {m}
              </button>
            ))}
        </nav>
        <div className="border-t border-white/[0.06] p-3">
          <div className="rounded-xl border border-white/[0.08] bg-[rgba(255,255,255,0.03)] p-3 shadow-[0_8px_32px_rgba(0,0,0,0.35)] backdrop-blur-[20px]">
            <div className="truncate text-xs font-semibold text-slate-100">{profile.username}</div>
            <div className="mt-0.5 text-[10px] font-medium uppercase tracking-wider text-slate-500">{profile.role}</div>
            <button
              type="button"
              className="mt-3 w-full rounded-xl border border-white/[0.1] bg-[rgba(255,255,255,0.05)] py-2 text-xs font-medium text-slate-300 shadow-[0_4px_20px_rgba(0,0,0,0.25)] transition hover:border-cyan-400/25 hover:bg-white/[0.08] hover:text-white"
              onClick={logout}
            >
              退出登录
            </button>
          </div>
        </div>
      </aside>

      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <header className="shrink-0 border-b border-white/[0.06] bg-[rgba(10,15,20,0.55)] px-6 py-4 shadow-[0_8px_40px_rgba(0,0,0,0.35)] backdrop-blur-[20px]">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div className="min-w-0">
              <div className="flex items-center gap-3">
                {TabHeaderIcon ? (
                  <span className="grid h-10 w-10 shrink-0 place-items-center rounded-xl border border-white/[0.1] bg-gradient-to-br from-emerald-500/20 to-cyan-500/15 text-cyan-300 shadow-[0_0_20px_rgba(0,255,180,0.15)] ring-1 ring-white/[0.06]">
                    <TabHeaderIcon size={22} strokeWidth={2} aria-hidden />
                  </span>
                ) : null}
                <h1 className="text-lg font-semibold tracking-tight text-slate-100">{tab}</h1>
              </div>
              {tab === "目标群组" && lastGroupMetadataSync ? (
                <p className="mt-1 text-xs text-slate-500">上次 Telegram 同步：{lastGroupMetadataSync}</p>
              ) : null}
            </div>
            <div className="flex flex-wrap items-center gap-2">
              {tab === "目标群组" ? (
                <button
                  type="button"
                  disabled={refreshLoading}
                  className={`${BTN_PRIMARY} inline-flex items-center justify-center gap-2 px-4 py-2 disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:translate-y-0`}
                  onClick={onForceSyncGroups}
                >
                  {refreshLoading && refreshPhase === "force" ? (
                    <>
                      <UiSpinner tone="primary" />
                      同步中...
                    </>
                  ) : (
                    "强制同步群组信息"
                  )}
                </button>
              ) : null}
              <button
                type="button"
                disabled={refreshLoading}
                className={`${BTN_SECONDARY} inline-flex items-center justify-center gap-2 disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:translate-y-0`}
                onClick={() => triggerRefresh({}, "header")}
              >
                {refreshLoading && refreshPhase === "header" ? (
                  <>
                    <UiSpinner tone="muted" />
                    刷新中...
                  </>
                ) : (
                  "刷新数据"
                )}
              </button>
            </div>
          </div>
        </header>

        <main
          className={`flex-1 px-6 pb-10 pt-6 lg:px-8 ${
            tab === "用户增长" ? "flex min-h-0 flex-col overflow-hidden" : "growth-scroll overflow-y-auto"
          }`}
        >
        {msg ? (
          <p className="mb-4 shrink-0 text-sm font-medium text-rose-400 drop-shadow-[0_0_8px_rgba(251,113,133,0.35)]">
            {msg}
          </p>
        ) : null}

        {tab === "用户增长" && (
          <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden rounded-2xl border border-white/[0.07] bg-gradient-to-b from-[#0b0f1a]/95 via-[#0c1220]/92 to-[#0f172a]/90 shadow-[inset_0_1px_0_rgba(255,255,255,0.05),0_12px_48px_rgba(0,0,0,0.35)] backdrop-blur-[12px]">
            <div
              className="pointer-events-none absolute left-[12%] top-0 h-48 w-48 -translate-y-1/2 rounded-full bg-cyan-500/10 blur-[80px]"
              aria-hidden
            />
            <div
              className="pointer-events-none absolute bottom-0 right-[8%] h-56 w-56 translate-y-1/3 rounded-full bg-emerald-500/8 blur-[90px]"
              aria-hidden
            />
            <div
              className="pointer-events-none absolute right-1/4 top-1/3 h-40 w-40 rounded-full bg-violet-500/10 blur-[70px]"
              aria-hidden
            />

            <div className="relative z-[1] flex min-h-0 flex-1 flex-col gap-4 p-4 sm:p-5">
              <div className="grid min-h-0 flex-1 grid-cols-1 gap-4 lg:grid-cols-[minmax(300px,340px)_minmax(0,1fr)] lg:items-stretch lg:gap-5">
                {/* 左侧：统计（shrink-0）→ 执行状态（固定高 ~150px，grow-0）→ 账号队列（flex-1 占余量，列表区仍可滚动） */}
                <div className="order-1 flex h-full min-h-0 flex-col gap-3 lg:order-none">
                  <div className="shrink-0">
                    <AccountPoolNeonDistributionPanel
                      total_accounts={growthAccountPoolStats.total_accounts}
                      active_accounts={growthAccountPoolStats.active_accounts}
                      limited_accounts={growthAccountPoolStats.limited_accounts}
                      risk_accounts={growthAccountPoolStats.risk_accounts}
                    />
                  </div>
                  <GrowthExecutionStatusModule
                    selectedGroup={selectedGroup}
                    snapshot={growthExecSnapshot}
                    taskRunning={taskRunning}
                    taskHighlight={taskHighlight}
                  />
                  <div
                    className={`${GLASS_PANEL_GROWTH} flex min-h-[12.5rem] flex-1 flex-col overflow-hidden`}
                  >
                    <div className={GLASS_PANEL_CHROME_GROWTH}>
                      <span className="h-2 w-2 rounded-full bg-rose-400 shadow-sm" />
                      <span className="h-2 w-2 rounded-full bg-amber-400 shadow-sm" />
                      <span className="h-2 w-2 rounded-full bg-[#22c55e] shadow-sm shadow-emerald-400/50" />
                      <span className="ml-1 font-log text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        accounts.queue
                      </span>
                      <span className="ml-auto font-log text-[10px] text-slate-400">{sidebarQueueAccounts.length}</span>
                    </div>
                    <p className="shrink-0 border-b border-emerald-400/10 px-3 py-1.5 text-[10px] leading-snug text-slate-500">
                      ACTIVE / LIMITED / RISK 标签 · 执行中账号高亮 · 非 ACTIVE 提示最多 60s
                    </p>
                    <div className="growth-queue-scroll min-h-0 flex-1 overflow-y-auto overflow-x-hidden px-3 py-2.5">
                      <div className="flex flex-col gap-2.5 pr-0.5">
                        {sidebarQueueAccounts.map((a) => {
                          const pk = normalizePhoneKey(a.phone);
                          const isActiveHighlight = taskHighlight.active && pk === normalizePhoneKey(taskHighlight.active);
                          const isConnectingHighlight =
                            taskHighlight.connecting && pk === normalizePhoneKey(taskHighlight.connecting);
                          const isPrevHighlight = taskHighlight.previous && pk === normalizePhoneKey(taskHighlight.previous);
                          const isEcho = a._queueKind === "echo";
                          const rowKind = growthQueueRowKind(a, taskHighlight);
                          const isExecuting = rowKind === "executing";

                          let phoneCls = "font-medium tracking-tight";
                          if (isActiveHighlight) phoneCls += " text-emerald-400";
                          else if (isConnectingHighlight) phoneCls += " text-amber-400";
                          else if (isPrevHighlight) phoneCls += " text-cyan-400";
                          else if (isEcho) phoneCls += " text-slate-500";
                          else phoneCls += " text-slate-100";

                          let avatarShell =
                            "grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-emerald-500/10 text-emerald-300 ring-1 ring-emerald-400/25";
                          if (isActiveHighlight)
                            avatarShell =
                              "grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-emerald-500/25 text-emerald-200 ring-1 ring-emerald-400/45";
                          else if (isConnectingHighlight)
                            avatarShell =
                              "grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-amber-500/20 text-amber-300 ring-1 ring-amber-400/35";
                          else if (isPrevHighlight)
                            avatarShell =
                              "grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-cyan-500/15 text-cyan-300 ring-1 ring-cyan-400/30";
                          else if (isEcho)
                            avatarShell =
                              "grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-white/[0.04] text-slate-500 ring-1 ring-white/[0.08]";

                          const pill =
                            rowKind === "risk" ? (
                              <span className="badge-glow-risk inline-flex rounded-full border border-rose-400/35 bg-rose-500/15 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wide text-rose-200">
                                RISK
                              </span>
                            ) : rowKind === "limited" ? (
                              <span className="badge-glow-warn inline-flex rounded-full border border-amber-400/35 bg-amber-500/12 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wide text-amber-200">
                                LIMITED
                              </span>
                            ) : (
                              <span className="badge-glow-available inline-flex rounded-full border border-emerald-400/35 bg-emerald-500/12 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wide text-emerald-200">
                                ACTIVE
                              </span>
                            );

                          const cardRing = isExecuting
                            ? "ring-2 ring-cyan-400/75 shadow-[0_0_32px_rgba(34,211,238,0.45),0_0_48px_rgba(52,211,153,0.2)]"
                            : "ring-1 ring-transparent";

                          return (
                            <div
                              key={`${a.id}-${a._queueKind || "x"}`}
                              className={`group flex gap-2.5 rounded-xl border px-3 py-2.5 text-sm shadow-[0_4px_24px_rgba(0,0,0,0.35)] backdrop-blur-[16px] transition-all duration-300 ease-out will-change-transform hover:-translate-y-0.5 hover:border-emerald-400/25 hover:shadow-[0_12px_40px_rgba(0,255,180,0.12),0_0_28px_rgba(96,239,255,0.1)] ${cardRing} ${
                                isEcho
                                  ? "border-white/[0.06] bg-[rgba(255,255,255,0.03)]"
                                  : "border-white/[0.08] bg-[rgba(255,255,255,0.04)]"
                              }`}
                            >
                              <div className={avatarShell}>
                                <UserCircle size={18} strokeWidth={1.75} aria-hidden />
                              </div>
                              <div className="min-w-0 flex-1">
                                <div className="flex flex-wrap items-center gap-2">
                                  <div className={phoneCls}>{displayPhone(a)}</div>
                                  {pill}
                                </div>
                                <div className="mt-0.5 text-[10px] font-medium uppercase tracking-wide text-slate-400">
                                  {isEcho ? "侧栏提示 · 非拉人队列" : "拉人队列"}
                                </div>
                                {isEcho ? (
                                  <p className="mt-1 text-[10px] leading-snug text-amber-400/90">
                                    {(a.echo_label || a.lifecycle_sub || "—") + " · 不参与拉人"}
                                  </p>
                                ) : null}
                                <div className="mt-2 flex items-center gap-1.5 text-[11px] text-slate-500">
                                  <Globe className="h-3.5 w-3.5 shrink-0 text-slate-400" aria-hidden />
                                  <span
                                    className={
                                      a.proxy_type === "direct"
                                        ? "font-medium text-amber-400"
                                        : "font-medium text-emerald-400"
                                    }
                                  >
                                    {a.proxy_type || "direct"}
                                  </span>
                                </div>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                </div>

                {/* 右侧：任务控制 + 实时日志（日志紧贴任务下沿，flex 铺满剩余高度） */}
                <div className="order-2 flex min-h-0 min-w-0 flex-col gap-3 lg:order-none lg:min-h-0">
                  <div className="flex shrink-0 flex-col gap-4">
                    <div className="grid shrink-0 grid-cols-3 gap-2 sm:gap-3">
                      <StatTile title="今日新增" value={stats.todayAdd} icon={TrendingUp} tone="growth" />
                      <StatTile title="昨日新增" value={stats.yestAdd} icon={CalendarClock} tone="info" />
                      <StatTile title="累计新增" value={stats.total} icon={Layers} tone="info" />
                    </div>
                    <section className="task-control-panel shrink-0" aria-label="任务控制面板">
                    <div className="task-control-panel-inner">
                      <div className="mb-5 flex flex-col gap-4 border-b border-white/[0.08] pb-5 lg:flex-row lg:items-end lg:justify-between lg:gap-6">
                        <div className="min-w-0">
                          <h3 className="task-control-panel-title">TASK CONTROL PANEL</h3>
                          <p className="mt-2 text-xs leading-relaxed text-slate-400">
                            实时任务配置 <span className="text-[#00AFFF]/80">/</span> 自动执行系统
                          </p>
                        </div>
                        <TaskControlStatusBar phase={taskPanelPhase} />
                      </div>
                      <div className="flex flex-col gap-5">
                        <div className="flex flex-wrap items-end gap-2 gap-y-3">
                          <div className="flex min-w-[200px] flex-1 flex-col gap-1.5">
                            <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
                              目标群组
                            </span>
                            <GlassDropdown
                              variant="task"
                              value={selectedGroup}
                              onChange={setSelectedGroup}
                              options={selectedGroupDropdownOptions}
                              placeholder="选择群组…"
                              searchable
                              className="w-full"
                            />
                          </div>
                          <div className="flex min-w-[160px] flex-1 flex-col gap-1.5">
                            <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
                              强制加入
                            </span>
                            <GlassDropdown
                              variant="task"
                              value={forceCandidate}
                              onChange={setForceCandidate}
                              options={forceCandidateDropdownOptions}
                              placeholder="隐藏群组…"
                              searchable
                              className="w-full"
                            />
                          </div>
                          <div className="flex gap-1.5">
                            <button
                              type="button"
                              className="rounded-xl border border-[#00AFFF]/35 bg-[rgba(0,175,255,0.1)] px-3 py-2 text-sm font-bold text-sky-200 shadow-[0_0_16px_rgba(0,175,255,0.2)] transition hover:scale-105 hover:border-[#7A5CFF]/40 hover:shadow-[0_0_28px_rgba(122,92,255,0.3)] active:scale-95"
                              onClick={onForceAddGroup}
                            >
                              +
                            </button>
                            <button
                              type="button"
                              className="rounded-xl border border-rose-400/35 bg-rose-500/10 px-3 py-2 text-sm font-bold text-rose-300 shadow-[0_0_12px_rgba(251,113,133,0.15)] transition hover:scale-105 hover:shadow-[0_0_24px_rgba(251,113,133,0.28)] active:scale-95"
                              onClick={onRemoveGroup}
                            >
                              −
                            </button>
                          </div>
                        </div>
                        <label className="flex flex-col gap-1.5">
                          <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">
                            用户列表
                          </span>
                          <textarea
                            className="growth-scroll task-control-field max-h-[200px] min-h-[80px] resize-y"
                            rows={4}
                            placeholder="每行一个 @username 或用户标识…"
                            value={form.users}
                            onChange={(e) => setForm((v) => ({ ...v, users: e.target.value }))}
                          />
                        </label>
                        <div className="flex flex-wrap items-center gap-3">
                          <button
                            type="button"
                            disabled={false}
                            onClick={taskRunning ? onStopRunningTask : onStartTask}
                            className={
                              taskRunning
                                ? "inline-flex items-center justify-center gap-2 rounded-xl border border-rose-400/40 bg-rose-500/20 px-6 py-2.5 text-sm font-bold text-rose-100 shadow-[0_0_20px_rgba(251,113,133,0.25)] transition-all duration-200 hover:bg-rose-500/30"
                                : "task-control-start-btn inline-flex items-center justify-center gap-2"
                            }
                          >
                            {taskRunning ? "停止" : "开始增长"}
                          </button>
                          {taskRunning ? (
                            <span className="inline-flex items-center gap-2 text-xs font-medium text-cyan-300/90">
                              <UiSpinner tone="primary" />
                              执行中…
                            </span>
                          ) : null}
                        </div>
                      </div>
                    </div>
                  </section>
                  </div>
                  <div className="flex min-h-[200px] min-w-0 flex-1 flex-col lg:min-h-0">
                  <div className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-2xl border border-blue-400/18 bg-[rgba(6,10,18,0.65)] shadow-[0_8px_40px_rgba(0,0,0,0.42),0_0_42px_rgba(59,130,246,0.12)] backdrop-blur-[18px]">
                    <div className={GLASS_PANEL_CHROME_LOG}>
                      <span className="h-2 w-2 rounded-full bg-rose-400 shadow-sm" />
                      <span className="h-2 w-2 rounded-full bg-amber-400 shadow-sm" />
                      <span className="h-2 w-2 rounded-full bg-sky-400 shadow-[0_0_8px_rgba(56,189,248,0.6)]" />
                      <span className="ml-1 font-log text-[10px] uppercase tracking-[0.2em] text-slate-500">
                        session.log
                      </span>
                      <span className="ml-auto font-log text-[10px] text-slate-400">live</span>
                    </div>
                    <p className="shrink-0 border-b border-blue-400/10 bg-blue-500/[0.04] px-3 py-1.5 text-[10px] text-slate-500">
                      终端视图 · 最多 {MAX_LOG_ENTRIES} 条 · 上滑暂停跟随，回到底部恢复 · INFO / SUCCESS / WARN / ERROR 分色
                    </p>
                    <div
                      ref={logRef}
                      role="log"
                      aria-live="polite"
                      aria-relevant="additions"
                      onScroll={handleLogScroll}
                      className="growth-terminal-scroll terminal-log-body log-container min-h-0 flex-1 overflow-y-auto overflow-x-hidden px-3 py-2 font-log"
                    >
                      {logs.map(({ id, time, message, type }) => (
                        <LogLineRow key={id} time={time} message={message} type={type} />
                      ))}
                    </div>
                  </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {tab === "目标群组" && (
          <div className="space-y-5">
            {featuredTargetGroup ? <GroupsHeroCard group={featuredTargetGroup} /> : null}
            <div className="grid gap-5 md:grid-cols-2 xl:grid-cols-3">
              {groups.map((g) => (
                <TargetGroupDashboardCard key={g.id} group={g} onUpdateDailyLimit={onUpdateDailyLimit} />
              ))}
            </div>
          </div>
        )}

        {tab === "群组互动" && (
          <div className="grid min-h-0 gap-5 lg:grid-cols-[minmax(0,1fr)_min(420px,42vw)] lg:items-stretch">
            <div
              className={`${cardShellClass("risk")} relative flex min-h-0 flex-col overflow-hidden !p-0`}
              style={{
                boxShadow:
                  "0 8px 40px rgba(0,0,0,0.42), 0 0 48px rgba(192,38,211,0.1), 0 0 72px rgba(59,130,246,0.06)",
              }}
            >
              <div
                className="pointer-events-none absolute inset-0 opacity-[0.35]"
                style={{
                  background:
                    "radial-gradient(900px 420px at 10% 0%, rgba(192,38,211,0.18), transparent 55%), radial-gradient(700px 380px at 90% 20%, rgba(59,130,246,0.14), transparent 50%)",
                }}
              />
              <div className="relative flex min-h-0 flex-1 flex-col space-y-4 p-5 sm:p-7">
                <div className="flex flex-col gap-2 border-b border-white/[0.06] pb-4 sm:flex-row sm:items-end sm:justify-between">
                  <div>
                    <p className="font-log text-[10px] font-bold uppercase tracking-[0.2em] text-cyan-400/70">exec.terminal</p>
                    <h2 className="mt-1 text-lg font-bold tracking-tight text-white">群组互动</h2>
                    <p className="mt-1 text-xs leading-relaxed text-slate-500">
                      <span className="text-fuchsia-200/80">可用 + 当日受限</span> 账号（不含冷却中）· 今日消息随机表情 · 群间隔 5–15s
                    </p>
                  </div>
                  <div className="rounded-xl border border-fuchsia-400/20 bg-fuchsia-500/[0.06] px-3 py-2 text-center backdrop-blur-md sm:text-right">
                    <p className="text-[10px] font-semibold uppercase tracking-wider text-fuchsia-300/80">账号池</p>
                    <p className="stat-num-risk text-2xl tabular-nums">{engagementAccountPoolCount}</p>
                  </div>
                </div>

                <div className="min-h-0 flex-1 space-y-2">
                  <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-400">目标群组</span>
                  <EngagementGroupPanel
                    values={engagementSelectedGroups}
                    onChange={setEngagementSelectedGroups}
                    options={engagementGroupOptions}
                    disabled={!profile}
                  />
                  <p className="text-[10px] leading-relaxed text-slate-600">
                    选项仅来自「目标群组」库（groups 表同步数据），不支持自由输入。
                  </p>
                </div>

                {engagementGroupResolution ? (
                  <div
                    role="alert"
                    className="shrink-0 rounded-xl border border-amber-400/28 bg-[rgba(20,16,8,0.55)] p-4 shadow-[0_0_32px_rgba(251,191,36,0.12)] backdrop-blur-[16px]"
                  >
                    <h4 className="text-sm font-bold text-amber-200/95">部分群组未识别</h4>
                    <p className="mt-1 text-[11px] leading-relaxed text-slate-400">
                      下列标识在目标群组库中不存在。可登记入库后整单执行，或仅对已登记群继续。
                    </p>
                    <ul className="mt-2 max-h-28 overflow-y-auto rounded-lg border border-white/[0.06] bg-black/25 px-2 py-1.5 font-mono text-[11px] text-amber-100/90">
                      {(engagementGroupResolution.invalid || []).map((u) => (
                        <li key={u} className="py-0.5">
                          {u}
                        </li>
                      ))}
                    </ul>
                    {(engagementGroupResolution.valid || []).length > 0 ? (
                      <p className="mt-2 text-[10px] text-slate-500">
                        已登记可执行：
                        <span className="text-slate-400"> {(engagementGroupResolution.valid || []).join(" · ")}</span>
                      </p>
                    ) : (
                      <p className="mt-2 text-[10px] text-rose-300/90">当前勾选项均不在库中，请先「加入目标群组」。</p>
                    )}
                    <div className="mt-3 flex flex-wrap gap-2">
                      <button
                        type="button"
                        disabled={engagementRegisterLoading || !(engagementGroupResolution.invalid || []).length}
                        onClick={onEngagementRegisterUnknown}
                        className="rounded-xl border border-emerald-400/35 bg-emerald-500/15 px-3 py-2 text-xs font-semibold text-emerald-200 shadow-[0_0_16px_rgba(52,211,153,0.15)] transition hover:border-emerald-400/50 hover:bg-emerald-500/25 disabled:cursor-not-allowed disabled:opacity-45"
                      >
                        {engagementRegisterLoading ? (
                          <span className="inline-flex items-center gap-2">
                            <UiSpinner tone="primary" />
                            写入中…
                          </span>
                        ) : (
                          "加入目标群组"
                        )}
                      </button>
                      <button
                        type="button"
                        disabled={!(engagementGroupResolution.valid || []).length || engagementRegisterLoading}
                        onClick={onEngagementIgnoreUnknown}
                        className="rounded-xl border border-cyan-400/30 bg-cyan-500/10 px-3 py-2 text-xs font-semibold text-cyan-200 transition hover:border-cyan-400/45 hover:bg-cyan-500/18 disabled:cursor-not-allowed disabled:opacity-45"
                      >
                        忽略并继续
                      </button>
                      <button
                        type="button"
                        disabled={engagementRegisterLoading}
                        onClick={() => setEngagementGroupResolution(null)}
                        className="rounded-xl border border-white/10 bg-white/[0.05] px-3 py-2 text-xs font-medium text-slate-400 transition hover:border-white/15 hover:text-slate-200 disabled:opacity-45"
                      >
                        关闭
                      </button>
                    </div>
                  </div>
                ) : null}

                <div className="grid shrink-0 gap-4 border-t border-white/[0.06] pt-4 sm:grid-cols-2">
                  <label className="flex flex-col gap-1.5">
                    <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">扫描条数</span>
                    <input
                      type="number"
                      min={10}
                      max={5000}
                      className={INPUT_FIELD}
                      value={engagementScanLimit}
                      onChange={(e) => setEngagementScanLimit(Number(e.target.value) || 300)}
                    />
                  </label>
                  <div className="flex flex-col justify-end rounded-xl border border-white/[0.08] bg-[rgba(0,0,0,0.2)] px-3 py-2 text-xs text-slate-500 backdrop-blur-md">
                    <span className="font-medium text-slate-400">群组间隔</span>
                    <span className="mt-1 tabular-nums text-cyan-200/80">5–15 秒随机</span>
                  </div>
                </div>

                <button
                  type="button"
                  disabled={!profile}
                  onClick={engagementSubmitting ? onStopEngagement : onStartEngagement}
                  className={
                    engagementSubmitting
                      ? "shrink-0 w-full rounded-2xl border border-rose-400/40 bg-rose-500/25 px-5 py-3 text-sm font-bold text-rose-100 shadow-[0_0_24px_rgba(251,113,133,0.3)] transition-all duration-200 hover:bg-rose-500/35 sm:w-auto sm:min-w-[220px]"
                      : "shrink-0 w-full rounded-2xl bg-[linear-gradient(135deg,#e879f9,#60efff)] px-5 py-3 text-sm font-bold text-slate-900 shadow-[0_0_28px_rgba(217,70,239,0.45),0_8px_28px_rgba(0,0,0,0.35)] transition-all duration-200 hover:-translate-y-0.5 hover:shadow-[0_0_42px_rgba(96,239,255,0.35),0_12px_36px_rgba(217,70,239,0.25)] active:translate-y-0 disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0 sm:w-auto sm:min-w-[220px]"
                  }
                >
                  {engagementSubmitting ? "停止" : "开始互动"}
                </button>
                {engagementSubmitting ? (
                  <div className="flex items-center justify-center gap-2 text-xs font-medium text-cyan-200/90 sm:justify-start">
                    <UiSpinner tone="primary" />
                    执行中…
                  </div>
                ) : null}
              </div>
            </div>

            <aside className="engagement-live-terminal flex min-h-[min(520px,72vh)] flex-col overflow-hidden rounded-2xl border border-cyan-400/15 bg-[rgba(6,10,18,0.72)] shadow-[0_8px_40px_rgba(0,0,0,0.5),0_0_48px_rgba(34,211,238,0.08)] backdrop-blur-[20px]">
              <div className="flex shrink-0 items-center justify-between gap-2 border-b border-cyan-400/12 bg-[rgba(0,255,200,0.04)] px-4 py-3 backdrop-blur-md">
                <div className="flex items-center gap-2">
                  <span
                    className={`h-2 w-2 rounded-full ${engagementSubmitting ? "animate-pulse bg-emerald-400 shadow-[0_0_10px_rgba(52,211,153,0.8)]" : "bg-slate-600"}`}
                    aria-hidden
                  />
                  <h3 className="text-sm font-bold tracking-tight text-slate-100">Live Log</h3>
                  <span className="font-log text-[9px] font-medium uppercase tracking-widest text-slate-500">stream</span>
                </div>
                <span className="rounded border border-white/10 bg-black/30 px-2 py-0.5 font-mono text-[10px] tabular-nums text-slate-400">
                  {Math.min(engagementLiveLogs.length, 200)} / 200
                </span>
              </div>
              <div
                ref={engagementLogRef}
                role="log"
                aria-live="polite"
                className="engagement-live-terminal-body growth-scroll min-h-0 flex-1 overflow-y-auto px-3 py-2 font-mono"
              >
                {engagementLiveLogs.length === 0 ? (
                  <p className="py-12 text-center text-[11px] leading-relaxed text-slate-600">
                    等待执行…
                    <br />
                    <span className="text-slate-500">点击「开始互动」后实时输出</span>
                  </p>
                ) : (
                  <ul className="space-y-1">
                    {engagementLiveLogs.map((line, idx) => (
                      <li
                        key={`${idx}-${line.t}-${line.layer}-${line.progress}-${line.account}-${line.group}-${line.message}`}
                        className={`break-words text-[11px] leading-snug ${engagementLiveLogTone(line.level)}`}
                      >
                        {formatEngagementLogLine(line)}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </aside>
          </div>
        )}

        {tab === "账号检测" && (
          <div className="space-y-6">
            <header className="account-console-hero flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between lg:gap-6">
              <div className="flex min-w-0 items-start gap-4">
                <div className="hidden h-14 w-14 shrink-0 place-items-center rounded-2xl border border-cyan-400/20 bg-gradient-to-br from-cyan-500/15 to-emerald-500/10 shadow-[0_0_28px_rgba(56,189,248,0.2)] sm:grid">
                  <Shield className="h-7 w-7 text-cyan-300" strokeWidth={1.75} aria-hidden />
                </div>
                <div className="min-w-0">
                  <h2 className="text-xl font-bold tracking-tight text-white md:text-2xl">账号检测</h2>
                  <p className="account-console-title-en mt-1.5">Account Monitoring Center</p>
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-2 lg:justify-end">
                <button
                  type="button"
                  className={`${BTN_SECONDARY} hover:scale-[1.02] active:scale-[0.98]`}
                  onClick={() => setShowPathModal(true)}
                >
                  账号路径
                </button>
                <label className="inline-flex cursor-pointer items-center rounded-xl border border-dashed border-cyan-400/25 bg-[rgba(255,255,255,0.04)] px-3 py-2 text-xs text-slate-400 shadow-[0_4px_20px_rgba(0,0,0,0.28)] backdrop-blur-[14px] transition hover:scale-[1.02] hover:border-cyan-400/40 hover:bg-white/[0.06] hover:text-slate-200 active:scale-[0.98]">
                  <input className="sr-only" type="file" accept=".zip" onChange={(e) => setUploadFile(e.target.files?.[0] || null)} />
                  {uploadFile ? uploadFile.name : "选择 .zip"}
                </label>
                <button
                  type="button"
                  disabled={uploadLoading}
                  className="account-console-btn-primary inline-flex items-center justify-center gap-2 disabled:hover:scale-100"
                  onClick={onUpload}
                >
                  {uploadLoading ? (
                    <>
                      <UiSpinner tone="primary" />
                      上传中...
                    </>
                  ) : (
                    <>
                      <Upload className="h-4 w-4 shrink-0" aria-hidden />
                      上传
                    </>
                  )}
                </button>
                <button
                  type="button"
                  disabled={refreshLoading}
                  className={`${BTN_SECONDARY} inline-flex items-center justify-center gap-2 hover:scale-[1.02] active:scale-[0.98] disabled:hover:scale-100`}
                  onClick={() => triggerRefresh({}, "header")}
                >
                  {refreshLoading && refreshPhase === "header" ? (
                    <>
                      <UiSpinner tone="muted" />
                      刷新中...
                    </>
                  ) : (
                    "刷新"
                  )}
                </button>
              </div>
            </header>
            <div className="grid gap-5 xl:grid-cols-3">
              <AccountMonitorColumn
                variant="active"
                title="可用账号"
                titleEn="Active · Healthy"
                count={(accounts.active || []).length}
                countClassName="stat-num-growth text-3xl md:text-4xl"
              >
                {(accounts.active || []).map((a) => (
                  <AccountInspectRow
                    key={a.id}
                    variant="active"
                    phoneLine={displayPhone(a)}
                    subLine={`今日使用: ${a.today_used_count || 0}`}
                    badge={<Badge glow status="可用" />}
                  />
                ))}
              </AccountMonitorColumn>
              <AccountMonitorColumn
                variant="limited"
                title="受限账号"
                titleEn="Limited · Daily / Long-term"
                count={(accounts.limited || []).length}
                countClassName="stat-num-warn text-3xl md:text-4xl"
              >
                {(accounts.limited || []).map((a) => (
                  <AccountInspectRow
                    key={a.id}
                    variant="limited"
                    phoneLine={displayPhone(a)}
                    subLine={`${a.lifecycle_primary || "LIMITED"} · 今日成功拉人: ${a.today_count ?? 0}`}
                    badge={<Badge glow status={a.lifecycle_sub || "受限"} />}
                  />
                ))}
              </AccountMonitorColumn>
              <AccountMonitorColumn
                variant="banned"
                title="疑似风控"
                titleEn="Risk · Review"
                count={(accounts.banned || []).length}
                countClassName="stat-num-risk text-3xl md:text-4xl"
              >
                {(accounts.banned || []).map((a) => (
                  <AccountInspectRow
                    key={a.id}
                    variant="banned"
                    phoneLine={displayPhone(a)}
                    subLine={`${a.lifecycle_primary || "RISK"} · 今日成功拉人: ${a.today_count ?? 0}`}
                    badge={
                      <Badge
                        glow
                        status={a.lifecycle_sub || "疑似风控"}
                      />
                    }
                    right={
                      <button
                        type="button"
                        className="rounded-lg border border-rose-400/35 bg-rose-500/10 px-2.5 py-1 text-xs font-medium text-rose-300 shadow-[0_0_12px_rgba(251,113,133,0.15)] transition hover:scale-105 hover:border-rose-400/50 hover:shadow-[0_0_20px_rgba(251,113,133,0.25)] active:scale-95"
                        onClick={() => onDeleteAccount(a.phone)}
                      >
                        删除
                      </button>
                    }
                  />
                ))}
              </AccountMonitorColumn>
            </div>
          </div>
        )}

        {tab === "代理监控" && (
          <div className="space-y-5">
            <div className="grid gap-5 sm:grid-cols-2 xl:grid-cols-4">
              <StatTileLg title="代理总数" value={proxyData.summary.total} icon={Network} tone="info" />
              <StatTileLg title="已使用数量" value={proxyData.summary.used} icon={Activity} tone="growth" />
              <StatTileLg title="空闲数量" value={proxyData.summary.idle} icon={Server} tone="info" />
              <StatTileLg title="失效数量" value={proxyData.summary.dead} icon={XCircle} tone="risk" />
            </div>
            <Card title="代理列表" accent="log">
              {!isAdmin ? <p className="mb-3 text-sm text-slate-500">当前为只读视图（管理员可操作代理）</p> : null}
              <div className={TABLE_WRAP}>
                <table className="w-full border-separate border-spacing-0 text-sm">
                  <thead className="border-b border-white/[0.06] bg-[rgba(255,255,255,0.04)] text-left text-[11px] font-medium uppercase tracking-wider text-slate-400">
                    <tr>
                      <th className="px-3 py-3">手机号</th>
                      <th className="px-3 py-3">代理类型</th>
                      <th className="px-3 py-3">代理值</th>
                      <th className="px-3 py-3">状态</th>
                      <th className="px-3 py-3">操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {proxyData.items.map((p) => (
                      <tr
                        key={p.id}
                        className="border-t border-white/[0.06] transition-colors duration-150 first:border-t-0 hover:bg-white/[0.04]"
                      >
                        <td className="px-3 py-2.5 text-slate-200">{p.phone || "-"}</td>
                        <td className="px-3 py-2.5 text-slate-400">
                          <span className="inline-flex items-center gap-1.5">
                            <Globe className="h-3.5 w-3.5 shrink-0 text-slate-400" aria-hidden />
                            {p.proxy_type || "-"}
                          </span>
                        </td>
                        <td className="max-w-[380px] truncate px-3 py-2.5 font-log text-xs text-slate-500">{p.proxy_value || "-"}</td>
                        <td className="px-3 py-2.5">
                          <span
                            className={`rounded-lg border px-2 py-0.5 text-xs font-medium ${
                              p.status === "idle"
                                ? "border-emerald-400/35 bg-emerald-500/10 text-emerald-300"
                                : p.status === "used"
                                  ? "border-cyan-400/35 bg-cyan-500/10 text-cyan-200"
                                  : "border-rose-400/35 bg-rose-500/10 text-rose-300"
                            }`}
                          >
                            {p.status}
                          </span>
                        </td>
                        <td className="px-3 py-2.5">
                          <div className="flex flex-wrap gap-2">
                            <button
                              type="button"
                              className="rounded-lg border border-rose-400/35 bg-rose-500/10 px-2 py-1 text-xs font-medium text-rose-300 transition hover:-translate-y-0.5 hover:shadow-[0_0_14px_rgba(251,113,133,0.2)] disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:translate-y-0"
                              onClick={() => p.proxy_id && onMarkProxyDead(p.proxy_id)}
                              disabled={!p.proxy_id || !isAdmin}
                            >
                              标记失效
                            </button>
                            <button
                              type="button"
                              className="rounded-lg border border-amber-400/35 bg-amber-500/10 px-2 py-1 text-xs font-medium text-amber-200 transition hover:-translate-y-0.5 hover:shadow-[0_0_14px_rgba(251,191,36,0.15)] disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:translate-y-0"
                              onClick={() => p.proxy_id && onUnbindProxy(p.proxy_id)}
                              disabled={!p.proxy_id || !isAdmin}
                            >
                              解绑账号
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          </div>
        )}

        {tab === "用户采集" && (
          <div className={`${SCRAPER_PAGE} mx-auto max-w-6xl`}>
            <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:gap-8">
              <div className="min-w-0 flex-1 space-y-5">
                <div className={SCRAPER_GLASS_CARD}>
                  <div className="mb-1 flex flex-wrap items-center justify-between gap-3">
                    <h3 className="text-base font-bold tracking-tight text-slate-100">采集账号</h3>
                    <button type="button" className={SCRAPER_BTN_GLOW_SM} onClick={openScraperAccountModal}>
                      更新账号
                    </button>
                  </div>
                  <p className="mb-4 text-xs leading-relaxed text-slate-500">
                    Telethon 独立 session，与账号池 / 代理池无关。
                  </p>
                  {scraperAccount?.status === "not_logged" || scraperAccount == null ? (
                    <p className="text-sm text-slate-500">
                      <span className="inline-flex items-center gap-2">
                        <span className="h-2 w-2 rounded-full bg-slate-600 ring-2 ring-white/10" />
                        未登录，请点击「更新账号」完成验证。
                      </span>
                    </p>
                  ) : scraperAccount.status === "active" ? (
                    <div className="flex flex-wrap items-center gap-3">
                      <span className="h-2.5 w-2.5 rounded-full bg-emerald-500 shadow-[0_0_10px_rgba(34,197,94,0.55)]" />
                      <span className="text-xs text-slate-500">手机号</span>
                      <span className="text-lg font-bold tabular-nums text-teal-200 drop-shadow-[0_0_14px_rgba(45,212,191,0.45)]">
                        {scraperAccount.phone}
                      </span>
                      <span className="rounded-lg border border-emerald-400/35 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-200">
                        active
                      </span>
                    </div>
                  ) : (
                    <div className="flex flex-wrap items-center gap-3 text-sm">
                      <span className="h-2.5 w-2.5 rounded-full bg-rose-500 shadow-[0_0_8px_rgba(244,63,94,0.45)]" />
                      <span className="text-xs text-slate-500">手机号</span>
                      <span className="font-bold tabular-nums text-slate-200">{scraperAccount.phone || "—"}</span>
                      <span className="rounded-lg border border-rose-400/35 bg-rose-500/10 px-2 py-0.5 text-[10px] font-semibold text-rose-300">
                        invalid
                      </span>
                      <span className="text-xs text-rose-400/90">请重新绑定</span>
                    </div>
                  )}
                </div>

                <div className={SCRAPER_GLASS_CARD}>
                  <h3 className="text-base font-bold tracking-tight text-slate-100">新建采集</h3>
                  <p className="mt-1 text-xs leading-relaxed text-slate-500">
                    按时间窗口扫描群内消息，提取 <span className="font-log">@username</span> 并去重保存；session 路径{" "}
                    <span className="font-log text-[10px]">data/scraper/</span>
                  </p>
                  <div className="mt-5 space-y-4">
                    <label className="flex flex-col gap-1.5">
                      <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">群组 ID / 用户名 / 链接</span>
                      <input
                        className={SCRAPER_FIELD}
                        placeholder="例如 username、-100… 或 t.me/…"
                        value={scraperForm.group_id}
                        onChange={(e) => setScraperForm((f) => ({ ...f, group_id: e.target.value }))}
                      />
                    </label>
                    <div className="grid gap-4 sm:grid-cols-2">
                      <div className="flex flex-col gap-1.5">
                        <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">时间范围</span>
                        <GlassDropdown
                          value={String(scraperForm.days)}
                          onChange={(v) => setScraperForm((f) => ({ ...f, days: Number(v) || 7 }))}
                          options={scraperDaysDropdownOptions}
                          placeholder="选择天数"
                          className="w-full"
                        />
                      </div>
                      <label className="flex flex-col gap-1.5">
                        <span className="text-[11px] font-semibold uppercase tracking-wider text-slate-500">最多扫描消息数</span>
                        <input
                          type="number"
                          min={1}
                          max={200000}
                          className={SCRAPER_FIELD}
                          value={scraperForm.max_messages}
                          onChange={(e) =>
                            setScraperForm((f) => ({ ...f, max_messages: Number(e.target.value) || 5000 }))
                          }
                        />
                      </label>
                    </div>
                    <button
                      type="button"
                      disabled={scraperLoading || !scraperForm.group_id.trim()}
                      className={SCRAPER_BTN_GLOW_BLOCK}
                      onClick={onRunScraper}
                    >
                      {scraperLoading ? (
                        <>
                          <UiSpinner tone="primary" />
                          采集中…
                        </>
                      ) : (
                        "开始采集"
                      )}
                    </button>
                  </div>
                </div>

                {scraperResult ? (
                  <div className={SCRAPER_GLASS_CARD}>
                    <h3 className="text-base font-bold tracking-tight text-slate-100">本次结果</h3>
                    <p className="mt-1 text-xs text-slate-500">当前任务摘要，可下载 txt 或从历史重复下载。</p>
                    <div className="mt-5 space-y-3">
                      <div>
                        <p className="text-[11px] font-medium text-slate-500">目标群组</p>
                        <p className="mt-0.5 break-all font-log text-sm font-semibold text-slate-200">{scraperResult.group_id}</p>
                      </div>
                      <div>
                        <p className="text-[11px] font-medium text-slate-500">去重用户</p>
                        <p className="stat-num-scraper mt-1 text-3xl tabular-nums tracking-tight">{scraperResult.count}</p>
                      </div>
                      <button
                        type="button"
                        disabled={scraperResultDownloadLoading}
                        className={`${SCRAPER_BTN_GLOW_BLOCK} mt-2`}
                        onClick={onDownloadScrape}
                      >
                        {scraperResultDownloadLoading ? (
                          <>
                            <UiSpinner tone="primary" />
                            下载中…
                          </>
                        ) : (
                          <>
                            <Download className="h-4 w-4 shrink-0" aria-hidden />
                            下载结果
                          </>
                        )}
                      </button>
                    </div>
                  </div>
                ) : null}
              </div>

              <aside className="w-full shrink-0 space-y-4 lg:sticky lg:top-24 lg:w-[360px]">
                <div className={SCRAPER_GLASS_CARD}>
                  <h3 className="text-base font-bold tracking-tight text-slate-100">采集历史</h3>
                  <p className="mt-1 text-xs text-slate-500">仅展示有用户的任务（user_count {">"} 0）。</p>
                </div>
                {scraperHistoryLoading && scraperTasks.length === 0 ? (
                  <div className={`${SCRAPER_GLASS_CARD} flex items-center justify-center gap-2 py-10 text-sm text-slate-500`}>
                    <UiSpinner tone="muted" />
                    加载中…
                  </div>
                ) : scraperTasksVisible.length === 0 ? (
                  <div className="rounded-2xl border border-dashed border-white/[0.12] bg-[rgba(255,255,255,0.02)] px-4 py-10 text-center text-sm text-slate-500 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)] backdrop-blur-[16px]">
                    暂无记录，完成一次采集后将显示在此
                  </div>
                ) : (
                  <ul className="growth-scroll flex max-h-[min(72vh,600px)] flex-col gap-4 overflow-y-auto pr-1">
                    {scraperTasksVisible.map((t) => {
                      const done = t.status === "done";
                      const dt = t.created_at
                        ? new Date(t.created_at).toLocaleString("zh-CN", { hour12: false })
                        : "—";
                      const titleName = (t.group_name && String(t.group_name).trim()) || t.group_link;
                      const subLink =
                        t.group_name && String(t.group_name).trim() && t.group_link !== titleName ? t.group_link : null;
                      return (
                        <li key={t.id} className={`${SCRAPER_HISTORY_CARD} ${done ? "" : "opacity-75"}`}>
                          <h4 className="line-clamp-2 text-[15px] font-bold leading-snug text-slate-100" title={titleName}>
                            {titleName}
                          </h4>
                          {subLink ? (
                            <p className="mt-1 truncate font-log text-[10px] text-slate-400" title={subLink}>
                              {subLink}
                            </p>
                          ) : null}
                          <div className="mt-4 flex items-end justify-between gap-3">
                            <div>
                              <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">用户</p>
                              <p className="stat-num-scraper text-3xl leading-none tabular-nums">{t.user_count}</p>
                            </div>
                            <div className="text-right">
                              <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">下载</p>
                              <p className="stat-num-log text-lg tabular-nums">{t.download_count ?? 0}</p>
                            </div>
                          </div>
                          <p className="mt-3 text-[11px] text-slate-400">{dt}</p>
                          {!done ? (
                            <p className="mt-2 text-[11px] font-semibold text-amber-400">
                              {t.status === "running" ? "未完成" : "失败"}
                            </p>
                          ) : null}
                          <button
                            type="button"
                            disabled={!done || scraperDownloadTaskId != null}
                            className={`${SCRAPER_BTN_GLOW_SM} mt-4 w-full`}
                            onClick={() => onDownloadScraperHistoryTask(t.id)}
                          >
                            {scraperDownloadTaskId === t.id ? (
                              <>
                                <UiSpinner tone="primary" />
                                下载中…
                              </>
                            ) : (
                              <>
                                <Download className="h-3.5 w-3.5 shrink-0" aria-hidden />
                                下载
                              </>
                            )}
                          </button>
                        </li>
                      );
                    })}
                  </ul>
                )}
              </aside>
            </div>
          </div>
        )}

        {tab === "消息Copy" && (
          <div className={`${COPY_PAGE} mx-auto max-w-7xl`}>
            <input
              ref={copySessionFileInputRef}
              type="file"
              accept=".session,application/octet-stream"
              className="hidden"
              onChange={onCopySessionFileSelected}
            />
            <div className="pointer-events-none absolute right-[10%] top-0 h-40 w-40 rounded-full bg-violet-500/10 blur-[72px]" aria-hidden />
            <div className="pointer-events-none absolute bottom-[20%] left-[5%] h-36 w-36 rounded-full bg-cyan-500/8 blur-[64px]" aria-hidden />
            <div className="relative z-[1] flex flex-col gap-6">
              <div
                className={`grid grid-cols-1 gap-6 ${isAdmin ? "xl:grid-cols-[minmax(300px,380px)_minmax(0,1fr)]" : ""}`}
              >
                {isAdmin ? (
                  <div className="flex min-w-0 flex-col gap-4">
                  <div className={COPY_GLASS_CARD}>
                    <h3 className="text-base font-bold tracking-tight text-slate-100">机器人库</h3>
                    <p className="mt-1 text-xs leading-relaxed text-slate-500">
                      Telethon 监听源频道；发送走 Bot API <span className="font-log">copyMessage</span>。凭证仅存服务端。
                    </p>
                    <div className="mt-4 space-y-3">
                      <label className="flex flex-col gap-1">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">api_id</span>
                        <input
                          type="number"
                          className={COPY_FIELD}
                          placeholder="整数"
                          value={copyBotForm.api_id}
                          onChange={(e) => setCopyBotForm((f) => ({ ...f, api_id: e.target.value }))}
                        />
                      </label>
                      <label className="flex flex-col gap-1">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">api_hash</span>
                        <input className={COPY_FIELD} placeholder="来自 my.telegram.org" value={copyBotForm.api_hash} onChange={(e) => setCopyBotForm((f) => ({ ...f, api_hash: e.target.value }))} />
                      </label>
                      <label className="flex flex-col gap-1">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">bot_token</span>
                        <input
                          type="password"
                          className={COPY_FIELD}
                          placeholder="BotFather 下发"
                          value={copyBotForm.bot_token}
                          onChange={(e) => setCopyBotForm((f) => ({ ...f, bot_token: e.target.value }))}
                        />
                      </label>
                      <button type="button" disabled={copyBotSaving} className={`${COPY_BTN_GLOW_SM} w-full justify-center`} onClick={onCreateCopyBot}>
                        {copyBotSaving ? "提交中…" : "添加到库"}
                      </button>
                    </div>
                  </div>
                  <div className="space-y-3">
                    {copyBots.length === 0 ? (
                      <p className="text-sm text-slate-500">暂无机器人，请先添加。</p>
                    ) : (
                      copyBots.map((b) => (
                        <div key={b.id} className={COPY_GLASS_CARD}>
                          <div className="flex flex-wrap items-start justify-between gap-2">
                            <div>
                              <p className="font-log text-xs text-slate-500">#{b.id}</p>
                              <p className="mt-0.5 text-sm font-semibold text-slate-200">
                                api_id <span className="tabular-nums text-violet-200">{b.api_id}</span>
                              </p>
                              <p className="mt-1 font-log text-[11px] text-slate-500">{b.bot_token_masked}</p>
                            </div>
                            <span
                              className={`shrink-0 rounded-lg border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide ${
                                b.status === "active"
                                  ? "border-emerald-400/35 bg-emerald-500/10 text-emerald-200"
                                  : "border-rose-400/35 bg-rose-500/10 text-rose-200"
                              }`}
                            >
                              {b.status === "active" ? "ACTIVE" : "ERROR"}
                            </span>
                            <span
                              className={`shrink-0 rounded-lg border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide ${
                                b.session_ready
                                  ? "border-cyan-400/35 bg-cyan-500/10 text-cyan-200"
                                  : "border-rose-400/40 bg-rose-500/12 text-rose-200"
                              }`}
                              title={b.session_name || ""}
                            >
                              {b.session_ready ? "SESSION OK" : "无 SESSION"}
                            </span>
                          </div>
                          {!b.session_ready ? (
                            <p className="mt-2 text-xs text-rose-400/90">未生成 session：请使用「导入 session」或删除后重新添加 Bot。</p>
                          ) : null}
                          {b.last_error ? (
                            <p className="mt-2 line-clamp-3 text-xs text-rose-400/90" title={b.last_error}>
                              {b.last_error}
                            </p>
                          ) : null}
                          <div className="mt-3 flex flex-wrap gap-2">
                            <button
                              type="button"
                              disabled={uploadingSessionBotId != null}
                              className="rounded-xl border border-violet-400/35 bg-violet-500/10 px-3 py-1.5 text-xs font-medium text-violet-200 transition hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-50"
                              onClick={() => triggerCopySessionImport(b.id)}
                            >
                              {uploadingSessionBotId === b.id ? "导入中…" : "导入 session"}
                            </button>
                            {b.status === "error" ? (
                              <button type="button" className={COPY_BTN_GLOW_SM} onClick={() => onResetCopyBot(b.id)}>
                                清除错误
                              </button>
                            ) : null}
                            <button
                              type="button"
                              className="rounded-xl border border-rose-400/35 bg-rose-500/10 px-3 py-1.5 text-xs font-medium text-rose-200 transition hover:-translate-y-0.5"
                              onClick={() => onDeleteCopyBot(b.id)}
                            >
                              删除
                            </button>
                          </div>
                        </div>
                      ))
                    )}
                  </div>
                </div>
                ) : null}

                <div className="flex min-w-0 flex-col gap-4">
                  <div className={COPY_GLASS_CARD}>
                    <h3 className="text-base font-bold tracking-tight text-slate-100">新建转发任务</h3>
                    {!isAdmin ? (
                      <p className="mt-1 text-xs text-slate-500">
                        Bot 由管理员在「机器人库」维护；你可从下拉框选择可用 Bot，仅可查看与管理自己的转发任务。
                      </p>
                    ) : null}
                    <p className="mt-1 text-xs text-slate-500">
                      只能从库中选择 Bot；新建为 <span className="font-log">idle</span>，点「启动」经{" "}
                      <span className="font-log">starting</span> 进入 <span className="font-log">running</span>。服务重启会自动恢复{" "}
                      <span className="font-log">running</span> 任务。
                    </p>
                    <div className="mt-4 grid gap-3 sm:grid-cols-2">
                      <label className="flex flex-col gap-1 sm:col-span-2">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">机器人</span>
                        <select
                          className={COPY_FIELD}
                          value={copyTaskForm.bot_id}
                          onChange={(e) => setCopyTaskForm((f) => ({ ...f, bot_id: e.target.value }))}
                        >
                          <option value="">选择库中 Bot…</option>
                          {copyBots.map((b) => (
                            <option key={b.id} value={String(b.id)} disabled={!b.session_ready}>
                              #{b.id} · {b.bot_token_masked}
                              {b.session_ready ? "" : " · 无session"} ({b.status})
                            </option>
                          ))}
                        </select>
                      </label>
                      <label className="flex flex-col gap-1 sm:col-span-2">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">来源频道 / 群</span>
                        <input className={COPY_FIELD} placeholder="@username 或 -100…" value={copyTaskForm.source_channel} onChange={(e) => setCopyTaskForm((f) => ({ ...f, source_channel: e.target.value }))} />
                      </label>
                      <label className="flex flex-col gap-1 sm:col-span-2">
                        <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">目标频道 / 群</span>
                        <input className={COPY_FIELD} placeholder="@username 或 -100…" value={copyTaskForm.target_channel} onChange={(e) => setCopyTaskForm((f) => ({ ...f, target_channel: e.target.value }))} />
                      </label>
                    </div>
                    <button type="button" disabled={copyTaskSaving} className={`${COPY_BTN_GLOW_SM} mt-4 w-full justify-center`} onClick={onCreateCopyTask}>
                      {copyTaskSaving ? "创建中…" : "创建任务（idle）"}
                    </button>
                  </div>

                  <div className={`${TABLE_WRAP} border-violet-400/12`}>
                    <div className="border-b border-violet-400/10 px-3 py-2">
                      <h4 className="text-sm font-semibold text-slate-200">Copy 任务</h4>
                      <p className="text-[11px] text-slate-500">
                        状态：<span className="font-log">idle / starting / running / paused / error</span> · 约 1.5s 轮询同步 · 今日/累计（UTC 日切）
                      </p>
                    </div>
                    <div className="max-h-[min(52vh,520px)] overflow-auto">
                      <table className="w-full min-w-[640px] border-collapse text-left text-sm">
                        <thead className="sticky top-0 z-[1] bg-[rgba(12,16,28,0.92)] backdrop-blur-md">
                          <tr className="border-b border-white/[0.06] text-[11px] uppercase tracking-wider text-slate-500">
                            <th className="px-3 py-2 font-semibold">来源</th>
                            <th className="px-3 py-2 font-semibold">目标</th>
                            <th className="px-3 py-2 font-semibold">Bot</th>
                            <th className="px-3 py-2 font-semibold">状态</th>
                            <th className="px-3 py-2 font-semibold">今日/总计</th>
                            <th className="px-3 py-2 font-semibold">操作</th>
                          </tr>
                        </thead>
                        <tbody>
                          {copyTasks.length === 0 ? (
                            <tr>
                              <td colSpan={6} className="px-3 py-8 text-center text-slate-500">
                                暂无任务
                              </td>
                            </tr>
                          ) : (
                            copyTasks.map((t) => {
                              const taskBot = copyBots.find((b) => b.id === t.bot_id);
                              const sessionOk = Boolean(taskBot?.session_ready);
                              const displaySt = resolveCopyDisplayStatus(t, copyStartOptimistic);
                              let stWrap =
                                "inline-flex items-center gap-1.5 rounded-lg border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide";
                              let stLabel = "IDLE";
                              if (displaySt === "running") {
                                stWrap +=
                                  " border-emerald-400/45 bg-emerald-500/15 text-emerald-200 shadow-[0_0_16px_rgba(34,197,94,0.35)] ring-1 ring-emerald-400/20";
                                stLabel = "RUNNING";
                              } else if (displaySt === "starting") {
                                stWrap +=
                                  " animate-pulse border-amber-400/45 bg-amber-500/15 text-amber-200 shadow-[0_0_14px_rgba(251,191,36,0.28)] ring-1 ring-amber-400/25";
                                stLabel = "STARTING";
                              } else if (displaySt === "error") {
                                stWrap += " border-rose-400/45 bg-rose-500/12 text-rose-200 ring-1 ring-rose-400/20";
                                stLabel = "ERROR";
                              } else if (displaySt === "paused") {
                                stWrap += " border-slate-500/35 bg-slate-600/15 text-slate-400";
                                stLabel = "PAUSED";
                              } else {
                                stWrap += " border-slate-500/30 bg-slate-600/12 text-slate-400";
                                stLabel = "IDLE";
                              }
                              const canPause = displaySt === "running" || displaySt === "starting";
                              const showMainSpinner = displaySt === "starting";
                              const primaryIsRetry = displaySt === "error";
                              const primaryHidden = displaySt === "running";
                              return (
                                <tr key={t.id} className="border-t border-white/[0.06] hover:bg-white/[0.03]">
                                  <td className="max-w-[140px] truncate px-3 py-2.5 font-log text-xs text-slate-200" title={t.source_channel}>
                                    {t.source_channel}
                                  </td>
                                  <td className="max-w-[140px] truncate px-3 py-2.5 font-log text-xs text-slate-200" title={t.target_channel}>
                                    {t.target_channel}
                                  </td>
                                  <td className="px-3 py-2.5 font-log text-xs text-violet-200/90">#{t.bot_id}</td>
                                  <td className="px-3 py-2.5">
                                    <span className={stWrap}>
                                      {displaySt === "starting" ? (
                                        <Loader className="h-3 w-3 shrink-0 animate-spin text-amber-200" aria-hidden />
                                      ) : null}
                                      {stLabel}
                                    </span>
                                    {displaySt === "error" && t.last_error ? (
                                      <p className="mt-1 line-clamp-2 text-[10px] text-rose-400/90" title={t.last_error}>
                                        {t.last_error}
                                      </p>
                                    ) : null}
                                    {displaySt !== "error" && t.last_error ? (
                                      <p className="mt-1 line-clamp-2 text-[10px] text-slate-500" title={t.last_error}>
                                        {t.last_error}
                                      </p>
                                    ) : null}
                                  </td>
                                  <td className="px-3 py-2.5 tabular-nums text-slate-300">
                                    {t.today_forwarded ?? 0} / {t.total_forwarded ?? 0}
                                  </td>
                                  <td className="px-3 py-2.5">
                                    <div className="flex flex-wrap gap-1.5">
                                      {primaryHidden ? null : showMainSpinner ? (
                                        <button
                                          type="button"
                                          disabled
                                          className="inline-flex cursor-not-allowed items-center gap-1.5 rounded-lg border border-amber-400/35 bg-amber-500/10 px-2 py-1 text-[11px] font-medium text-amber-200 opacity-80"
                                        >
                                          <UiSpinner tone="muted" />
                                          启动中…
                                        </button>
                                      ) : (
                                        <button
                                          type="button"
                                          disabled={!sessionOk || displaySt === "starting"}
                                          title={
                                            !sessionOk ? "未生成 session，请导入或由管理员处理" : undefined
                                          }
                                          className="rounded-lg border border-emerald-400/35 bg-emerald-500/10 px-2 py-1 text-[11px] font-medium text-emerald-200 transition hover:-translate-y-0.5 hover:shadow-[0_0_12px_rgba(52,211,153,0.2)] disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:translate-y-0"
                                          onClick={() => onStartCopyTask(t.id)}
                                        >
                                          {primaryIsRetry ? "重试" : "启动"}
                                        </button>
                                      )}
                                      {displaySt === "running" ? (
                                        <button
                                          type="button"
                                          className="rounded-lg border border-amber-400/35 bg-amber-500/12 px-2 py-1 text-[11px] font-medium text-amber-200 transition hover:-translate-y-0.5 hover:shadow-[0_0_12px_rgba(251,191,36,0.18)]"
                                          onClick={() => onPauseCopyTask(t.id)}
                                        >
                                          暂停
                                        </button>
                                      ) : (
                                        <button
                                          type="button"
                                          disabled={!canPause}
                                          className="rounded-lg border border-white/[0.08] bg-white/[0.04] px-2 py-1 text-[11px] font-medium text-slate-500 transition hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:translate-y-0"
                                          onClick={() => onPauseCopyTask(t.id)}
                                        >
                                          {displaySt === "starting" ? "取消" : "暂停"}
                                        </button>
                                      )}
                                      <button
                                        type="button"
                                        className="rounded-lg border border-rose-400/35 bg-rose-500/10 px-2 py-1 text-[11px] font-medium text-rose-200 transition hover:-translate-y-0.5"
                                        onClick={() => onDeleteCopyTask(t.id)}
                                      >
                                        删除
                                      </button>
                                    </div>
                                  </td>
                                </tr>
                              );
                            })
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              </div>

              <div className={`${COPY_GLASS_CARD} flex min-h-[200px] flex-col overflow-hidden`}>
                <div className="mb-2 flex items-center justify-between gap-2 border-b border-violet-400/10 pb-2">
                  <h3 className="text-sm font-bold text-slate-100">实时日志</h3>
                  <span className="text-[10px] text-slate-500">约每 1.5s 拉取任务与日志</span>
                </div>
                <div className="growth-scroll max-h-56 min-h-[160px] flex-1 overflow-y-auto rounded-lg border border-white/[0.06] bg-black/25 px-2 py-2 font-log text-[11px] leading-relaxed">
                  {copyLogs.length === 0 ? (
                    <p className="px-2 py-4 text-slate-500">暂无日志</p>
                  ) : (
                    copyLogs.map((line, idx) => {
                      const lv = (line.level || "info").toLowerCase();
                      const cls =
                        lv === "error"
                          ? "text-rose-400"
                          : lv === "warn"
                            ? "text-amber-300"
                            : "text-slate-300";
                      const tail = [line.task_id != null ? `task=${line.task_id}` : null, line.bot_id != null ? `bot=${line.bot_id}` : null]
                        .filter(Boolean)
                        .join(" ");
                      return (
                        <div key={`${line.ts}-${idx}`} className={`border-b border-white/[0.04] py-1.5 last:border-b-0 ${cls}`}>
                          <span className="text-slate-500">{line.ts}</span>
                          {tail ? <span className="ml-2 text-slate-500">[{tail}]</span> : null}
                          <span className="ml-2 break-words">{line.message}</span>
                        </div>
                      );
                    })
                  )}
                </div>
              </div>
            </div>
          </div>
        )}

        {tab === "用户管理" && (
          <Card
            title="用户权限管理"
            right={
              isAdmin ? (
                <button
                  type="button"
                  disabled={loadUsersLoading}
                  className={`${BTN_PRIMARY} inline-flex items-center justify-center gap-2 px-3 py-1.5 text-xs disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:translate-y-0`}
                  onClick={onLoadUsers}
                >
                  {loadUsersLoading ? (
                    <>
                      <UiSpinner tone="primary" />
                      刷新中...
                    </>
                  ) : (
                    "刷新用户"
                  )}
                </button>
              ) : null
            }
          >
            {!isAdmin ? (
              <p className="text-sm text-slate-500">仅管理员可查看和修改用户权限</p>
            ) : (
              <div className="space-y-2">
                {users.map((u) => (
                  <div
                    key={u.id}
                    className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-white/[0.08] bg-[rgba(255,255,255,0.03)] px-3 py-2.5 text-sm shadow-[0_8px_32px_rgba(0,0,0,0.35)] backdrop-blur-[16px] transition-all duration-[250ms] ease-out hover:-translate-y-1 hover:border-cyan-400/20 hover:shadow-[0_12px_40px_rgba(0,255,180,0.06)]"
                  >
                    <div className="flex min-w-0 flex-1 items-center gap-3">
                      <div className="grid h-9 w-9 shrink-0 place-items-center rounded-lg bg-cyan-500/12 text-cyan-300 ring-1 ring-cyan-400/25">
                        <UserCog size={18} strokeWidth={1.75} aria-hidden />
                      </div>
                      <div className="min-w-0 text-slate-200">
                        <div className="font-medium">
                          #{u.id} {u.username}
                        </div>
                        <div className="text-xs text-slate-500">角色 · {u.role}</div>
                      </div>
                    </div>
                    <GlassDropdown
                      value={u.role}
                      onChange={(role) => onChangeRole(u.id, role)}
                      options={userRoleDropdownOptions}
                      placeholder="角色"
                      className="min-w-[132px] max-w-[200px]"
                      triggerClassName="px-2.5 py-1.5 text-xs"
                    />
                  </div>
                ))}
              </div>
            )}
          </Card>
        )}
        </main>
      </div>

      {showPathModal && (
        <div
          className="fixed inset-0 z-40 flex items-center justify-center bg-[rgba(11,15,20,0.55)] p-4 backdrop-blur-md"
          role="presentation"
          onClick={() => setShowPathModal(false)}
        >
          <div
            className={`${MODAL_SHELL} max-h-[80vh] w-full max-w-[760px] overflow-hidden`}
            role="dialog"
            aria-modal="true"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="border-b border-white/[0.06] px-5 py-4">
              <div className="flex items-center justify-between gap-3">
                <h3 className="text-base font-semibold text-slate-100">账号路径管理</h3>
                <button type="button" className={`${BTN_SECONDARY} px-3 py-1.5 text-sm`} onClick={() => setShowPathModal(false)}>
                  关闭
                </button>
              </div>
            </div>
            <div className="space-y-4 px-5 py-4">
              <div className="flex flex-wrap items-center gap-2">
                <input
                  className={`min-w-0 flex-1 ${INPUT_FIELD} py-2.5`}
                  value={newPath}
                  onChange={(e) => setNewPath(e.target.value)}
                  placeholder="输入账号路径，例如 C:/Users/.../TGTDATAaccount"
                />
                <button
                  type="button"
                  disabled={pathSubmitLoading}
                  className={`${BTN_PRIMARY} inline-flex items-center justify-center gap-2 px-4 py-2.5 disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:translate-y-0`}
                  onClick={onAddOrUpdatePath}
                >
                  {pathSubmitLoading ? (
                    <>
                      <UiSpinner tone="primary" />
                      提交中...
                    </>
                  ) : editingPathId ? (
                    "保存"
                  ) : (
                    "添加"
                  )}
                </button>
              </div>
              <div className="growth-scroll max-h-[55vh] space-y-2 overflow-y-auto pr-1">
                {accountPaths.map((item) => (
                  <div
                    key={item.id}
                    className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-white/[0.08] bg-[rgba(255,255,255,0.03)] p-3 shadow-[0_8px_28px_rgba(0,0,0,0.35)] backdrop-blur-[16px] transition hover:border-cyan-400/20 hover:shadow-[0_0_20px_rgba(0,255,180,0.05)]"
                  >
                    <div className="min-w-0 flex-1 truncate font-log text-xs text-slate-400">{item.path}</div>
                    <div className="flex gap-2">
                      <button
                        type="button"
                        className="rounded-lg border border-amber-400/35 bg-amber-500/10 px-2.5 py-1 text-xs font-medium text-amber-200 transition hover:-translate-y-0.5 hover:shadow-[0_0_14px_rgba(251,191,36,0.15)]"
                        onClick={() => onEditPath(item)}
                      >
                        编辑
                      </button>
                      <button
                        type="button"
                        className="rounded-lg border border-rose-400/35 bg-rose-500/10 px-2.5 py-1 text-xs font-medium text-rose-300 transition hover:-translate-y-0.5 hover:shadow-[0_0_14px_rgba(251,113,133,0.2)]"
                        onClick={() => onDeletePath(item.id)}
                      >
                        删除
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {showScraperAccountModal && (
        <div
          className="fixed inset-0 z-40 flex items-center justify-center bg-[rgba(11,15,20,0.55)] p-4 backdrop-blur-md"
          role="presentation"
          onClick={closeScraperAccountModal}
        >
          <div
            className={`${MODAL_SHELL} w-full max-w-md overflow-hidden`}
            role="dialog"
            aria-modal="true"
            aria-labelledby="scraper-account-modal-title"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="border-b border-white/[0.06] px-5 py-4">
              <div className="flex items-center justify-between gap-3">
                <h3 id="scraper-account-modal-title" className="text-base font-semibold text-slate-100">
                  更新采集账号
                </h3>
                <button type="button" className={`${BTN_SECONDARY} px-3 py-1.5 text-sm`} onClick={closeScraperAccountModal}>
                  关闭
                </button>
              </div>
              <p className="mt-2 text-xs leading-relaxed text-slate-500">
                验证码与二步验证密码均在弹窗内完成；登录成功后 session 保存在服务端，全局仅一个采集账号。
              </p>
            </div>
            <div className="space-y-4 px-5 py-4">
              {scraperModalBanner ? (
                <div
                  className={`rounded-xl border px-3 py-2.5 text-sm font-medium ${
                    scraperModalBanner.kind === "success"
                      ? "border-emerald-400/35 bg-emerald-500/10 text-emerald-200"
                      : scraperModalBanner.kind === "error"
                        ? "border-rose-400/35 bg-rose-500/10 text-rose-200"
                        : "border-cyan-400/35 bg-cyan-500/10 text-cyan-200"
                  }`}
                  role="status"
                >
                  {scraperModalBanner.kind === "loading" ? (
                    <span className="inline-flex items-center gap-2">
                      <UiSpinner tone="muted" />
                      {scraperModalBanner.text}
                    </span>
                  ) : (
                    scraperModalBanner.text
                  )}
                </div>
              ) : null}

              <label className="flex flex-col gap-1.5">
                <span className="text-[11px] font-medium uppercase tracking-wider text-slate-500">手机号（含区号）</span>
                <input
                  className={INPUT_FIELD}
                  placeholder="例如 8613800138000 或 +8613800138000"
                  value={scraperBindPhone}
                  onChange={(e) => setScraperBindPhone(e.target.value)}
                  autoComplete="tel"
                  disabled={scraperBindLoginLoading || scraperSendCodeLoading}
                />
              </label>
              <button
                type="button"
                disabled={scraperSendCodeLoading || scraperBindLoginLoading || !scraperBindPhone.trim()}
                className={`${BTN_SECONDARY} inline-flex w-full items-center justify-center gap-2 disabled:cursor-not-allowed disabled:opacity-60`}
                onClick={onScraperSendCode}
              >
                {scraperSendCodeLoading ? (
                  <>
                    <UiSpinner tone="muted" />
                    发送中…
                  </>
                ) : (
                  "发送验证码"
                )}
              </button>
              <label className="flex flex-col gap-1.5">
                <span className="text-[11px] font-medium uppercase tracking-wider text-slate-500">验证码</span>
                <input
                  className={INPUT_FIELD}
                  placeholder="Telegram 中的登录码"
                  value={scraperBindCode}
                  onChange={(e) => setScraperBindCode(e.target.value)}
                  autoComplete="one-time-code"
                  disabled={scraperNeedPassword || scraperBindLoginLoading}
                />
              </label>
              {scraperNeedPassword ? (
                <label className="flex flex-col gap-1.5">
                  <span className="text-[11px] font-medium uppercase tracking-wider text-slate-500">二步验证密码</span>
                  <input
                    type="password"
                    className={INPUT_FIELD}
                    placeholder="Telegram 云密码"
                    value={scraperBindPassword}
                    onChange={(e) => setScraperBindPassword(e.target.value)}
                    autoComplete="current-password"
                    disabled={scraperBindLoginLoading}
                  />
                </label>
              ) : null}
              <button
                type="button"
                disabled={
                  scraperBindLoginLoading ||
                  scraperSendCodeLoading ||
                  !scraperBindPhone.trim() ||
                  (scraperNeedPassword
                    ? !scraperBindPassword.trim()
                    : !scraperBindCode.trim() || !scraperPhoneCodeHash)
                }
                className={`${BTN_PRIMARY} inline-flex w-full items-center justify-center gap-2 disabled:cursor-not-allowed disabled:opacity-60 disabled:hover:translate-y-0`}
                onClick={onScraperBindLogin}
              >
                {scraperBindLoginLoading ? (
                  <>
                    <UiSpinner tone="primary" />
                    登录中…
                  </>
                ) : (
                  "登录"
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
