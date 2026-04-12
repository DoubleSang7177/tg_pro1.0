import { useEffect, useState } from "react";
import { Lock, Sparkles, UserRound } from "lucide-react";
import { UiSpinner } from "./UiSpinner";

const GLOW_INPUT =
  "w-full rounded-xl border border-white/[0.1] bg-[rgba(8,14,26,0.85)] px-4 py-3 text-sm text-slate-100 outline-none backdrop-blur-xl transition-all duration-300 placeholder:text-slate-500 " +
  "shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_0_0_1px_rgba(0,0,0,0.2)] " +
  "hover:border-cyan-400/25 hover:shadow-[0_0_24px_rgba(34,211,238,0.12)] " +
  "focus:border-cyan-400/45 focus:shadow-[0_0_32px_rgba(34,211,238,0.22),0_0_60px_rgba(139,92,246,0.08)] focus:ring-0";

const TAB_BTN =
  "relative flex-1 rounded-lg py-2.5 text-center text-xs font-semibold uppercase tracking-wider transition-all duration-300";

const TAB_ACTIVE =
  "text-white shadow-[0_0_24px_rgba(34,211,238,0.25)] bg-gradient-to-r from-cyan-500/25 via-violet-500/20 to-emerald-500/20 border border-white/[0.12]";

const TAB_IDLE = "text-slate-500 hover:text-slate-300";

const SUBMIT_BTN =
  "group relative mt-2 w-full overflow-hidden rounded-xl py-3 text-sm font-bold tracking-wide text-slate-950 transition-all duration-300 " +
  "bg-gradient-to-r from-[#22d3ee] via-[#a78bfa] to-[#34d399] shadow-[0_0_28px_rgba(34,211,238,0.35),0_8px_32px_rgba(0,0,0,0.35)] " +
  "hover:scale-[1.02] hover:shadow-[0_0_40px_rgba(167,139,250,0.45),0_12px_40px_rgba(34,211,238,0.2)] active:scale-[0.98] " +
  "disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:scale-100";

/**
 * 全屏登录 / 注册（SaaS 交易后台风格）
 */
export function AuthPortal({ message, authLoading, onLogin, onRegister }) {
  const [tab, setTab] = useState("login");
  const [localErr, setLocalErr] = useState("");
  const [loginUser, setLoginUser] = useState("admin");
  const [loginPass, setLoginPass] = useState("admin123");
  const [regUser, setRegUser] = useState("");
  const [regPass, setRegPass] = useState("");
  const [regConfirm, setRegConfirm] = useState("");

  useEffect(() => {
    setLocalErr("");
  }, [tab]);

  const handleLogin = () => {
    onLogin?.(loginUser.trim(), loginPass);
  };

  const handleRegister = () => {
    setLocalErr("");
    const u = regUser.trim();
    const p = regPass;
    const c = regConfirm;
    if (p !== c) {
      setLocalErr("两次输入的密码不一致");
      return;
    }
    onRegister?.(u, p);
  };

  const banner = message || localErr;

  return (
    <div className="relative flex min-h-[100dvh] w-full flex-col items-center justify-center overflow-x-hidden bg-[#05080f] px-4 py-12 text-slate-200">
      {/* 背景光晕 */}
      <div
        className="pointer-events-none absolute -left-[20%] top-[-10%] h-[min(90vw,520px)] w-[min(90vw,520px)] rounded-full bg-gradient-to-br from-cyan-600/25 via-violet-600/15 to-transparent blur-[100px]"
        aria-hidden
      />
      <div
        className="pointer-events-none absolute bottom-[-15%] right-[-10%] h-[min(85vw,480px)] w-[min(85vw,480px)] rounded-full bg-gradient-to-tl from-emerald-600/20 via-cyan-500/10 to-transparent blur-[110px]"
        aria-hidden
      />
      <div
        className="pointer-events-none absolute left-1/2 top-1/2 h-px w-[min(120vw,900px)] -translate-x-1/2 -translate-y-1/2 rotate-[12deg] bg-gradient-to-r from-transparent via-cyan-400/20 to-transparent"
        aria-hidden
      />

      <div className="relative z-[1] w-full max-w-[420px]">
        {/* Logo */}
        <div className="mb-10 text-center">
          <div className="mx-auto mb-5 flex h-16 w-16 items-center justify-center rounded-2xl bg-gradient-to-br from-cyan-400 via-violet-500 to-emerald-400 p-[2px] shadow-[0_0_48px_rgba(34,211,238,0.35)]">
            <div className="flex h-full w-full items-center justify-center rounded-[14px] bg-[#070b14]">
              <Sparkles className="h-7 w-7 text-cyan-300" strokeWidth={1.75} aria-hidden />
            </div>
          </div>
          <h1 className="bg-gradient-to-r from-slate-100 via-cyan-100 to-slate-200 bg-clip-text text-2xl font-bold tracking-tight text-transparent">
            TG Pro Console
          </h1>
          <p className="mt-2 text-[11px] font-medium uppercase tracking-[0.2em] text-slate-500">Growth · Ops · Trading-grade UI</p>
        </div>

        {/* 渐变边框卡片 */}
        <div className="rounded-2xl bg-gradient-to-br from-cyan-500/35 via-violet-500/30 to-emerald-500/30 p-px shadow-[0_24px_80px_rgba(0,0,0,0.55),0_0_80px_rgba(34,211,238,0.08)]">
          <div className="rounded-[15px] bg-[rgba(6,10,18,0.92)] px-6 py-7 backdrop-blur-2xl">
            {/* Tab */}
            <div className="mb-6 flex gap-1 rounded-xl border border-white/[0.06] bg-black/30 p-1">
              <button
                type="button"
                className={`${TAB_BTN} ${tab === "login" ? TAB_ACTIVE : TAB_IDLE}`}
                onClick={() => setTab("login")}
              >
                登录
              </button>
              <button
                type="button"
                className={`${TAB_BTN} ${tab === "register" ? TAB_ACTIVE : TAB_IDLE}`}
                onClick={() => setTab("register")}
              >
                注册
              </button>
            </div>

            {tab === "login" ? (
              <div className="space-y-4">
                <div>
                  <label className="mb-1.5 flex items-center gap-2 text-[10px] font-bold uppercase tracking-wider text-slate-500">
                    <UserRound className="h-3.5 w-3.5 text-cyan-400/80" aria-hidden />
                    用户名
                  </label>
                  <input
                    className={GLOW_INPUT}
                    autoComplete="username"
                    value={loginUser}
                    onChange={(e) => setLoginUser(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && handleLogin()}
                  />
                </div>
                <div>
                  <label className="mb-1.5 flex items-center gap-2 text-[10px] font-bold uppercase tracking-wider text-slate-500">
                    <Lock className="h-3.5 w-3.5 text-violet-400/80" aria-hidden />
                    密码
                  </label>
                  <input
                    type="password"
                    className={GLOW_INPUT}
                    autoComplete="current-password"
                    value={loginPass}
                    onChange={(e) => setLoginPass(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && handleLogin()}
                  />
                </div>
                <button type="button" disabled={authLoading} className={SUBMIT_BTN} onClick={handleLogin}>
                  {authLoading ? (
                    <span className="inline-flex items-center justify-center gap-2">
                      <UiSpinner tone="primary" />
                      登录中…
                    </span>
                  ) : (
                    "进入控制台"
                  )}
                </button>
              </div>
            ) : (
              <div className="space-y-4">
                <div>
                  <label className="mb-1.5 flex items-center gap-2 text-[10px] font-bold uppercase tracking-wider text-slate-500">
                    <UserRound className="h-3.5 w-3.5 text-cyan-400/80" aria-hidden />
                    用户名
                  </label>
                  <input
                    className={GLOW_INPUT}
                    autoComplete="username"
                    placeholder="2～50 位，字母数字下划线或中文"
                    value={regUser}
                    onChange={(e) => setRegUser(e.target.value)}
                  />
                </div>
                <div>
                  <label className="mb-1.5 flex items-center gap-2 text-[10px] font-bold uppercase tracking-wider text-slate-500">
                    <Lock className="h-3.5 w-3.5 text-violet-400/80" aria-hidden />
                    密码
                  </label>
                  <input
                    type="password"
                    className={GLOW_INPUT}
                    autoComplete="new-password"
                    placeholder="至少 6 位"
                    value={regPass}
                    onChange={(e) => setRegPass(e.target.value)}
                  />
                </div>
                <div>
                  <label className="mb-1.5 flex items-center gap-2 text-[10px] font-bold uppercase tracking-wider text-slate-500">
                    <Lock className="h-3.5 w-3.5 text-emerald-400/80" aria-hidden />
                    确认密码
                  </label>
                  <input
                    type="password"
                    className={GLOW_INPUT}
                    autoComplete="new-password"
                    value={regConfirm}
                    onChange={(e) => setRegConfirm(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && handleRegister()}
                  />
                </div>
                <button type="button" disabled={authLoading} className={SUBMIT_BTN} onClick={handleRegister}>
                  {authLoading ? (
                    <span className="inline-flex items-center justify-center gap-2">
                      <UiSpinner tone="primary" />
                      注册中…
                    </span>
                  ) : (
                    "创建账号"
                  )}
                </button>
                <p className="text-center text-[10px] text-slate-600">注册后角色为普通用户（USER），管理员由后台指定。</p>
              </div>
            )}

            {banner ? (
              <p className="mt-5 text-center text-sm font-medium text-rose-400/95 drop-shadow-[0_0_12px_rgba(251,113,133,0.35)]">
                {banner}
              </p>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}
