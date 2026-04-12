import { useEffect, useState } from "react";
import { Lock, Sparkles, UserRound, X } from "lucide-react";
import { UiSpinner } from "./UiSpinner";

const GLOW_INPUT =
  "w-full rounded-xl border border-white/[0.1] bg-[rgba(8,14,26,0.72)] px-4 py-3 text-sm text-slate-100 outline-none backdrop-blur-xl transition-all duration-300 placeholder:text-slate-500 " +
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

const PANEL_GLASS =
  "relative w-full max-w-[400px] overflow-hidden rounded-2xl border border-white/[0.12] bg-[rgba(6,10,18,0.72)] shadow-[0_24px_80px_rgba(0,0,0,0.65),0_0_80px_rgba(34,211,238,0.1)] backdrop-blur-[28px]";

/**
 * 登录 / 注册弹窗：玻璃拟态 + 渐变按钮，进入/退出 fade + scale
 */
export function AuthModal({
  open,
  onClose,
  initialTab = "login",
  message,
  authLoading,
  onLogin,
  onRegister,
}) {
  const [tab, setTab] = useState(initialTab);
  const [localErr, setLocalErr] = useState("");
  const [loginUser, setLoginUser] = useState("");
  const [loginPass, setLoginPass] = useState("");
  const [regUser, setRegUser] = useState("");
  const [regPass, setRegPass] = useState("");
  const [regConfirm, setRegConfirm] = useState("");
  const [entered, setEntered] = useState(false);

  useEffect(() => {
    if (!open) {
      setEntered(false);
      return undefined;
    }
    const id = requestAnimationFrame(() => setEntered(true));
    return () => cancelAnimationFrame(id);
  }, [open]);

  useEffect(() => {
    if (open) setTab(initialTab);
  }, [open, initialTab]);

  useEffect(() => {
    setLocalErr("");
  }, [tab]);

  if (!open) return null;

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
    <div
      className={`fixed inset-0 z-[3000] flex items-center justify-center px-4 py-8 transition-opacity duration-300 ease-out ${
        entered ? "bg-black/65 opacity-100 backdrop-blur-md" : "bg-black/40 opacity-0 backdrop-blur-none"
      }`}
      role="dialog"
      aria-modal="true"
      aria-labelledby="auth-modal-title"
      onClick={(e) => e.target === e.currentTarget && onClose?.()}
    >
      <div
        className={`${PANEL_GLASS} transition-all duration-300 ease-out ${
          entered ? "translate-y-0 scale-100 opacity-100" : "translate-y-2 scale-[0.96] opacity-0"
        }`}
        onClick={(e) => e.stopPropagation()}
      >
        <div
          className="pointer-events-none absolute -right-20 -top-20 h-48 w-48 rounded-full bg-gradient-to-br from-cyan-500/20 via-violet-500/15 to-transparent blur-[64px]"
          aria-hidden
        />
        <div
          className="pointer-events-none absolute -bottom-16 -left-16 h-40 w-40 rounded-full bg-gradient-to-tr from-emerald-500/15 to-transparent blur-[56px]"
          aria-hidden
        />

        <div className="relative flex items-center justify-between gap-3 border-b border-white/[0.06] px-5 py-4">
          <div className="flex items-center gap-2.5">
            <div className="grid h-9 w-9 place-items-center rounded-xl bg-gradient-to-br from-cyan-400/30 to-violet-500/30 shadow-[0_0_20px_rgba(34,211,238,0.25)]">
              <Sparkles className="h-4 w-4 text-cyan-200" strokeWidth={2} aria-hidden />
            </div>
            <div>
              <h2 id="auth-modal-title" className="text-sm font-bold tracking-tight text-white">
                TG Pro
              </h2>
              <p className="text-[10px] font-medium uppercase tracking-wider text-slate-500">登录以操作控制台</p>
            </div>
          </div>
          <button
            type="button"
            className="rounded-lg p-2 text-slate-500 transition hover:bg-white/[0.08] hover:text-white"
            onClick={() => onClose?.()}
            aria-label="关闭"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="relative px-5 pb-6 pt-5">
          <div className="mb-5 flex gap-1 rounded-xl border border-white/[0.06] bg-black/25 p-1 backdrop-blur-md">
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
                  "登录"
                )}
              </button>
              <p className="text-center text-xs text-slate-500">
                没有账号？{" "}
                <button type="button" className="text-cyan-400/90 underline-offset-2 hover:underline" onClick={() => setTab("register")}>
                  去注册
                </button>
              </p>
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
                  "注册"
                )}
              </button>
              <p className="text-center text-[10px] text-slate-600">注册后角色为普通用户（USER），管理员由后台指定。</p>
              <p className="text-center text-xs text-slate-500">
                已有账号？{" "}
                <button type="button" className="text-cyan-400/90 underline-offset-2 hover:underline" onClick={() => setTab("login")}>
                  去登录
                </button>
              </p>
            </div>
          )}

          {banner ? (
            <p className="mt-4 text-center text-sm font-medium text-rose-400/95 drop-shadow-[0_0_12px_rgba(251,113,133,0.35)]">{banner}</p>
          ) : null}
        </div>
      </div>
    </div>
  );
}
