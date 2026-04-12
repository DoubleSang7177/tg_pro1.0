import { createPortal } from "react-dom";
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { ChevronDown, KeyRound, LogOut, UserRound, X } from "lucide-react";
import { api, BASE_URL } from "../api";
import { UiSpinner } from "./UiSpinner";

const DOCK_GLASS =
  "rounded-xl border border-white/[0.1] bg-[rgba(8,12,22,0.75)] shadow-[0_8px_40px_rgba(0,0,0,0.45),0_0_32px_rgba(34,211,238,0.06)] backdrop-blur-xl transition-all duration-300 hover:border-cyan-400/20 hover:shadow-[0_12px_48px_rgba(0,0,0,0.5),0_0_40px_rgba(34,211,238,0.1)]";

const MENU_ITEM =
  "flex w-full items-center gap-2.5 rounded-lg px-3 py-2.5 text-left text-xs font-medium text-slate-200 transition-all duration-200 hover:scale-[1.02] hover:bg-white/[0.08] hover:text-white active:scale-[0.98]";

const MENU_Z = 2000;
const MODAL_Z = 3000;

const MODAL_PANEL =
  "relative w-full max-w-md max-h-[min(90vh,920px)] overflow-y-auto rounded-2xl border border-white/[0.1] bg-[rgba(8,12,22,0.95)] shadow-[0_24px_80px_rgba(0,0,0,0.65),0_0_60px_rgba(139,92,246,0.12)] backdrop-blur-2xl";

const GLOW_IN =
  "w-full rounded-xl border border-white/[0.1] bg-[rgba(6,10,18,0.9)] px-3 py-2.5 text-sm text-slate-100 outline-none transition-all duration-300 placeholder:text-slate-500 hover:border-cyan-400/25 focus:border-cyan-400/45 focus:shadow-[0_0_24px_rgba(34,211,238,0.15)]";

const BTN_GRAD =
  "inline-flex items-center justify-center gap-2 rounded-xl bg-gradient-to-r from-cyan-500 via-violet-500 to-emerald-500 px-4 py-2.5 text-xs font-bold text-slate-950 shadow-[0_0_24px_rgba(34,211,238,0.3)] transition-all duration-300 hover:scale-[1.02] hover:shadow-[0_0_36px_rgba(167,139,250,0.35)] active:scale-[0.98] disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:scale-100";

function computeUserMenuStyle(triggerEl, isHeader) {
  if (!triggerEl) return null;
  const r = triggerEl.getBoundingClientRect();
  const gap = 8;
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  const menuWidth = Math.min(240, vw - 16);
  let left = isHeader ? r.right - menuWidth : r.left;
  if (left < 8) left = 8;
  if (left + menuWidth > vw - 8) left = vw - menuWidth - 8;

  if (isHeader) {
    return {
      zIndex: MENU_Z,
      top: r.bottom + gap,
      left,
      width: menuWidth,
      maxHeight: Math.min(360, vh - r.bottom - gap - 12),
    };
  }
  const maxH = Math.min(320, Math.max(160, r.top - gap - 12));
  return {
    zIndex: MENU_Z,
    left,
    width: menuWidth,
    maxHeight: maxH,
    bottom: vh - r.top + gap,
  };
}

function avatarSrc(profile) {
  if (!profile?.avatar_url) return null;
  const u = profile.avatar_url;
  if (u.startsWith("http")) return u;
  return `${BASE_URL}${u.startsWith("/") ? u : `/${u}`}`;
}

/**
 * 头像 + 用户名 + 下拉（个人中心 / 修改密码 / 退出）
 * @param {"sidebar" | "header"} variant — header：顶栏右上角，菜单向下展开
 */
export function UserAccountDock({ profile, setProfile, isAdmin, onLogout, postAuthSync, variant = "sidebar" }) {
  const [avatarBust, setAvatarBust] = useState(0);
  const [menuOpen, setMenuOpen] = useState(false);
  const [profileOpen, setProfileOpen] = useState(false);
  const [pwdOpen, setPwdOpen] = useState(false);
  const wrapRef = useRef(null);
  const triggerRef = useRef(null);
  const menuPanelRef = useRef(null);
  const [menuStyle, setMenuStyle] = useState(null);

  const [uname, setUname] = useState(profile?.username || "");
  const [profileSaving, setProfileSaving] = useState(false);
  const [profileMsg, setProfileMsg] = useState("");
  const avatarInputRef = useRef(null);

  const [curPwd, setCurPwd] = useState("");
  const [newPwd, setNewPwd] = useState("");
  const [newPwd2, setNewPwd2] = useState("");
  const [pwdSaving, setPwdSaving] = useState(false);
  const [pwdMsg, setPwdMsg] = useState("");

  useEffect(() => {
    setUname(profile?.username || "");
  }, [profile?.username]);

  const updateMenuPosition = useCallback(() => {
    const el = triggerRef.current;
    if (!el || !menuOpen) return;
    setMenuStyle(computeUserMenuStyle(el, variant === "header"));
  }, [menuOpen, variant]);

  useLayoutEffect(() => {
    if (!menuOpen) {
      setMenuStyle(null);
      return;
    }
    updateMenuPosition();
  }, [menuOpen, updateMenuPosition]);

  useEffect(() => {
    if (!menuOpen) return;
    const onScroll = () => updateMenuPosition();
    const onResize = () => updateMenuPosition();
    window.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", onResize);
    return () => {
      window.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", onResize);
    };
  }, [menuOpen, updateMenuPosition]);

  useEffect(() => {
    const onDoc = (e) => {
      const t = e.target;
      if (wrapRef.current?.contains(t)) return;
      if (menuPanelRef.current?.contains(t)) return;
      setMenuOpen(false);
    };
    document.addEventListener("click", onDoc);
    return () => document.removeEventListener("click", onDoc);
  }, []);

  const letter = (profile?.username || "?").slice(0, 1).toUpperCase();
  const rawSrc = avatarSrc(profile);
  const src = rawSrc ? `${rawSrc}${rawSrc.includes("?") ? "&" : "?"}_=${avatarBust}` : null;

  const applyUserPayload = (u, bumpAvatar = false) => {
    if (!u) return;
    if (bumpAvatar) setAvatarBust((n) => n + 1);
    setProfile({
      id: u.id,
      username: u.username,
      role: u.role,
      avatar_url: u.avatar_url ?? null,
    });
  };

  const saveProfile = async () => {
    setProfileMsg("");
    setProfileSaving(true);
    try {
      const r = await api.updateProfile(uname.trim());
      applyUserPayload(r.user);
      await postAuthSync?.();
      setProfileMsg("已保存");
    } catch (e) {
      setProfileMsg(e?.message || "保存失败");
    } finally {
      setProfileSaving(false);
    }
  };

  const onPickAvatar = async (e) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;
    setProfileMsg("");
    setProfileSaving(true);
    try {
      const r = await api.uploadAvatar(file);
      applyUserPayload(r.user, true);
      await postAuthSync?.();
      setProfileMsg("头像已更新");
    } catch (err) {
      setProfileMsg(err?.message || "上传失败");
    } finally {
      setProfileSaving(false);
    }
  };

  const savePassword = async () => {
    setPwdMsg("");
    if (newPwd !== newPwd2) {
      setPwdMsg("两次新密码不一致");
      return;
    }
    setPwdSaving(true);
    try {
      await api.changePassword(curPwd, newPwd);
      setCurPwd("");
      setNewPwd("");
      setNewPwd2("");
      setPwdMsg("密码已更新");
      setTimeout(() => setPwdOpen(false), 800);
    } catch (e) {
      setPwdMsg(e?.message || "修改失败");
    } finally {
      setPwdSaving(false);
    }
  };

  const isHeader = variant === "header";
  const wrapClass = isHeader ? "relative" : "relative border-t border-white/[0.06] p-3";
  const btnClass = isHeader
    ? `${DOCK_GLASS} flex max-w-[220px] items-center gap-2.5 rounded-xl px-2.5 py-2 text-left`
    : `${DOCK_GLASS} flex w-full items-center gap-3 px-3 py-2.5 text-left`;

  return (
    <div className={wrapClass} ref={wrapRef}>
      <button
        ref={triggerRef}
        type="button"
        className={btnClass}
        onClick={() => setMenuOpen((v) => !v)}
      >
        <div
          className={`relative shrink-0 overflow-hidden rounded-xl border border-cyan-400/25 bg-gradient-to-br from-cyan-500/30 to-violet-600/30 shadow-[0_0_20px_rgba(34,211,238,0.2)] ${isHeader ? "h-9 w-9" : "h-10 w-10"}`}
        >
          {src ? (
            <img src={src} alt="" className="h-full w-full object-cover" />
          ) : (
            <span className="flex h-full w-full items-center justify-center text-sm font-bold text-cyan-100">{letter}</span>
          )}
        </div>
        <div className="min-w-0 flex-1">
          <div className="truncate text-xs font-semibold text-slate-100">{profile?.username}</div>
          <div className="truncate text-[10px] font-medium uppercase tracking-wider text-slate-500">
            {isAdmin ? "Administrator" : "User"}
          </div>
        </div>
        <ChevronDown
          className={`h-4 w-4 shrink-0 text-slate-500 transition-transform duration-300 ${menuOpen ? "rotate-180 text-cyan-400" : ""}`}
          aria-hidden
        />
      </button>

      {typeof document !== "undefined" && menuOpen && menuStyle
        ? createPortal(
            <div
              ref={menuPanelRef}
              style={{
                position: "fixed",
                zIndex: menuStyle.zIndex,
                left: menuStyle.left,
                width: menuStyle.width,
                maxHeight: menuStyle.maxHeight,
                ...(menuStyle.top != null ? { top: menuStyle.top } : {}),
                ...(menuStyle.bottom != null ? { bottom: menuStyle.bottom } : {}),
              }}
              className="flex flex-col overflow-y-auto overflow-x-hidden rounded-xl border border-white/[0.12] bg-[rgba(10,14,24,0.97)] py-1 shadow-[0_16px_48px_rgba(0,0,0,0.55),0_0_40px_rgba(34,211,238,0.08)] backdrop-blur-xl"
            >
              <button
                type="button"
                className={MENU_ITEM}
                onClick={() => {
                  setMenuOpen(false);
                  setProfileOpen(true);
                  setProfileMsg("");
                }}
              >
                <UserRound className="h-4 w-4 text-cyan-400" />
                个人中心
              </button>
              <button
                type="button"
                className={MENU_ITEM}
                onClick={() => {
                  setMenuOpen(false);
                  setPwdOpen(true);
                  setPwdMsg("");
                }}
              >
                <KeyRound className="h-4 w-4 text-violet-400" />
                修改密码
              </button>
              <div className="mx-2 my-1 h-px bg-white/[0.06]" />
              <button
                type="button"
                className={`${MENU_ITEM} text-rose-300 hover:bg-rose-500/10 hover:text-rose-200`}
                onClick={() => {
                  setMenuOpen(false);
                  onLogout?.();
                }}
              >
                <LogOut className="h-4 w-4" />
                退出登录
              </button>
            </div>,
            document.body,
          )
        : null}

      {/* 个人中心 */}
      {profileOpen ? (
        <div
          className="fixed inset-0 flex items-center justify-center bg-black/60 px-4 backdrop-blur-sm"
          style={{ zIndex: MODAL_Z }}
        >
          <div className={MODAL_PANEL} onClick={(e) => e.stopPropagation()}>
            <div className="border-b border-white/[0.06] bg-gradient-to-r from-cyan-500/10 via-violet-500/8 to-transparent px-5 py-4">
              <div className="flex items-center justify-between gap-3">
                <h3 className="text-sm font-bold tracking-tight text-white">个人中心</h3>
                <button
                  type="button"
                  className="rounded-lg p-1.5 text-slate-500 transition hover:bg-white/[0.08] hover:text-white"
                  onClick={() => setProfileOpen(false)}
                  aria-label="关闭"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            </div>
            <div className="space-y-4 p-5">
              <div className="flex flex-col items-center gap-3">
                <div className="relative h-24 w-24 overflow-hidden rounded-2xl border border-cyan-400/30 bg-gradient-to-br from-cyan-500/20 to-violet-600/25 shadow-[0_0_32px_rgba(34,211,238,0.2)]">
                  {src ? (
                    <img src={src} alt="" className="h-full w-full object-cover" />
                  ) : (
                    <span className="flex h-full w-full items-center justify-center text-3xl font-bold text-cyan-100">{letter}</span>
                  )}
                </div>
                <input ref={avatarInputRef} type="file" accept="image/png,image/jpeg,image/webp,image/gif" className="hidden" onChange={onPickAvatar} />
                <button type="button" className={BTN_GRAD} disabled={profileSaving} onClick={() => avatarInputRef.current?.click()}>
                  {profileSaving ? <UiSpinner tone="primary" /> : null}
                  上传头像
                </button>
              </div>
              <div>
                <label className="mb-1 block text-[10px] font-bold uppercase tracking-wider text-slate-500">用户名</label>
                <input className={GLOW_IN} value={uname} onChange={(e) => setUname(e.target.value)} autoComplete="username" />
              </div>
              <div className="flex gap-2">
                <button type="button" className={`${BTN_GRAD} flex-1`} disabled={profileSaving} onClick={saveProfile}>
                  {profileSaving ? <UiSpinner tone="primary" /> : null}
                  保存资料
                </button>
              </div>
              {profileMsg ? <p className="text-center text-xs text-slate-400">{profileMsg}</p> : null}
            </div>
          </div>
        </div>
      ) : null}

      {/* 修改密码 */}
      {pwdOpen ? (
        <div
          className="fixed inset-0 flex items-center justify-center bg-black/60 px-4 backdrop-blur-sm"
          style={{ zIndex: MODAL_Z }}
        >
          <div className={MODAL_PANEL} onClick={(e) => e.stopPropagation()}>
            <div className="border-b border-white/[0.06] bg-gradient-to-r from-violet-500/10 via-cyan-500/8 to-transparent px-5 py-4">
              <div className="flex items-center justify-between gap-3">
                <h3 className="text-sm font-bold tracking-tight text-white">修改密码</h3>
                <button
                  type="button"
                  className="rounded-lg p-1.5 text-slate-500 transition hover:bg-white/[0.08] hover:text-white"
                  onClick={() => setPwdOpen(false)}
                  aria-label="关闭"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            </div>
            <div className="space-y-3 p-5">
              <div>
                <label className="mb-1 block text-[10px] font-bold uppercase tracking-wider text-slate-500">当前密码</label>
                <input type="password" className={GLOW_IN} value={curPwd} onChange={(e) => setCurPwd(e.target.value)} autoComplete="current-password" />
              </div>
              <div>
                <label className="mb-1 block text-[10px] font-bold uppercase tracking-wider text-slate-500">新密码</label>
                <input type="password" className={GLOW_IN} value={newPwd} onChange={(e) => setNewPwd(e.target.value)} autoComplete="new-password" />
              </div>
              <div>
                <label className="mb-1 block text-[10px] font-bold uppercase tracking-wider text-slate-500">确认新密码</label>
                <input type="password" className={GLOW_IN} value={newPwd2} onChange={(e) => setNewPwd2(e.target.value)} autoComplete="new-password" />
              </div>
              <button type="button" className={`${BTN_GRAD} w-full`} disabled={pwdSaving} onClick={savePassword}>
                {pwdSaving ? <UiSpinner tone="primary" /> : null}
                更新密码
              </button>
              {pwdMsg ? <p className="text-center text-xs text-slate-400">{pwdMsg}</p> : null}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
