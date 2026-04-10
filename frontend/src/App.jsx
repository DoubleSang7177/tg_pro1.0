import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "./api";

const menus = ["拉新", "账号检测", "目标群组", "代理监控", "用户管理"];

function Card({ title, children, right, className = "" }) {
  return (
    <div className={`rounded-2xl border border-slate-700/60 bg-slate-900/70 p-4 shadow-xl ${className}`}>
      <div className="mb-3 flex items-center justify-between">
        <h3 className="font-semibold text-slate-100">{title}</h3>
        {right}
      </div>
      {children}
    </div>
  );
}

function Badge({ status }) {
  const s = (status || "").toLowerCase();
  const cls =
    s.includes("正常") || s === "normal" || s === "active"
      ? "bg-emerald-500/20 text-emerald-300 border-emerald-400/30"
      : s.includes("风控") || s === "banned" || s.includes("受限")
        ? "bg-rose-500/20 text-rose-300 border-rose-400/30"
        : "bg-amber-500/20 text-amber-300 border-amber-400/30";
  return <span className={`rounded-full border px-2 py-0.5 text-xs ${cls}`}>{status}</span>;
}

function displayPhone(account) {
  if (account?.formatted_phone) return String(account.formatted_phone).replace(/^#/, "+");
  const digits = String(account?.phone || "").replace(/\D/g, "");
  return digits ? `+${digits}` : "+unknown";
}

export default function App() {
  const [tab, setTab] = useState("拉新");
  const [auth, setAuth] = useState({ username: "user", password: "user123" });
  const [profile, setProfile] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [accounts, setAccounts] = useState({ active: [], limited: [], banned: [] });
  const [groups, setGroups] = useState([]);
  const [users, setUsers] = useState([]);
  const [selectedGroup, setSelectedGroup] = useState("");
  const [forcedGroups, setForcedGroups] = useState([]);
  const [removedGroups, setRemovedGroups] = useState([]);
  const [forceCandidate, setForceCandidate] = useState("");
  const [logs, setLogs] = useState(["[system] dashboard initialized"]);
  const [msg, setMsg] = useState("");
  const logRef = useRef(null);
  const [uploadFile, setUploadFile] = useState(null);
  const [form, setForm] = useState({ users: "", accounts_path: "" });

  const isAdmin = useMemo(() => profile?.role === "admin", [profile]);
  const availableAccounts = useMemo(() => accounts.active || [], [accounts]);
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

  const appendLog = (line) => {
    const ts = new Date().toLocaleTimeString("zh-CN", { hour12: false });
    setLogs((prev) => [...prev.slice(-300), `[${ts}] ${line}`]);
  };

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [logs]);

  const refreshBase = async () => {
    try {
      const [t, a, g] = await Promise.all([api.listTasks(), api.listAccounts(), api.listGroups()]);
      setTasks(t.tasks || []);
      setAccounts({ active: a.active || [], limited: a.limited || [], banned: a.banned || [] });
      setGroups(g.groups || []);
      appendLog("sync ok");
    } catch (e) {
      setMsg(e.message);
      appendLog(`sync failed | ${e.message}`);
    }
  };

  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) return;
    api.me()
      .then((r) => setProfile({ username: r.username, role: r.role }))
      .then(refreshBase)
      .catch(() => localStorage.removeItem("token"));
  }, []);

  const login = async () => {
    try {
      const res = await api.login(auth.username, auth.password);
      localStorage.setItem("token", res.token);
      setProfile({ username: res.username, role: res.role });
      await refreshBase();
      if (res.role === "admin") {
        const list = await api.listUsers();
        setUsers(list.users || []);
      }
      setMsg(`登录成功：${res.username}`);
    } catch (e) {
      setMsg(e.message);
    }
  };

  const logout = () => {
    localStorage.removeItem("token");
    setProfile(null);
    setTasks([]);
    setAccounts({ active: [], limited: [], banned: [] });
    setGroups([]);
    setUsers([]);
      setSelectedGroup("");
      setForcedGroups([]);
      setRemovedGroups([]);
  };

  const onUpload = async () => {
    if (!uploadFile) return setMsg("请选择 zip 文件");
    try {
      await api.uploadAccount(uploadFile);
      setMsg("上传成功");
      await refreshBase();
    } catch (e) {
      setMsg(e.message);
    }
  };

  const onStartTask = async () => {
    try {
      await api.startTask({
        group: selectedGroup,
        users: form.users.split("\n").map((x) => x.trim()).filter(Boolean),
        accounts_path: form.accounts_path,
      });
      appendLog(`task started | group=${selectedGroup} accounts_auto=${availableAccounts.length}`);
      setMsg("任务已启动");
      await refreshBase();
    } catch (e) {
      setMsg(e.message);
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

  const onLoadUsers = async () => {
    const list = await api.listUsers();
    setUsers(list.users || []);
  };

  const onChangeRole = async (id, role) => {
    await api.updateUserRole(id, role);
    await onLoadUsers();
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

  return (
    <div className="min-h-screen bg-slate-950 text-slate-200">
      <header className="fixed left-0 top-0 z-20 w-full border-b border-slate-800 bg-slate-950/90 backdrop-blur">
        <div className="mx-auto flex h-16 max-w-[1500px] items-center justify-between px-6">
          <div className="flex items-center gap-3">
            <div className="grid h-9 w-9 place-items-center rounded-lg bg-blue-600 font-bold text-white">TG</div>
            <div className="font-semibold tracking-wide">Telegram拉新系统</div>
          </div>
          <nav className="hidden items-center gap-2 md:flex">
            {menus.filter((m) => (m === "用户管理" ? isAdmin : true)).map((m) => (
              <button
                key={m}
                onClick={() => setTab(m)}
                className={`rounded-lg px-4 py-2 text-sm transition ${tab === m ? "bg-blue-600 text-white" : "text-slate-300 hover:bg-slate-800"}`}
              >
                {m}
              </button>
            ))}
          </nav>
          <div className="flex items-center gap-2 text-sm">
            {!profile ? (
              <>
                <input className="w-28 rounded-lg border border-slate-700 bg-slate-900 px-2 py-1" value={auth.username} onChange={(e) => setAuth((v) => ({ ...v, username: e.target.value }))} />
                <input type="password" className="w-28 rounded-lg border border-slate-700 bg-slate-900 px-2 py-1" value={auth.password} onChange={(e) => setAuth((v) => ({ ...v, password: e.target.value }))} />
                <button className="rounded-lg bg-blue-600 px-3 py-1.5 transition hover:bg-blue-500" onClick={login}>登录 / 注册</button>
              </>
            ) : (
              <>
                <span>{profile.username} ({profile.role})</span>
                <button className="rounded-lg bg-slate-800 px-3 py-1.5 transition hover:bg-slate-700" onClick={logout}>退出</button>
              </>
            )}
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1500px] px-6 pb-8 pt-24">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-xl font-semibold">{tab}</h2>
          <button className="rounded-lg bg-slate-800 px-3 py-2 text-sm transition hover:bg-slate-700" onClick={refreshBase}>刷新数据</button>
        </div>
        {msg ? <p className="mb-4 text-sm text-rose-300">{msg}</p> : null}

        {tab === "拉新" && (
          <>
            <div className="mb-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
              <Card title="今日拉新人数"><p className="text-3xl font-bold text-blue-300">{stats.todayAdd}</p></Card>
              <Card title="昨日拉新人数"><p className="text-3xl font-bold text-indigo-300">{stats.yestAdd}</p></Card>
              <Card title="总拉新人数"><p className="text-3xl font-bold text-cyan-300">{stats.total}</p></Card>
              <Card title="可用账号数量"><p className="text-3xl font-bold text-emerald-300">{stats.accounts}</p></Card>
            </div>
            <div className="grid gap-4 xl:grid-cols-12">
              <Card title="账号列表（仅可用）" className="xl:col-span-3">
                <div className="max-h-[420px] space-y-2 overflow-auto pr-1">
                  {availableAccounts.map((a) => (
                    <div key={a.id} className="flex items-center justify-between rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-sm">
                      <div>
                        <div className="font-medium">{displayPhone(a)}</div>
                        <div className="text-xs text-slate-400">今日使用 {a.today_used_count || 0}</div>
                      </div>
                      <Badge status="自动顺序使用" />
                    </div>
                  ))}
                </div>
              </Card>
              <Card title="任务控制面板" className="xl:col-span-9">
                <div className="grid gap-3">
                  <div className="rounded-lg border border-slate-700 bg-slate-900 p-3">
                    <p className="mb-2 text-sm text-slate-300">目标群组（单选）</p>
                    <div className="flex items-center gap-2">
                      <select
                        className="min-w-[360px] rounded border border-slate-700 bg-slate-950 px-2 py-2 text-sm"
                        value={selectedGroup}
                        onChange={(e) => setSelectedGroup(e.target.value)}
                      >
                        <option value="">请选择目标群组</option>
                        {availableGroups.map((username) => (
                          <option key={username} value={username}>
                            {username}
                          </option>
                        ))}
                      </select>
                      <select
                        className="min-w-[220px] rounded border border-slate-700 bg-slate-950 px-2 py-2 text-sm"
                        value={forceCandidate}
                        onChange={(e) => setForceCandidate(e.target.value)}
                      >
                        <option value="">选择强制加入群组</option>
                        {hiddenGroups.map((g) => (
                          <option key={`hidden-${g.id}`} value={g.username}>
                            {g.username}
                          </option>
                        ))}
                      </select>
                      <button className="rounded bg-blue-600 px-3 py-1 text-sm" onClick={onForceAddGroup}>+</button>
                      <button className="rounded bg-rose-600 px-3 py-1 text-sm" onClick={onRemoveGroup}>-</button>
                    </div>
                  </div>
                  <textarea className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2" rows={8} placeholder="用户列表（每行一个）" value={form.users} onChange={(e) => setForm((v) => ({ ...v, users: e.target.value }))} />
                  <input className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2" placeholder="账号路径 accounts_path" value={form.accounts_path} onChange={(e) => setForm((v) => ({ ...v, accounts_path: e.target.value }))} />
                  <button className="w-fit rounded-lg bg-blue-600 px-4 py-2 transition hover:bg-blue-500" onClick={onStartTask}>开始拉新</button>
                </div>
              </Card>
            </div>
            <Card title="实时日志面板" className="mt-4">
              <div ref={logRef} className="h-52 overflow-auto rounded-lg bg-black p-3 font-mono text-xs text-emerald-300">
                {logs.map((line, idx) => <div key={`${idx}-${line}`}>{line}</div>)}
              </div>
            </Card>
          </>
        )}

        {tab === "目标群组" && (
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {groups.map((g) => (
              <Card key={g.id} title={g.title}>
                <div className="mb-2 text-sm text-slate-300">{g.username}</div>
                <div className="text-xs text-slate-400">当前人数: {g.members_count}</div>
                <div className="text-xs text-slate-400">总拉人: {g.total_added}</div>
                <div className="text-xs text-slate-400">今日拉人: {g.today_added}</div>
                <div className="text-xs text-slate-400">昨日拉人: {g.yesterday_added}</div>
                <div className="text-xs text-slate-400">昨日退出: {g.yesterday_left}</div>
                <div className="mt-2">
                  <label className="mb-1 block text-xs text-slate-400">单日数量限制</label>
                  <input
                    type="number"
                    min={1}
                    className="w-full rounded border border-slate-700 bg-slate-950 px-2 py-1 text-sm"
                    defaultValue={g.daily_limit || 30}
                    onBlur={(e) => onUpdateDailyLimit(g.id, e.target.value)}
                  />
                </div>
                <div className="mt-2"><Badge status={g.status === "limited" ? "限制" : "正常"} /></div>
              </Card>
            ))}
          </div>
        )}

        {tab === "账号检测" && (
          <div className="grid gap-4 xl:grid-cols-2">
            <Card title="上传 TDATA (zip)">
              <input type="file" accept=".zip" onChange={(e) => setUploadFile(e.target.files?.[0] || null)} />
              <button className="mt-3 rounded-lg bg-blue-600 px-4 py-2 transition hover:bg-blue-500" onClick={onUpload}>上传账号包</button>
            </Card>
            <Card title="账号检测结果">
              <div className="space-y-4 text-sm">
                <div><p className="mb-2 text-emerald-300">可用账号</p>{(accounts.active || []).map((a) => <div key={a.id}>{displayPhone(a)}</div>)}</div>
                <div><p className="mb-2 text-amber-300">当日受限</p>{(accounts.limited || []).map((a) => <div key={a.id}>{displayPhone(a)}</div>)}</div>
                <div>
                  <p className="mb-2 text-rose-300">长期受限</p>
                  {(accounts.banned || []).map((a) => (
                    <div key={a.id} className="mb-1 flex items-center justify-between">
                      <span>{displayPhone(a)}</span>
                      <button className="rounded bg-rose-600 px-2 py-1 text-xs" onClick={() => onDeleteAccount(a.phone)}>删除</button>
                    </div>
                  ))}
                </div>
              </div>
            </Card>
          </div>
        )}

        {tab === "代理监控" && <Card title="代理监控"><p className="text-sm text-slate-300">当前为占位面板。</p></Card>}

        {tab === "用户管理" && (
          <Card title="用户权限管理" right={isAdmin ? <button onClick={onLoadUsers}>刷新用户</button> : null}>
            {!isAdmin ? <p className="text-sm text-slate-400">仅管理员可查看和修改用户权限</p> : (
              <div className="space-y-2">
                {users.map((u) => (
                  <div key={u.id} className="flex items-center justify-between rounded-lg border border-slate-700 bg-slate-900 p-2 text-sm">
                    <div>#{u.id} {u.username} ({u.role})</div>
                    <select className="rounded border border-slate-700 bg-slate-950 px-2 py-1" value={u.role} onChange={(e) => onChangeRole(u.id, e.target.value)}>
                      <option value="user">user</option>
                      <option value="admin">admin</option>
                    </select>
                  </div>
                ))}
              </div>
            )}
          </Card>
        )}
      </main>
    </div>
  );
}
