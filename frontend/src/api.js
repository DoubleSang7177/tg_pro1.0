const envApi = import.meta.env.VITE_API_BASE_URL;
/** 生产默认直连 8000；开发默认走同源 /api（由 vite 代理），避免 192.168.x.x:5173 访问时跨域被拒 */
export const BASE_URL =
  typeof envApi === "string" && envApi.trim() !== ""
    ? envApi.trim().replace(/\/$/, "")
    : import.meta.env.DEV
      ? "/api"
      : "http://localhost:8000";

/** 账号状态 WebSocket（与 BASE_URL 同源；token 走 query） */
export function getWebSocketUrl(token) {
  const t = encodeURIComponent(token || "");
  if (typeof window === "undefined") return "";
  if (BASE_URL.startsWith("/")) {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const path = `${BASE_URL.replace(/\/$/, "")}/ws?token=${t}`;
    return `${proto}//${window.location.host}${path}`;
  }
  const u = BASE_URL.replace(/^http/, "ws").replace(/\/$/, "");
  return `${u}/ws?token=${t}`;
}

function jsonErrorMessage(data) {
  let detail = data?.detail;
  if (Array.isArray(detail)) {
    detail = detail
      .map((item) => item?.msg || JSON.stringify(item))
      .join("; ");
  } else if (detail && typeof detail === "object") {
    detail = detail.msg || JSON.stringify(detail);
  }
  return detail || data?.message || "请求失败";
}

async function fetchApi(path, init) {
  const url = `${BASE_URL}${path}`;
  try {
    return await fetch(url, init);
  } catch (e) {
    if (e?.name === "AbortError") throw e;
    const isNet =
      e instanceof TypeError ||
      (typeof e?.message === "string" &&
        (e.message === "Failed to fetch" || e.message.includes("NetworkError")));
    if (isNet) {
      throw new Error(
        `无法连接后端（${BASE_URL}）。常见原因：1) uvicorn 未在 8000 端口运行；2) 用局域网 IP 打开页面却直连了错误的 API 地址——开发环境请使用 npm run dev（走 /api 代理），或在 .env 设置 VITE_API_BASE_URL=http://本机IP:8000。原始错误：${e?.message || e}`,
      );
    }
    throw e;
  }
}

async function request(path, options = {}) {
  const token = localStorage.getItem("token") || "";
  const headers = {
    ...(options.headers || {}),
  };
  if (!(options.body instanceof FormData)) {
    headers["Content-Type"] = headers["Content-Type"] || "application/json";
  }
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }

  const res = await fetchApi(path, { ...options, headers });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(jsonErrorMessage(data));
  }
  return data;
}

/** 采集登录类接口：始终解析 JSON，业务错误用 body.ok / need_password 表示，不抛异常 */
async function scraperPost(path, body) {
  const token = localStorage.getItem("token") || "";
  const res = await fetchApi(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    return { ok: false, error: jsonErrorMessage(data) || `HTTP ${res.status}` };
  }
  return data;
}

export const api = {
  login: (username, password) =>
    request("/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
  register: (username, password) =>
    request("/auth/register", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
  updateProfile: (username) =>
    request("/auth/profile", {
      method: "PATCH",
      body: JSON.stringify({ username }),
    }),
  changePassword: (current_password, new_password) =>
    request("/auth/password", {
      method: "POST",
      body: JSON.stringify({ current_password, new_password }),
    }),
  uploadAvatar: (file) => {
    const form = new FormData();
    form.append("file", file);
    return request("/auth/avatar", { method: "POST", body: form });
  },
  logout: () => request("/auth/logout", { method: "POST", body: "{}" }),
  me: () => request("/auth/me"),
  systemStatus: () => request("/"),
  listGroups: () => request("/groups"),
  /** Telegram 群组元数据同步；独立超时，避免阻塞整页刷新与登录后的数据加载 */
  syncGroupMetadata: (payload = {}) => {
    const force = Boolean(payload.force);
    const timeoutMs =
      typeof payload.timeoutMs === "number" && payload.timeoutMs > 0
        ? payload.timeoutMs
        : force
          ? 180_000
          : 120_000;
    const token = localStorage.getItem("token") || "";
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    return fetchApi("/groups/sync-metadata", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify({ force }),
      signal: ctrl.signal,
    })
      .finally(() => clearTimeout(timer))
      .then(async (res) => {
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          throw new Error(jsonErrorMessage(data));
        }
        return data;
      });
  },
  startGroupSync: (payload = {}) =>
    request("/groups/sync", {
      method: "POST",
      body: JSON.stringify({ force: Boolean(payload.force) }),
    }),
  groupSyncJobStatus: (jobId) => request(`/groups/sync/${encodeURIComponent(jobId)}`),
  updateGroupLimit: (groupId, dailyLimit) =>
    request(`/groups/${groupId}/limit`, {
      method: "PUT",
      body: JSON.stringify({ daily_limit: Number(dailyLimit) }),
    }),
  listTasks: () => request("/tasks"),
  startTask: (payload) =>
    request("/start_task", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  taskJobStatus: (jobId) => request(`/start_task/status/${encodeURIComponent(jobId)}`),
  /** 群组互动等：全局停止信号 */
  stopTask: () => request("/stop-task", { method: "POST" }),
  /** 用户增长：按 job_id 停止对应 TaskRunner */
  stopGrowthTask: (jobId) =>
    request(`/tasks/${encodeURIComponent(jobId)}/stop`, { method: "POST" }),
  uploadAccount: (file) => {
    const form = new FormData();
    form.append("file", file);
    return request("/accounts/upload", { method: "POST", body: form });
  },
  listAccountPaths: () => request("/settings/account-paths"),
  addAccountPath: (path) =>
    request("/settings/account-paths", {
      method: "POST",
      body: JSON.stringify({ path }),
    }),
  deleteAccountPath: (id) =>
    request("/settings/account-paths", {
      method: "DELETE",
      body: JSON.stringify({ id }),
    }),
  listAccounts: () => request("/accounts"),
  listProxies: () => request("/proxy"),
  listProxyPool: () => request("/proxy/pool"),
  dedupeProxyPool: () =>
    request("/proxy/pool/dedupe", {
      method: "POST",
    }),
  startProxyPoolCheck: () =>
    request("/proxy/pool/check", {
      method: "POST",
      body: JSON.stringify({}),
    }),
  getProxyCheckJob: (jobId) =>
    request(`/proxy/pool/check-job/${encodeURIComponent(jobId)}`),
  cancelProxyPoolCheck: (jobId) =>
    request(`/proxy/pool/check/cancel/${encodeURIComponent(jobId)}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),
  stopProxyPoolCheck: (jobId) =>
    request(`/proxy/pool/check/stop/${encodeURIComponent(jobId)}`, {
      method: "POST",
      body: JSON.stringify({}),
    }),
  matchProxies: (body, opts = {}) =>
    request("/proxy/match", {
      method: "POST",
      body: JSON.stringify({
        match_unbound: Boolean(body?.match_unbound),
        match_dead_proxy: Boolean(body?.match_dead_proxy),
      }),
      ...opts,
    }),
  uploadProxyFile: (file) => {
    const form = new FormData();
    form.append("file", file);
    return request("/proxy/upload", { method: "POST", body: form });
  },
  markProxyDead: (proxyId) =>
    request(`/proxy/${proxyId}/mark_dead`, {
      method: "POST",
    }),
  unbindProxy: (proxyId) =>
    request(`/proxy/${proxyId}/unbind`, {
      method: "POST",
    }),
  deleteAccount: (phone) =>
    request(`/accounts/${encodeURIComponent(phone)}`, {
      method: "DELETE",
    }),
  deleteAccountById: (accountId) =>
    request(`/accounts/id/${Number(accountId)}`, {
      method: "DELETE",
    }),
  listUsers: () => request("/users"),
  updateUserRole: (userId, role) =>
    request(`/users/${userId}/role`, {
      method: "PUT",
      body: JSON.stringify({ role }),
    }),
  runScraper: (payload) =>
    request("/scraper/run", {
      method: "POST",
      body: JSON.stringify({
        group_id: String(payload.group_id || "").trim(),
        days: Number(payload.days),
        max_messages: Number(payload.max_messages),
      }),
    }),
  getScraperAccount: () => request("/scraper/account"),
  sendScraperCode: (phone) => scraperPost("/scraper/send_code", { phone: String(phone || "").trim() }),
  loginScraperAccount: (payload) => {
    const body = {
      phone: String(payload.phone || "").trim(),
      code: String(payload.code ?? "").trim(),
      phone_code_hash: String(payload.phone_code_hash ?? "").trim(),
    };
    if (payload.password != null && String(payload.password).trim() !== "") {
      body.password = String(payload.password).trim();
    }
    return scraperPost("/scraper/login", body);
  },
  listScraperTasks: () => request("/scraper/tasks"),
  listInteractionTasks: () => request("/interaction/tasks"),
  listInteractionTargetGroups: () => request("/interaction/target-groups"),
  createInteractionTargetGroups: (payload) =>
    request("/interaction/target-groups", {
      method: "POST",
      body: JSON.stringify({
        usernames: Array.isArray(payload?.usernames) ? payload.usernames : [],
        raw_input: String(payload?.raw_input || "").trim(),
        title: String(payload?.title || "").trim() || null,
        titles: Array.isArray(payload?.titles) ? payload.titles : [],
        remark: String(payload?.remark || "").trim() || null,
      }),
    }),
  deleteInteractionTargetGroups: (usernames) =>
    request("/interaction/target-groups", {
      method: "DELETE",
      body: JSON.stringify({
        usernames: Array.isArray(usernames) ? usernames : [],
      }),
    }),
  updateInteractionTargetGroupRemark: (groupId, remark) =>
    request(`/interaction/target-groups/${Number(groupId)}/remark`, {
      method: "PATCH",
      body: JSON.stringify({
        remark: String(remark || ""),
      }),
    }),
  updateInteractionTargetGroup: (groupId, payload) =>
    request(`/interaction/target-groups/${Number(groupId)}`, {
      method: "PATCH",
      body: JSON.stringify({
        username: String(payload?.username || "").trim(),
        title: String(payload?.title || "").trim(),
        remark: String(payload?.remark || ""),
      }),
    }),
  interactionLive: (jobId) => request(`/interaction/live/${encodeURIComponent(jobId)}`),
  startInteractionTask: (payload) =>
    request("/interaction/tasks", {
      method: "POST",
      body: JSON.stringify({
        groups: payload.groups || [],
        scan_limit: Number(payload.scan_limit ?? 300),
        valid_only: Boolean(payload.valid_only),
      }),
    }),
  registerInteractionTargetGroups: (usernames) =>
    request("/interaction/target-groups/register", {
      method: "POST",
      body: JSON.stringify({
        usernames: Array.isArray(usernames) ? usernames : [],
        raw_input: "",
        title: null,
        remark: null,
      }),
    }),
  listCopyBots: () => request("/copy/bots"),
  createCopyBot: (payload) =>
    request("/copy/bots", {
      method: "POST",
      body: JSON.stringify({
        bot_token: String(payload.bot_token || "").trim(),
      }),
    }),
  deleteCopyBot: (botId) => request(`/copy/bots/${Number(botId)}`, { method: "DELETE" }),
  uploadCopyBotSession: (botId, file) => {
    const form = new FormData();
    form.append("file", file);
    return request(`/copy/bots/${Number(botId)}/session`, { method: "POST", body: form });
  },
  resetCopyBot: (botId) => request(`/copy/bots/${Number(botId)}/reset`, { method: "POST" }),
  listCopyListeners: () => request("/copy/listeners"),
  sendCopyListenerCode: (payload) =>
    request("/copy/listeners/send_code", {
      method: "POST",
      body: JSON.stringify({
        phone: String(payload.phone || "").trim(),
      }),
    }),
  loginCopyListener: (payload) =>
    request("/copy/listeners/login", {
      method: "POST",
      body: JSON.stringify({
        phone: String(payload.phone || "").trim(),
        code: String(payload.code || "").trim(),
        phone_code_hash: String(payload.phone_code_hash || "").trim() || null,
        password: payload.password ? String(payload.password).trim() : null,
      }),
    }),
  enableCopyListener: (id) => request(`/copy/listeners/${Number(id)}/enable`, { method: "POST" }),
  disableCopyListener: (id) => request(`/copy/listeners/${Number(id)}/disable`, { method: "POST" }),
  deleteCopyListener: (id) => request(`/copy/listeners/${Number(id)}`, { method: "DELETE" }),
  listCopyTasks: () => request("/copy/tasks"),
  createCopyTask: (payload) =>
    request("/copy/tasks", {
      method: "POST",
      body: JSON.stringify({
        source_channel: String(payload.source_channel || "").trim(),
        target_channel: String(payload.target_channel || "").trim(),
        bot_id: Number(payload.bot_id),
        listener_id: payload.listener_id ? Number(payload.listener_id) : null,
      }),
    }),
  startCopyTask: (taskId) => request(`/copy/tasks/${Number(taskId)}/start`, { method: "POST" }),
  pauseCopyTask: (taskId) => request(`/copy/tasks/${Number(taskId)}/pause`, { method: "POST" }),
  deleteCopyTask: (taskId) => request(`/copy/tasks/${Number(taskId)}`, { method: "DELETE" }),
  copyLogs: (limit = 200) => request(`/copy/logs?limit=${Number(limit)}`),
};

/** 携带 Token 下载采集结果 txt（浏览器保存文件，兼容旧版按文件名） */
export async function downloadScraperFile(filename) {
  const token = localStorage.getItem("token") || "";
  const res = await fetchApi(`/scraper/download/${encodeURIComponent(filename)}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    let detail;
    try {
      const data = await res.json();
      detail = data?.detail;
      if (Array.isArray(detail)) {
        detail = detail.map((item) => item?.msg || JSON.stringify(item)).join("; ");
      }
    } catch {
      detail = null;
    }
    throw new Error(detail || "下载失败");
  }
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

/** 按任务 ID 下载（会递增服务端 download_count） */
export async function downloadScraperTaskById(taskId) {
  const token = localStorage.getItem("token") || "";
  const res = await fetchApi(`/scraper/download/${Number(taskId)}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    let detail;
    try {
      const data = await res.json();
      detail = data?.detail;
      if (Array.isArray(detail)) {
        detail = detail.map((item) => item?.msg || JSON.stringify(item)).join("; ");
      } else if (detail && typeof detail === "object") {
        detail = detail.msg || JSON.stringify(detail);
      }
    } catch {
      detail = null;
    }
    throw new Error(detail || "下载失败");
  }
  const blob = await res.blob();
  const fname = `scraper_${taskId}.txt`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fname;
  a.click();
  URL.revokeObjectURL(url);
}
