export const BASE_URL = "http://localhost:8000";

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

  const res = await fetch(`${BASE_URL}${path}`, { ...options, headers });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error(jsonErrorMessage(data));
  }
  return data;
}

/** 采集登录类接口：始终解析 JSON，业务错误用 body.ok / need_password 表示，不抛异常 */
async function scraperPost(path, body) {
  const token = localStorage.getItem("token") || "";
  const res = await fetch(`${BASE_URL}${path}`, {
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
    return fetch(`${BASE_URL}/groups/sync-metadata`, {
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
  stopTask: () => request("/stop-task", { method: "POST" }),
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
      body: JSON.stringify({ usernames: Array.isArray(usernames) ? usernames : [] }),
    }),
  listCopyBots: () => request("/copy/bots"),
  createCopyBot: (payload) =>
    request("/copy/bots", {
      method: "POST",
      body: JSON.stringify({
        api_id: Number(payload.api_id),
        api_hash: String(payload.api_hash || "").trim(),
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
  listCopyTasks: () => request("/copy/tasks"),
  createCopyTask: (payload) =>
    request("/copy/tasks", {
      method: "POST",
      body: JSON.stringify({
        source_channel: String(payload.source_channel || "").trim(),
        target_channel: String(payload.target_channel || "").trim(),
        bot_id: Number(payload.bot_id),
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
  const res = await fetch(`${BASE_URL}/scraper/download/${encodeURIComponent(filename)}`, {
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
  const res = await fetch(`${BASE_URL}/scraper/download/${Number(taskId)}`, {
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
