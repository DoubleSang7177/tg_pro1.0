export const BASE_URL = "http://localhost:8000";

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
    let detail = data?.detail;
    if (Array.isArray(detail)) {
      detail = detail
        .map((item) => item?.msg || JSON.stringify(item))
        .join("; ");
    } else if (detail && typeof detail === "object") {
      detail = detail.msg || JSON.stringify(detail);
    }
    throw new Error(detail || data?.message || "请求失败");
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
    let detail = data?.detail;
    if (Array.isArray(detail)) {
      detail = detail.map((item) => item?.msg || JSON.stringify(item)).join("; ");
    } else if (detail && typeof detail === "object") {
      detail = detail.msg || JSON.stringify(detail);
    }
    return { ok: false, error: detail || data?.message || `HTTP ${res.status}` };
  }
  return data;
}

export const api = {
  login: (username, password) =>
    request("/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
  me: () => request("/auth/me"),
  systemStatus: () => request("/"),
  listGroups: () => request("/groups"),
  syncGroupMetadata: (payload = {}) =>
    request("/groups/sync-metadata", {
      method: "POST",
      body: JSON.stringify({ force: Boolean(payload.force) }),
    }),
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
  startInteractionTask: (payload) =>
    request("/interaction/tasks", {
      method: "POST",
      body: JSON.stringify({
        groups: payload.groups || [],
        scan_limit: Number(payload.scan_limit ?? 300),
      }),
    }),
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
