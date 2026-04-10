const BASE_URL = "http://localhost:8000";

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

export const api = {
  login: (username, password) =>
    request("/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }),
  me: () => request("/auth/me"),
  systemStatus: () => request("/"),
  listGroups: () => request("/groups"),
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
};
