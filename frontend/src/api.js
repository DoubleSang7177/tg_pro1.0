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
  if (!res.ok) throw new Error(data.detail || data.message || "请求失败");
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
    return request("/upload_account", { method: "POST", body: form });
  },
  listAccounts: () => request("/accounts"),
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
