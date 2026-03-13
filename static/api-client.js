async function responseError(response) {
  try {
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      const payload = await response.json();
      return payload.detail || payload.error || JSON.stringify(payload);
    }
    return await response.text();
  } catch (_error) {
    return `HTTP ${response.status}`;
  }
}

export function createApiClient(getToken) {
  function authHeaders(extra = {}) {
    const token = String(getToken?.() || "").trim();
    return token ? { ...extra, "x-train-watch-token": token } : extra;
  }

  async function apiGet(path) {
    const response = await fetch(path, { headers: authHeaders() });
    if (!response.ok) {
      throw new Error(await responseError(response));
    }
    return response.json();
  }

  async function apiJson(method, path, body) {
    const response = await fetch(path, {
      method,
      headers: authHeaders({ "content-type": "application/json" }),
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(await responseError(response));
    }
    return response.json();
  }

  return { apiGet, apiJson };
}
