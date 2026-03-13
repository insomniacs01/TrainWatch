const TOKEN_STORAGE_KEY = "train_watch_token";

export function restoreToken() {
  try {
    const sessionToken = sessionStorage.getItem(TOKEN_STORAGE_KEY);
    if (sessionToken) return sessionToken;
    const legacyToken = localStorage.getItem(TOKEN_STORAGE_KEY) || "";
    if (legacyToken) {
      sessionStorage.setItem(TOKEN_STORAGE_KEY, legacyToken);
      localStorage.removeItem(TOKEN_STORAGE_KEY);
    }
    return legacyToken;
  } catch (_error) {
    return "";
  }
}

export function persistToken(token) {
  try {
    if (token) {
      sessionStorage.setItem(TOKEN_STORAGE_KEY, token);
    } else {
      sessionStorage.removeItem(TOKEN_STORAGE_KEY);
    }
    localStorage.removeItem(TOKEN_STORAGE_KEY);
  } catch (_error) {}
}
