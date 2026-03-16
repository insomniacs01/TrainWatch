import { persistToken } from "./token-store.js";

const MODE_LABELS = {
  personal: "个人模式",
  "personal-token": "令牌模式",
  team: "团队模式",
};

function isAuthError(error) {
  return Number(error?.status || 0) === 401;
}

export function createAuthController({
  state,
  els,
  apiGet,
  apiJson,
  showBanner,
  localizeMessage,
  fetchSnapshot,
  loadSshAliases,
  connectStream,
  disconnectStream,
  getOverlayElements,
} = {}) {
  function currentModeLabel() {
    return MODE_LABELS[state.authConfig?.mode] || "访问模式";
  }

  function updateModeBadge() {
    const mode = state.authConfig?.mode || "personal";
    const modeLabel = currentModeLabel();
    if (els.authModeBadge) {
      els.authModeBadge.textContent = modeLabel;
    }
    if (!els.modeBadge) return;
    if (mode === "personal") {
      els.modeBadge.textContent = "当前模式：个人模式 · 无需登录";
      return;
    }
    if (mode === "team") {
      els.modeBadge.textContent = state.authConfig?.bootstrap_required
        ? "当前模式：团队模式 · 首次创建管理员"
        : "当前模式：团队模式 · 账号登录";
      return;
    }
    els.modeBadge.textContent = "当前模式：令牌模式 · 输入访问令牌";
  }

  async function fetchAuthConfig() {
    const response = await fetch("/api/v1/auth/config");
    if (!response.ok) {
      throw new Error(`Failed to load auth config: HTTP ${response.status}`);
    }
    const payload = await response.json();
    state.authConfig = payload;
    const methods = Array.isArray(payload.login_methods) ? payload.login_methods : [];
    if (payload.bootstrap_required) {
      state.authMode = "password";
    } else if (methods.includes(state.authMode)) {
      state.authMode = state.authMode;
    } else if (payload.user_auth_enabled) {
      state.authMode = "password";
    } else if (payload.shared_token_enabled) {
      state.authMode = "token";
    }
    updateModeBadge();
    return payload;
  }

  function updateTokenButtonVisibility() {
    if (!els.tokenBtn) return;
    updateModeBadge();
    if (state.authConfig?.mode === "personal") {
      els.tokenBtn.classList.remove("hidden");
      els.tokenBtn.textContent = "开启团队";
      return;
    }
    const authRequired = Boolean(state.authConfig?.auth_required);
    els.tokenBtn.classList.toggle("hidden", !authRequired);
    if (!authRequired) return;
    if (state.currentUser?.source === "session") {
      els.tokenBtn.textContent = `账户 · ${state.currentUser.display_name || state.currentUser.username}`;
      return;
    }
    if (state.currentUser?.source === "shared_token") {
      els.tokenBtn.textContent = "令牌访问";
      return;
    }
    if (state.authConfig?.bootstrap_required) {
      els.tokenBtn.textContent = "团队注册";
      return;
    }
    if (state.authConfig?.mode === "team") {
      els.tokenBtn.textContent = "团队登录";
      return;
    }
    els.tokenBtn.textContent = state.token ? "更换令牌" : "输入令牌";
  }

  function setAuthError(message = "") {
    if (!els.authError) return;
    els.authError.textContent = message;
    els.authError.classList.toggle("hidden", !message);
  }

  function setAuthMode(mode) {
    const methods = state.authConfig?.login_methods || [];
    const bootstrapRequired = Boolean(state.authConfig?.bootstrap_required);
    if (!methods.includes(mode) && methods.length) {
      state.authMode = methods[0];
    } else {
      state.authMode = mode;
    }
    if (bootstrapRequired) {
      state.authMode = "password";
    }

    const isToken = state.authMode === "token";
    const isBootstrap = bootstrapRequired && !isToken;
    const isTeamMode = state.authConfig?.mode === "team";
    const canSwitch = !bootstrapRequired && methods.length > 1;

    els.authUsernameRow?.classList.toggle("hidden", isToken);
    els.authDisplayNameRow?.classList.toggle("hidden", !isBootstrap);
    els.authPasswordRow?.classList.toggle("hidden", isToken);
    els.authTokenRow?.classList.toggle("hidden", !isToken);
    els.authSwitchBtn?.classList.toggle("hidden", !canSwitch);
    els.authRegisterBtn?.classList.toggle("hidden", !isBootstrap);
    els.authSubmitBtn?.classList.toggle("hidden", isBootstrap);

    if (els.authSwitchBtn) {
      els.authSwitchBtn.textContent = isToken ? "改用账号密码" : "改用访问令牌";
    }
    if (els.authPasswordInput) {
      els.authPasswordInput.autocomplete = isBootstrap ? "new-password" : "current-password";
    }
    if (els.authTitle) {
      if (isBootstrap) {
        els.authTitle.textContent = "首次使用：创建管理员账号";
      } else if (isToken) {
        els.authTitle.textContent = isTeamMode ? "团队模式：输入访问令牌" : "输入访问令牌";
      } else if (isTeamMode) {
        els.authTitle.textContent = "团队模式登录";
      } else {
        els.authTitle.textContent = "登录 Train Watch";
      }
    }
    if (els.authDescription) {
      if (isBootstrap) {
        els.authDescription.textContent = "当前是团队模式，系统里还没有任何账号。先创建一个管理员账号，创建完成后会自动进入系统。";
      } else if (isToken) {
        els.authDescription.textContent = "当前是令牌模式，请输入部署时配置的访问令牌后进入。";
      } else if (isTeamMode) {
        els.authDescription.textContent = "当前是团队模式，请使用账号和密码登录。首次部署时，先由页面创建第一个管理员账号。";
      } else {
        els.authDescription.textContent = "请输入登录信息继续。";
      }
    }
    if (els.authSubmitBtn) {
      els.authSubmitBtn.textContent = isToken ? "进入系统" : "登录";
    }
    if (els.authRegisterBtn) {
      els.authRegisterBtn.textContent = "创建管理员并进入";
    }
  }

  function syncOverlayState() {
    const overlayElements = [els.authGate].concat(getOverlayElements?.() || []);
    const hasOverlay = overlayElements.some((element) => element && !element.classList.contains("hidden"));
    document.body.classList.toggle("overlay-open", hasOverlay);
  }

  function showAuthGate(message = "") {
    if (!state.authConfig?.auth_required) {
      els.authGate?.classList.add("hidden");
      document.body.classList.remove("overlay-open");
      return;
    }
    updateModeBadge();
    setAuthMode(state.authMode);
    setAuthError(message);
    els.authGate?.classList.remove("hidden");
    document.body.classList.add("overlay-open");
    window.setTimeout(() => {
      if (state.authConfig?.bootstrap_required || state.authMode === "password") {
        els.authUsernameInput?.focus();
      } else {
        els.authTokenInput?.focus();
      }
    }, 0);
  }

  function hideAuthGate() {
    setAuthError("");
    els.authGate?.classList.add("hidden");
    syncOverlayState();
  }

  async function loginWithTokenValue(token) {
    state.token = String(token || "").trim();
    persistToken(state.token);
    await loadSessionState({ silent: true });
    updateTokenButtonVisibility();
    return Boolean(state.token);
  }

  async function enableTeamMode() {
    const payload = await apiJson("POST", "/api/v1/auth/enable-team-mode", {});
    state.authConfig = payload;
    state.authMode = "password";
    updateTokenButtonVisibility();
    return payload;
  }

  async function logoutCurrentSession() {
    if (!state.token) return;
    try {
      await apiJson("POST", "/api/v1/session/logout", {});
    } catch (_error) {}
    state.token = "";
    state.sessionInfo = null;
    state.currentUser = null;
    persistToken("");
    updateTokenButtonVisibility();
    disconnectStream?.();
  }

  async function loadSessionState({ silent = false } = {}) {
    try {
      const payload = await apiGet("/api/v1/session/me");
      state.sessionInfo = payload;
      state.currentUser = payload?.user || null;
      updateTokenButtonVisibility();
      return payload;
    } catch (error) {
      if (!silent && !isAuthError(error)) {
        showBanner(error.message || String(error), "error");
      }
      if (isAuthError(error)) {
        state.currentUser = null;
      }
      state.sessionInfo = state.sessionInfo || { auth_required: isAuthError(error), user_auth_enabled: false };
      updateTokenButtonVisibility();
      throw error;
    }
  }

  async function withAuthRecovery(task) {
    try {
      return await task();
    } catch (error) {
      if (!isAuthError(error)) {
        throw error;
      }
      showAuthGate(error.message || "Please sign in to continue.");
      throw error;
    }
  }

  async function enterApp() {
    hideAuthGate();
    await fetchSnapshot?.();
    if (loadSshAliases) {
      await loadSshAliases().catch(() => {});
    }
    connectStream?.();
  }

  async function handleBootstrapAdmin() {
    const username = String(els.authUsernameInput?.value || "").trim();
    const password = String(els.authPasswordInput?.value || "");
    const displayName = String(els.authDisplayNameInput?.value || "").trim();
    if (!username || !password) {
      setAuthError("请先填写管理员账号和密码。");
      return;
    }
    const response = await fetch("/api/v1/session/bootstrap-admin", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username, password, display_name: displayName }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      setAuthError(localizeMessage(payload.detail || payload.error || `HTTP ${response.status}`));
      return;
    }
    state.token = String(payload.token || "").trim();
    persistToken(state.token);
    await fetchAuthConfig();
    await loadSessionState({ silent: true }).catch(() => {});
    updateTokenButtonVisibility();
    await enterApp();
  }

  async function handleAuthSubmit(event) {
    event.preventDefault();
    setAuthError("");
    try {
      if (state.authConfig?.bootstrap_required && state.authMode !== "token") {
        await handleBootstrapAdmin();
        return;
      }
      if (state.authMode === "password") {
        const username = String(els.authUsernameInput?.value || "").trim();
        const password = String(els.authPasswordInput?.value || "");
        if (!username || !password) {
          setAuthError("Username and password are required.");
          return;
        }
        const response = await fetch("/api/v1/session/login", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ username, password }),
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          setAuthError(localizeMessage(payload.detail || payload.error || `HTTP ${response.status}`));
          return;
        }
        state.token = String(payload.token || "").trim();
        persistToken(state.token);
      } else {
        const token = String(els.authTokenInput?.value || "").trim();
        if (!token) {
          setAuthError("Access token is required.");
          return;
        }
        await loginWithTokenValue(token);
      }
      await loadSessionState({ silent: true }).catch(() => {});
      updateTokenButtonVisibility();
      await enterApp();
    } catch (error) {
      setAuthError(error.message || String(error));
    }
  }

  return {
    enableTeamMode,
    enterApp,
    fetchAuthConfig,
    handleAuthSubmit,
    handleBootstrapAdmin,
    hideAuthGate,
    loadSessionState,
    logoutCurrentSession,
    setAuthError,
    setAuthMode,
    showAuthGate,
    syncOverlayState,
    updateTokenButtonVisibility,
    withAuthRecovery,
  };
}
