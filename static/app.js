import { createApiClient } from "./api-client.js";
import { createConnectionsController } from "./connections-controller.js";
import { drawChart } from "./charts.js";
import { applyFoldState } from "./fold-state.js";
import { createJobsController } from "./jobs-controller.js";
import { renderExternalJobsPanel, renderJobsPanel } from "./jobs-view.js";
import { renderEventsList, renderNodesList, renderSummaryCards } from "./nodes-view.js";
import { renderRunDetail } from "./run-detail-view.js";
import { createSnapshotStream } from "./stream-client.js";
import {
  aliasDescription,
  alertMessage,
  localizeMessage,
  statusLabel,
} from "./formatters.js";
import { persistToken, restoreToken } from "./token-store.js";

const state = {
  snapshot: null,
  token: restoreToken(),
  authConfig: null,
  authMode: "password",
  sessionInfo: null,
  currentUser: null,
  recentEvents: [],
  currentAlerts: [],
  alertFeed: [],
  unreadAlerts: 0,
  sshAliases: [],
  connections: [],
  jobs: [],
  jobsSummary: {},
  externalJobs: [],
  externalJobsSummary: {},
  connectSubmitting: false,
  jobSubmitting: false,
  refreshing: false,
  cancelingJobIds: new Set(),
  removingConnectionIds: new Set(),
  detailRequestId: 0,
};

const els = {
  subtitle: document.getElementById("subtitle"),
  summaryGrid: document.getElementById("summaryGrid"),
  jumpSelect: document.getElementById("jumpSelect"),
  nodeList: document.getElementById("nodeList"),
  eventsList: document.getElementById("eventsList"),
  banner: document.getElementById("banner"),
  connectBtn: document.getElementById("connectBtn"),
  connectDrawer: document.getElementById("connectDrawer"),
  connectForm: document.getElementById("connectForm"),
  closeConnectBtn: document.getElementById("closeConnectBtn"),
  refreshBtn: document.getElementById("refreshBtn"),
  tokenBtn: document.getElementById("tokenBtn"),
  authGate: document.getElementById("authGate"),
  authForm: document.getElementById("authForm"),
  authModeBadge: document.getElementById("authModeBadge"),
  authTitle: document.getElementById("authTitle"),
  authDescription: document.getElementById("authDescription"),
  authUsernameRow: document.getElementById("authUsernameRow"),
  authDisplayNameRow: document.getElementById("authDisplayNameRow"),
  authPasswordRow: document.getElementById("authPasswordRow"),
  authTokenRow: document.getElementById("authTokenRow"),
  authUsernameInput: document.getElementById("authUsernameInput"),
  authDisplayNameInput: document.getElementById("authDisplayNameInput"),
  authPasswordInput: document.getElementById("authPasswordInput"),
  authTokenInput: document.getElementById("authTokenInput"),
  authSubmitBtn: document.getElementById("authSubmitBtn"),
  authSwitchBtn: document.getElementById("authSwitchBtn"),
  authRegisterBtn: document.getElementById("authRegisterBtn"),
  authError: document.getElementById("authError"),
  alertsBtn: document.getElementById("alertsBtn"),
  alertBadge: document.getElementById("alertBadge"),
  modeBadge: document.getElementById("modeBadge"),
  detailDrawer: document.getElementById("detailDrawer"),
  drawerTitle: document.getElementById("drawerTitle"),
  drawerEyebrow: document.getElementById("drawerEyebrow"),
  drawerMeta: document.getElementById("drawerMeta"),
  drawerProcesses: document.getElementById("drawerProcesses"),
  drawerLog: document.getElementById("drawerLog"),
  closeDrawerBtn: document.getElementById("closeDrawerBtn"),
  lossChart: document.getElementById("lossChart"),
  gpuChart: document.getElementById("gpuChart"),
  memChart: document.getElementById("memChart"),
  tempChart: document.getElementById("tempChart"),
  aliasSelect: document.getElementById("aliasSelect"),
  aliasMeta: document.getElementById("aliasMeta"),
  applyAliasBtn: document.getElementById("applyAliasBtn"),
  refreshAliasesBtn: document.getElementById("refreshAliasesBtn"),
  submitConnectBtn: document.getElementById("submitConnectBtn"),
  jobForm: document.getElementById("jobForm"),
  jobNodeSelect: document.getElementById("jobNodeSelect"),
  submitJobBtn: document.getElementById("submitJobBtn"),
  jobList: document.getElementById("jobList"),
  jobSummary: document.getElementById("jobSummary"),
  externalJobList: document.getElementById("externalJobList"),
  externalJobSummary: document.getElementById("externalJobSummary"),
  emptyConnectBtn: null,
};

const { apiGet, apiJson } = createApiClient(() => state.token);

const MODE_LABELS = {
  personal: "个人模式",
  "personal-token": "令牌模式",
  team: "团队模式",
};

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

const stream = createSnapshotStream({
  getToken: () => state.token,
  onSnapshot: (payload) => {
    syncAlertFeed(payload.snapshot, payload.events || []);
    renderSnapshot(payload.snapshot);
    handleIncomingEvents(payload.events || []);
    loadConnections().catch(() => {});
    loadJobs().catch(() => {});
  },
  onError: (message, meta) => {
    showBanner(message, "error");
    if (meta?.kind === "auth") {
      showAuthGate(message || "Please sign in again.");
    }
  },
});

const DETAIL_CHARTS = [
  { canvas: () => els.lossChart, color: "#38bdf8", label: "损失" },
  { canvas: () => els.gpuChart, color: "#22c55e", label: "GPU 利用率" },
  { canvas: () => els.memChart, color: "#f59e0b", label: "显存 MB" },
  { canvas: () => els.tempChart, color: "#ef4444", label: "温度 °C" },
];

function showBanner(text, tone = "error", { kind = "general" } = {}) {
  els.banner.textContent = text;
  els.banner.dataset.kind = kind;
  els.banner.classList.remove("hidden");
  els.banner.style.borderColor = tone === "info" ? "rgba(56, 189, 248, 0.35)" : "rgba(239, 68, 68, 0.35)";
  els.banner.style.background = tone === "info" ? "rgba(12, 74, 110, 0.35)" : "rgba(127, 29, 29, 0.35)";
}
function clearBanner(kind = null) {
  if (kind && els.banner.dataset.kind !== kind) return;
  els.banner.classList.add("hidden");
  els.banner.dataset.kind = "";
}

function updateTokenButtonVisibility() {
  if (!els.tokenBtn) return;
  const authRequired = Boolean(state.authConfig?.auth_required);
  els.tokenBtn.classList.toggle("hidden", !authRequired);
  if (!authRequired) return;
  updateModeBadge();
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

function renderJumpOptions(snapshot) {
  if (!els.jumpSelect) return;
  const previousValue = els.jumpSelect.value;
  const options = [
    { value: "summaryGrid", label: "顶部概览" },
    { value: "eventsPanel", label: "告警" },
    { value: "jobsPanel", label: "GPU 队列" },
  ];
  (snapshot?.nodes || []).forEach((node) => {
    const runs = Array.isArray(node.runs) ? node.runs.length : 0;
    const gpus = Array.isArray(node.gpus) ? node.gpus.length : 0;
    options.push({
      value: `node-${node.id}`,
      label: `${node.label} · ${statusLabel(node.status)} · ${runs} 个任务 · ${gpus} 张 GPU`,
    });
  });

  const fragment = document.createDocumentFragment();
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "选择一个区域或节点";
  fragment.appendChild(placeholder);

  options.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    fragment.appendChild(option);
  });

  els.jumpSelect.replaceChildren(fragment);
  const nextValue = options.some((item) => item.value === previousValue) ? previousValue : "";
  els.jumpSelect.value = nextValue;
}

function expandFoldAncestors(element) {
  let current = element;
  while (current) {
    if (current.tagName === "DETAILS" && !current.open) {
      current.open = true;
    }
    current = current.parentElement;
  }
}

function jumpToTarget(targetId) {
  const target = document.getElementById(targetId);
  if (!target) return;
  expandFoldAncestors(target);
  target.scrollIntoView({ behavior: "smooth", block: "start" });
  target.classList.add("jump-target-flash");
  window.setTimeout(() => {
    target.classList.remove("jump-target-flash");
  }, 1400);
}

function resetJumpSelection() {
  if (!els.jumpSelect) return;
  window.requestAnimationFrame(() => {
    els.jumpSelect.value = "";
  });
}

function isAuthError(error) {
  return Number(error?.status || 0) === 401;
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
    if (state.authConfig?.bootstrap_required) {
      els.authUsernameInput?.focus();
    } else if (state.authMode === "password") {
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

function syncOverlayState() {
  const hasOverlay = [els.authGate, els.detailDrawer].some(
    (element) => element && !element.classList.contains("hidden")
  );
  document.body.classList.toggle("overlay-open", hasOverlay);
}

function promptForToken(message = "设置 Train Watch 令牌") {
  const nextToken = window.prompt(message, state.token || "");
  if (nextToken === null) return false;
  state.token = nextToken.trim();
  persistToken(state.token);
  updateTokenButtonVisibility();
  stream.connect();
  return Boolean(state.token);
}

async function loginWithPassword(message = "Login to Train Watch") {
  const username = window.prompt(`${message}\nUsername`, state.currentUser?.username || "");
  if (username === null) return false;
  const password = window.prompt("Password", "");
  if (password === null) return false;
  const response = await fetch("/api/v1/session/login", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username: username.trim(), password }),
  });
  let payload = {};
  try {
    payload = await response.json();
  } catch (_error) {}
  if (!response.ok) {
    throw new Error(localizeMessage(payload.detail || payload.error || `HTTP ${response.status}`));
  }
  state.token = String(payload.token || "").trim();
  persistToken(state.token);
  await loadSessionState({ silent: true });
  updateTokenButtonVisibility();
  stream.connect();
  return Boolean(state.token);
}

async function loginWithTokenValue(token) {
  state.token = String(token || "").trim();
  persistToken(state.token);
  await loadSessionState({ silent: true });
  updateTokenButtonVisibility();
  return Boolean(state.token);
}

async function logoutCurrentSession() {
  if (!state.token) return;
  try {
    await apiJson("POST", "/api/v1/session/logout", {});
  } catch (_error) {
    // ignore logout failures and clear locally
  }
  state.token = "";
  state.sessionInfo = null;
  state.currentUser = null;
  persistToken("");
  updateTokenButtonVisibility();
  stream.disconnect();
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
  await fetchSnapshot();
  await loadSshAliases().catch(() => {});
  stream.connect();
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

const jobsController = createJobsController({
  state,
  els,
  apiGet,
  apiJson,
  showBanner,
  renderJobsPanel,
  renderExternalJobsPanel,
  withAuthRecovery,
});

const connectionsController = createConnectionsController({
  state,
  els,
  apiGet,
  apiJson,
  showBanner,
  closeConnectDrawer,
  onConnectionsChanged: jobsController.loadConnections,
  withAuthRecovery,
  refreshAll: async () => {
    await refreshNow();
    await jobsController.loadConnections();
    await jobsController.loadJobs();
  },
});

const {
  cancelJob,
  loadConnections,
  loadJobs,
  submitJob,
} = jobsController;

const {
  applySelectedAlias,
  loadSshAliases,
  removeConnection,
  submitConnection,
} = connectionsController;

function updateAlertBadge() {
  const badgeCount = state.currentAlerts.length > 0 ? state.currentAlerts.length : state.unreadAlerts;
  els.alertBadge.textContent = String(badgeCount);
  els.alertBadge.classList.toggle("hidden", badgeCount <= 0);
}

function playAlertTone() {
  try {
    const audioContext = new (window.AudioContext || window.webkitAudioContext)();
    const oscillator = audioContext.createOscillator();
    const gain = audioContext.createGain();
    oscillator.connect(gain);
    gain.connect(audioContext.destination);
    oscillator.type = "sine";
    oscillator.frequency.value = 880;
    gain.gain.value = 0.03;
    oscillator.start();
    oscillator.stop(audioContext.currentTime + 0.15);
  } catch (_error) {}
}

function buildCurrentAlerts(snapshot) {
  if (!snapshot) return [];
  if (Array.isArray(snapshot.current_alerts) && snapshot.current_alerts.length) {
    return snapshot.current_alerts.slice(0, 20);
  }
  const items = [];
  const sortOrder = { failed: 0, offline: 1, stalled: 2, degraded: 3 };
  (snapshot.nodes || []).forEach((node) => {
    const runs = Array.isArray(node.runs) ? node.runs : [];
    const runAlerts = runs.filter((run) => ["failed", "stalled"].includes(run.status));
    runAlerts.forEach((run) => {
      items.push({
        kind: "current_run_alert",
        is_current: true,
        node_id: node.id,
        node_label: node.label,
        run_id: run.id,
        run_label: run.label,
        status: run.status,
        at: run.last_update_at || node.collected_at || snapshot.generated_at,
        message: `${node.label} / ${run.label}: ${statusLabel(run.status)}${run.error ? ` · ${localizeMessage(run.error)}` : ""}`,
      });
    });

    const needsNodeAlert = node.status === "offline" || (node.status === "degraded" && !runAlerts.length && node.error);
    if (!needsNodeAlert) return;
    items.push({
      kind: "current_node_alert",
      is_current: true,
      node_id: node.id,
      node_label: node.label,
      run_id: "",
      run_label: "",
      status: node.status,
      at: node.collected_at || snapshot.generated_at,
      message: `${node.label}: ${statusLabel(node.status)}${node.error ? ` · ${localizeMessage(node.error)}` : ""}`,
    });
  });

  return items
    .sort((left, right) => {
      const leftPriority = sortOrder[left.status] ?? 99;
      const rightPriority = sortOrder[right.status] ?? 99;
      if (leftPriority !== rightPriority) return leftPriority - rightPriority;
      return String(right.at || "").localeCompare(String(left.at || ""));
    })
    .slice(0, 20);
}

function alertIdentity(item) {
  if (!item) return "";
  if (item.run_id) return `run:${item.node_id}:${item.run_id}:${item.status}`;
  return `node:${item.node_id}:${item.status}`;
}

function syncAlertFeed(snapshot, fallbackEvents = []) {
  const recentEvents = Array.isArray(snapshot?.recent_events) ? snapshot.recent_events : [];
  state.recentEvents = recentEvents.length ? recentEvents : (fallbackEvents.length ? fallbackEvents.slice(0, 20) : []);
  state.currentAlerts = buildCurrentAlerts(snapshot);

  const merged = [];
  const seen = new Set();
  state.currentAlerts.concat(state.recentEvents).forEach((item) => {
    const key = alertIdentity(item);
    if (!key || seen.has(key)) return;
    seen.add(key);
    merged.push(item);
  });
  state.alertFeed = merged.slice(0, 20);
}

function renderAlertFeed() {
  renderEventsList(els.eventsList, state.alertFeed);
  updateAlertBadge();
}

function handleIncomingEvents(events) {
  if (!events || !events.length) {
    updateAlertBadge();
    return;
  }
  const alerting = events.filter((event) => ["completed", "failed", "stalled"].includes(event.status));
  if (!alerting.length) {
    updateAlertBadge();
    return;
  }
  state.unreadAlerts += alerting.length;
  updateAlertBadge();
  showBanner(alertMessage(alerting[0]), alerting[0].status === "completed" ? "info" : "error", { kind: "event" });
  playAlertTone();
}

function renderSnapshot(snapshot) {
  state.snapshot = snapshot;
  updateTokenButtonVisibility();
  renderJumpOptions(snapshot);
  const userLabel = state.currentUser ? ` · ${state.currentUser.display_name || state.currentUser.username}` : "";
  els.subtitle.textContent = `最近刷新：${snapshot.generated_at}${userLabel}`;
  renderSummaryCards(els.summaryGrid, snapshot);
  renderNodesList({
    nodeListEl: els.nodeList,
    snapshot,
    onOpenRunDetail: openRunDetail,
    onRemoveConnection: removeConnection,
    onOpenConnect: openConnectDrawer,
    removingConnectionIds: state.removingConnectionIds,
  });
  applyFoldState(document);
  renderAlertFeed();
  if (!snapshot.nodes.length) {
    showBanner("还没有连接真实 SSH 机器，点击“连接 SSH”开始。", "info", { kind: "empty-state" });
  } else {
    clearBanner("empty-state");
  }
}

async function fetchSnapshot() {
  const snapshot = await withAuthRecovery(() => apiGet("/api/v1/snapshot"));
  syncAlertFeed(snapshot);
  renderSnapshot(snapshot);
  await loadConnections();
  await loadJobs();
}

function setRefreshing(refreshing) {
  state.refreshing = refreshing;
  if (els.refreshBtn) {
    els.refreshBtn.disabled = refreshing;
    els.refreshBtn.textContent = refreshing ? "\u5237\u65b0\u4e2d..." : "\u7acb\u5373\u5237\u65b0";
  }
}

async function refreshNow() {
  if (state.refreshing) return;
  setRefreshing(true);
  try {
    const snapshot = await withAuthRecovery(() => apiJson("POST", "/api/v1/refresh"));
    syncAlertFeed(snapshot);
    renderSnapshot(snapshot);
    await loadConnections();
    await loadJobs();
  } finally {
    setRefreshing(false);
  }
}

function resetDetailCharts() {
  DETAIL_CHARTS.forEach(({ canvas, color, label }) => {
    drawChart(canvas(), [], color, label);
  });
}

async function openRunDetail(nodeId, runId) {
  const node = (state.snapshot?.nodes || []).find((item) => item.id === nodeId);
  const run = (node?.runs || []).find((item) => item.id === runId);
  if (!node || !run) return;
  const requestId = state.detailRequestId + 1;
  state.detailRequestId = requestId;
  renderRunDetail({
    eyebrowEl: els.drawerEyebrow,
    titleEl: els.drawerTitle,
    metaEl: els.drawerMeta,
    logEl: els.drawerLog,
    processesEl: els.drawerProcesses,
    drawerEl: els.detailDrawer,
    node,
    run,
  });
  syncOverlayState();
  resetDetailCharts();

  const end = new Date();
  const start = new Date(end.getTime() - 6 * 3600 * 1000).toISOString();
  try {
    const queries = await withAuthRecovery(() => Promise.all([
      apiGet(`/api/v1/history?node_id=${encodeURIComponent(nodeId)}&run_id=${encodeURIComponent(runId)}&metric=loss&from=${encodeURIComponent(start)}`),
      apiGet(`/api/v1/history?node_id=${encodeURIComponent(nodeId)}&metric=gpu_utilization_avg&from=${encodeURIComponent(start)}`),
      apiGet(`/api/v1/history?node_id=${encodeURIComponent(nodeId)}&metric=gpu_memory_used_mb_total&from=${encodeURIComponent(start)}`),
      apiGet(`/api/v1/history?node_id=${encodeURIComponent(nodeId)}&metric=gpu_temperature_avg&from=${encodeURIComponent(start)}`),
    ]));
    if (requestId !== state.detailRequestId) return;
    drawChart(els.lossChart, queries[0].points || [], "#38bdf8", "损失");
    drawChart(els.gpuChart, queries[1].points || [], "#22c55e", "GPU 利用率");
    drawChart(els.memChart, queries[2].points || [], "#f59e0b", "显存 MB");
    drawChart(els.tempChart, queries[3].points || [], "#ef4444", "温度 °C");
  } catch (error) {
    if (requestId !== state.detailRequestId) return;
    resetDetailCharts();
    showBanner(error.message || String(error), "error");
  }
}

function openConnectDrawer() {
  els.connectDrawer.classList.remove("hidden");
  els.connectDrawer.scrollIntoView({ behavior: "smooth", block: "start" });
  window.setTimeout(() => els.hostInput?.focus(), 50);
}
function closeConnectDrawer() {
  els.connectDrawer.classList.add("hidden");
}
function closeDetailDrawer() {
  state.detailRequestId += 1;
  els.detailDrawer.classList.add("hidden");
  syncOverlayState();
}

async function setupServiceWorker() {
  if (!("serviceWorker" in navigator)) return;
  const isLocalHost = ["localhost", "127.0.0.1", "::1"].includes(window.location.hostname);
  if (isLocalHost) {
    try {
      const registrations = await navigator.serviceWorker.getRegistrations();
      await Promise.all(registrations.map((registration) => registration.unregister()));
      if (window.caches?.keys) {
        const keys = await window.caches.keys();
        await Promise.all(keys.filter((key) => key.startsWith("train-watch-")).map((key) => window.caches.delete(key)));
      }
    } catch (_error) {
      // ignore cleanup failures on localhost
    }
    return;
  }
  navigator.serviceWorker.register("/service-worker.js").catch(() => {});
}

async function bootstrap() {
  const requestedPanel = new URLSearchParams(window.location.search).get("panel");
  if (els.connectDrawer && els.banner?.parentElement) {
    els.banner.insertAdjacentElement("afterend", els.connectDrawer);
  }
  els.connectBtn.addEventListener("click", openConnectDrawer);
  els.closeConnectBtn.addEventListener("click", closeConnectDrawer);
  els.authForm?.addEventListener("submit", handleAuthSubmit);
  els.authSwitchBtn?.addEventListener("click", () => {
    setAuthMode(state.authMode === "password" ? "token" : "password");
    setAuthError("");
  });
  els.authRegisterBtn?.addEventListener("click", () => {
    handleBootstrapAdmin().catch((error) => setAuthError(error.message || String(error)));
  });
  els.connectDrawer.addEventListener("click", (event) => {
    if (event.target === els.connectDrawer) closeConnectDrawer();
  });
  els.connectForm.addEventListener("submit", submitConnection);
  els.jobForm?.addEventListener("submit", submitJob);
  els.refreshBtn.addEventListener("click", () => refreshNow().catch((error) => showBanner(error.message || String(error), "error")));
  els.tokenBtn.addEventListener("click", async () => {
    try {
      if ((state.currentUser?.source === "session" || state.token) && window.confirm("Log out current access and return to sign-in?")) {
        await logoutCurrentSession();
      }
      await fetchAuthConfig().catch(() => {});
      showAuthGate();
    } catch (error) {
      showBanner(error.message || String(error), "error");
    }
  });
  els.alertsBtn.addEventListener("click", () => {
    state.unreadAlerts = 0;
    renderAlertFeed();
    jumpToTarget("eventsPanel");
    if (state.alertFeed.length) {
      showBanner(alertMessage(state.alertFeed[0]), state.alertFeed[0].status === "completed" ? "info" : "error", { kind: "event" });
    }
  });
  els.jumpSelect?.addEventListener("change", () => {
    const targetId = els.jumpSelect.value;
    if (!targetId) return;
    jumpToTarget(targetId);
    resetJumpSelection();
  });
  els.closeDrawerBtn.addEventListener("click", closeDetailDrawer);
  els.detailDrawer.addEventListener("click", (event) => {
    if (event.target === els.detailDrawer) closeDetailDrawer();
  });
  els.aliasSelect?.addEventListener("change", () => {
    const item = state.sshAliases.find((entry) => entry.alias === els.aliasSelect.value);
    els.aliasMeta.textContent = aliasDescription(item);
  });
  els.applyAliasBtn?.addEventListener("click", applySelectedAlias);
  els.refreshAliasesBtn?.addEventListener("click", () => loadSshAliases(true));
  await setupServiceWorker();
  await fetchAuthConfig();
  updateTokenButtonVisibility();
  applyFoldState(document);
  if (requestedPanel === "connect") {
    openConnectDrawer();
  }
  if (!state.authConfig?.auth_required) {
    await enterApp();
    return;
  }
  if (state.token) {
    try {
      await loadSessionState({ silent: true });
      await enterApp();
      return;
    } catch (_error) {
      state.token = "";
      state.currentUser = null;
      persistToken("");
    }
  }
  showAuthGate();
}

bootstrap().catch((error) => showBanner(error.message || String(error), "error"));
