import { createApiClient } from "./api-client.js";
import { createAlertsController } from "./alerts-controller.js";
import { createAuthController } from "./auth-controller.js";
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
import { restoreToken } from "./token-store.js";

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

let stream = null;

const authController = createAuthController({
  state,
  els,
  apiGet,
  apiJson,
  showBanner,
  localizeMessage,
  fetchSnapshot: () => fetchSnapshot(),
  loadSshAliases: () => loadSshAliases(),
  connectStream: () => stream?.connect(),
  disconnectStream: () => stream?.disconnect(),
  getOverlayElements: () => [els.detailDrawer],
});

const {
  enterApp,
  fetchAuthConfig,
  handleAuthSubmit,
  handleBootstrapAdmin,
  loadSessionState,
  logoutCurrentSession,
  setAuthError,
  setAuthMode,
  showAuthGate,
  syncOverlayState,
  updateTokenButtonVisibility,
  withAuthRecovery,
} = authController;

const alertsController = createAlertsController({
  state,
  els,
  showBanner,
  renderEventsList,
  alertMessage,
  localizeMessage,
  statusLabel,
});

const {
  handleIncomingEvents,
  renderAlertFeed,
  syncAlertFeed,
} = alertsController;

stream = createSnapshotStream({
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
      await logoutCurrentSession();
    }
  }
  showAuthGate();
}

bootstrap().catch((error) => showBanner(error.message || String(error), "error"));
