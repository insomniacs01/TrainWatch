import { createAlertsController } from "./alerts-controller.js";
import { createApiClient } from "./api-client.js";
import { createAuthController } from "./auth-controller.js";
import { drawChart } from "./charts.js";
import { createConnectionsController } from "./connections-controller.js";
import { applyFoldState } from "./fold-state.js";
import {
  aliasDescription,
  alertMessage,
  localizeMessage,
  statusLabel,
} from "./formatters.js";
import { createJobsController } from "./jobs-controller.js";
import { renderExternalJobsPanel, renderJobsPanel } from "./jobs-view.js";
import { renderEventsList, renderNodesList, renderSummaryCards } from "./nodes-view.js";
import { renderRunDetail } from "./run-detail-view.js";
import { createSnapshotStream } from "./stream-client.js";
import { restoreToken } from "./token-store.js";
import {
  createBannerController,
  createDrawerController,
  createNavigationController,
  createRunDetailController,
  setupServiceWorker,
} from "./ui-controllers.js";

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
  hostInput: document.getElementById("hostInput"),
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
const { showBanner, clearBanner } = createBannerController({ bannerEl: els.banner });

let stream = null;
let loadConnections = async () => {};
let loadJobs = async () => {};
let loadSshAliases = async () => {};
let submitConnection = async () => {};
let submitJob = async () => {};
let applySelectedAlias = () => {};
let removeConnection = async () => {};
let openRunDetail = async () => {};
let openConnectDrawer = () => {};
let closeConnectDrawer = () => {};
let closeDetailDrawer = () => {};
let renderJumpOptions = () => {};
let jumpToTarget = () => {};
let resetJumpSelection = () => {};

const authController = createAuthController({
  state,
  els,
  apiGet,
  apiJson,
  showBanner,
  localizeMessage,
  fetchSnapshot: () => fetchSnapshot(),
  loadSshAliases: (...args) => loadSshAliases(...args),
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

({ renderJumpOptions, jumpToTarget, resetJumpSelection } = createNavigationController({
  jumpSelect: els.jumpSelect,
  statusLabel,
}));

({ openConnectDrawer, closeConnectDrawer, closeDetailDrawer } = createDrawerController({
  state,
  connectDrawerEl: els.connectDrawer,
  detailDrawerEl: els.detailDrawer,
  hostInputEl: els.hostInput,
  syncOverlayState,
}));

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

({ loadConnections, loadJobs, submitJob } = jobsController);

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

({
  applySelectedAlias,
  loadSshAliases,
  removeConnection,
  submitConnection,
} = connectionsController);

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

({ openRunDetail } = createRunDetailController({
  state,
  els,
  drawChart,
  renderRunDetail,
  apiGet,
  showBanner,
  withAuthRecovery,
  syncOverlayState,
}));

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
    els.refreshBtn.textContent = refreshing ? "刷新中..." : "立即刷新";
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
