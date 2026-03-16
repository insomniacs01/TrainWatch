import { createAlertsController } from "./alerts-controller.js?v=20260316-1";
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

function readPageFromLocation() {
  const rawHash = (window.location.hash || "").replace(/^#/, "");
  if (!rawHash || rawHash === "home") {
    return { type: "home" };
  }
  if (rawHash.startsWith("node/")) {
    const nodeId = rawHash.slice(5);
    return nodeId ? { type: "node", nodeId: decodeURIComponent(nodeId) } : { type: "home" };
  }
  return { type: "home" };
}

function pageHash(page) {
  return page?.type === "node" && page?.nodeId ? `#node/${encodeURIComponent(page.nodeId)}` : "#home";
}

function samePage(left, right) {
  return left?.type === right?.type && left?.nodeId === right?.nodeId;
}

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
  unreadAlertKeys: new Set(),
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
  requestedPage: readPageFromLocation(),
  currentPage: { type: "home" },
  pendingScrollTarget: null,
};

const els = {
  subtitle: document.getElementById("subtitle"),
  pageNav: document.getElementById("pageNav"),
  homePage: document.getElementById("homePage"),
  devicePage: document.getElementById("devicePage"),
  summaryGrid: document.getElementById("summaryGrid"),
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
let jumpToTarget = () => {};

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
  enableTeamMode,
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

({ jumpToTarget } = createNavigationController({ statusLabel }));

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
      showAuthGate(message || "请重新登录。");
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

function getNodeById(nodeId) {
  return (state.snapshot?.nodes || []).find((item) => item.id === nodeId);
}

function resolveCurrentPage() {
  const page = state.requestedPage;
  if (page?.type === "node" && page.nodeId) {
    const node = getNodeById(page.nodeId);
    if (node) {
      return { type: "node", nodeId: node.id };
    }
    if (state.snapshot) {
      return { type: "home" };
    }
  }
  return { type: "home" };
}

function buildPageNavButton({ label, meta, pageType, nodeId, active = false } = {}) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = `page-nav-item${active ? " is-active" : ""}`;
  button.dataset.pageType = pageType;
  if (nodeId) {
    button.dataset.nodeId = nodeId;
  }
  if (active) {
    button.setAttribute("aria-current", "page");
  }

  const labelEl = document.createElement("span");
  labelEl.className = "page-nav-label";
  labelEl.textContent = label;

  const metaEl = document.createElement("span");
  metaEl.className = "page-nav-meta";
  metaEl.textContent = meta;

  button.append(labelEl, metaEl);
  return button;
}

function buildPageNavGroup(title, children = []) {
  const group = document.createElement("div");
  group.className = "page-nav-group";

  const titleEl = document.createElement("span");
  titleEl.className = "page-nav-group-label";
  titleEl.textContent = title;
  group.appendChild(titleEl);

  children.forEach((child) => {
    group.appendChild(child);
  });
  return group;
}

function renderPageNav(snapshot = state.snapshot) {
  if (!els.pageNav) return;

  const activePage = state.currentPage?.type === "node" && getNodeById(state.currentPage.nodeId)
    ? state.currentPage
    : { type: "home" };
  const nodes = snapshot?.nodes || [];
  const fragment = document.createDocumentFragment();

  fragment.appendChild(buildPageNavGroup("通用", [
    buildPageNavButton({
      label: "首页",
      meta: "概览、告警和队列",
      pageType: "home",
      active: activePage.type === "home",
    }),
  ]));

  const deviceChildren = [];
  if (nodes.length) {
    nodes.forEach((node) => {
      const runs = Array.isArray(node.runs) ? node.runs.length : 0;
      const gpus = Array.isArray(node.gpus) ? node.gpus.length : 0;
      deviceChildren.push(buildPageNavButton({
        label: node.label || node.host || node.id,
        meta: statusLabel(node.status) + " · " + runs + " 个任务 · " + gpus + " 张 GPU",
        pageType: "node",
        nodeId: node.id,
        active: activePage.type === "node" && activePage.nodeId === node.id,
      }));
    });
  } else {
    const empty = document.createElement("p");
    empty.className = "page-nav-empty";
    empty.textContent = "暂时还没有已连接设备。";
    deviceChildren.push(empty);
  }
  fragment.appendChild(buildPageNavGroup("已连接设备", deviceChildren));

  els.pageNav.replaceChildren(fragment);
}

function renderCurrentPage({ replaceInvalidHash = false } = {}) {
  const resolvedPage = resolveCurrentPage();
  const requestedPage = state.requestedPage;

  if (replaceInvalidHash && state.snapshot && !samePage(requestedPage, resolvedPage)) {
    history.replaceState(null, "", pageHash(resolvedPage));
    state.requestedPage = resolvedPage;
  }

  state.currentPage = resolvedPage;
  const isHomePage = resolvedPage.type === "home";
  els.homePage?.classList.toggle("hidden", !isHomePage);
  els.devicePage?.classList.toggle("hidden", isHomePage);

  if (isHomePage) {
    if (els.nodeList) {
      els.nodeList.innerHTML = "";
    }
  } else {
    const node = getNodeById(resolvedPage.nodeId);
    if (node) {
      renderNodesList({
        nodeListEl: els.nodeList,
        snapshot: { ...state.snapshot, nodes: [node] },
        onOpenRunDetail: openRunDetail,
        onRemoveConnection: removeConnection,
        onOpenConnect: openConnectDrawer,
        removingConnectionIds: state.removingConnectionIds,
      });
    }
  }

  applyFoldState(document);
  renderPageNav();

  if (state.pendingScrollTarget) {
    const targetId = state.pendingScrollTarget;
    state.pendingScrollTarget = null;
    window.requestAnimationFrame(() => {
      jumpToTarget(targetId);
    });
  }
}

function navigateToPage(page, { replace = false, scrollTarget = null } = {}) {
  const requestedPage = page?.type === "node" && page?.nodeId
    ? { type: "node", nodeId: page.nodeId }
    : { type: "home" };

  state.requestedPage = requestedPage;
  state.pendingScrollTarget = scrollTarget;

  const nextHash = pageHash(requestedPage);
  if (replace) {
    history.replaceState(null, "", nextHash);
    renderCurrentPage({ replaceInvalidHash: false });
    return;
  }
  if (window.location.hash !== nextHash) {
    window.location.hash = nextHash;
    return;
  }
  renderCurrentPage({ replaceInvalidHash: false });
}

function renderSnapshot(snapshot) {
  state.snapshot = snapshot;
  updateTokenButtonVisibility();
  const userLabel = state.currentUser ? ` · ${state.currentUser.display_name || state.currentUser.username}` : "";
  els.subtitle.textContent = `最近刷新：${snapshot.generated_at}${userLabel}`;
  renderSummaryCards(els.summaryGrid, snapshot);
  renderAlertFeed();
  renderCurrentPage({ replaceInvalidHash: true });
  applyFoldState(document);
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
      if (state.authConfig?.mode === "personal") {
        const confirmed = window.confirm("开启团队模式后，需要创建管理员账号并通过账号密码进入。是否继续？");
        if (!confirmed) return;
        const payload = await enableTeamMode();
        showBanner(payload.bootstrap_required ? "团队模式已启用，请先创建管理员账号。" : "团队模式已启用，请使用账号登录。", "info");
        showAuthGate();
        return;
      }
      if ((state.currentUser?.source === "session" || state.token) && window.confirm("确定要退出当前登录状态并返回登录页吗？")) {
        await logoutCurrentSession();
      }
      await fetchAuthConfig().catch(() => {});
      showAuthGate();
    } catch (error) {
      showBanner(error.message || String(error), "error");
    }
  });
  els.alertsBtn.addEventListener("click", () => {
    if (state.unreadAlertKeys instanceof Set) {
      state.unreadAlertKeys.clear();
    }
    state.unreadAlerts = 0;
    renderAlertFeed();
    navigateToPage({ type: "home" }, { scrollTarget: "eventsPanel" });
    if (state.alertFeed.length) {
      showBanner(alertMessage(state.alertFeed[0]), state.alertFeed[0].status === "completed" ? "info" : "error", { kind: "event" });
    }
  });
  els.pageNav?.addEventListener("click", (event) => {
    const target = event.target instanceof Element ? event.target : null;
    const trigger = target?.closest("[data-page-type]");
    if (!(trigger instanceof HTMLElement)) return;
    const pageType = trigger.dataset.pageType;
    if (pageType === "node" && trigger.dataset.nodeId) {
      navigateToPage({ type: "node", nodeId: trigger.dataset.nodeId });
      return;
    }
    navigateToPage({ type: "home" });
  });
  window.addEventListener("hashchange", () => {
    state.requestedPage = readPageFromLocation();
    renderCurrentPage({ replaceInvalidHash: true });
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
  renderPageNav();
  renderCurrentPage({ replaceInvalidHash: false });
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
