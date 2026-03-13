import { createApiClient } from "./api-client.js";
import { createConnectionsController } from "./connections-controller.js";
import { drawChart } from "./charts.js";
import { createJobsController } from "./jobs-controller.js";
import { renderExternalJobsPanel, renderJobsPanel } from "./jobs-view.js";
import { renderEventsList, renderNodesList, renderSummaryCards } from "./nodes-view.js";
import { renderRunDetail } from "./run-detail-view.js";
import { createSnapshotStream } from "./stream-client.js";
import {
  aliasDescription,
} from "./formatters.js";
import { persistToken, restoreToken } from "./token-store.js";

const state = {
  snapshot: null,
  token: restoreToken(),
  events: [],
  unreadAlerts: 0,
  sshAliases: [],
  connections: [],
  jobs: [],
  jobsSummary: {},
  externalJobs: [],
  externalJobsSummary: {},
  connectSubmitting: false,
  jobSubmitting: false,
};

const els = {
  subtitle: document.getElementById("subtitle"),
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
  alertsBtn: document.getElementById("alertsBtn"),
  alertBadge: document.getElementById("alertBadge"),
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

const stream = createSnapshotStream({
  getToken: () => state.token,
  onSnapshot: (payload) => {
    renderSnapshot(payload.snapshot);
    handleEvents(payload.events || []);
    loadConnections().catch(() => {});
    loadJobs().catch(() => {});
  },
  onError: (message) => {
    showBanner(message, "error");
  },
});

function showBanner(text, tone = "error") {
  els.banner.textContent = text;
  els.banner.classList.remove("hidden");
  els.banner.style.borderColor = tone === "info" ? "rgba(56, 189, 248, 0.35)" : "rgba(239, 68, 68, 0.35)";
  els.banner.style.background = tone === "info" ? "rgba(12, 74, 110, 0.35)" : "rgba(127, 29, 29, 0.35)";
}
function clearBanner() { els.banner.classList.add("hidden"); }

const jobsController = createJobsController({
  state,
  els,
  apiGet,
  apiJson,
  showBanner,
  renderJobsPanel,
  renderExternalJobsPanel,
});

const connectionsController = createConnectionsController({
  state,
  els,
  apiGet,
  apiJson,
  showBanner,
  closeConnectDrawer,
  onConnectionsChanged: jobsController.loadConnections,
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
  els.alertBadge.textContent = String(state.unreadAlerts);
  els.alertBadge.classList.toggle("hidden", state.unreadAlerts <= 0);
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

function handleEvents(events) {
  if (!events || !events.length) return;
  state.events = events.concat(state.events).slice(0, 20);
  renderEventsList(els.eventsList, state.events);
  const alerting = events.filter((event) => ["completed", "failed", "stalled"].includes(event.status));
  if (!alerting.length) return;
  state.unreadAlerts += alerting.length;
  updateAlertBadge();
  showBanner(alerting[0].message, alerting[0].status === "completed" ? "info" : "error");
  playAlertTone();
}

function renderSnapshot(snapshot) {
  state.snapshot = snapshot;
  els.subtitle.textContent = `最近刷新：${snapshot.generated_at}`;
  renderSummaryCards(els.summaryGrid, snapshot);
  renderNodesList({
    nodeListEl: els.nodeList,
    snapshot,
    onOpenRunDetail: openRunDetail,
    onRemoveConnection: removeConnection,
    onOpenConnect: openConnectDrawer,
  });
  renderEventsList(els.eventsList, state.events);
  if (!snapshot.nodes.length) {
    showBanner("还没有连接真实 SSH 机器，点击“连接 SSH”开始。", "info");
  } else if (!els.banner.textContent.includes("→")) {
    clearBanner();
  }
}

async function fetchSnapshot() {
  const snapshot = await apiGet("/api/v1/snapshot");
  renderSnapshot(snapshot);
  await loadConnections();
  await loadJobs();
}

async function refreshNow() {
  const snapshot = await apiJson("POST", "/api/v1/refresh");
  renderSnapshot(snapshot);
  await loadConnections();
  await loadJobs();
}

async function openRunDetail(nodeId, runId) {
  const node = (state.snapshot?.nodes || []).find((item) => item.id === nodeId);
  const run = (node?.runs || []).find((item) => item.id === runId);
  if (!node || !run) return;
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

  const end = new Date();
  const start = new Date(end.getTime() - 6 * 3600 * 1000).toISOString();
  const queries = await Promise.all([
    apiGet(`/api/v1/history?node_id=${encodeURIComponent(nodeId)}&run_id=${encodeURIComponent(runId)}&metric=loss&from=${encodeURIComponent(start)}`),
    apiGet(`/api/v1/history?node_id=${encodeURIComponent(nodeId)}&metric=gpu_utilization_avg&from=${encodeURIComponent(start)}`),
    apiGet(`/api/v1/history?node_id=${encodeURIComponent(nodeId)}&metric=gpu_memory_used_mb_total&from=${encodeURIComponent(start)}`),
    apiGet(`/api/v1/history?node_id=${encodeURIComponent(nodeId)}&metric=gpu_temperature_avg&from=${encodeURIComponent(start)}`),
  ]);

  drawChart(els.lossChart, queries[0].points || [], "#38bdf8", "Loss");
  drawChart(els.gpuChart, queries[1].points || [], "#22c55e", "GPU Util");
  drawChart(els.memChart, queries[2].points || [], "#f59e0b", "Memory MB");
  drawChart(els.tempChart, queries[3].points || [], "#ef4444", "Temp °C");
}

function openConnectDrawer() { els.connectDrawer.classList.remove("hidden"); }
function closeConnectDrawer() { els.connectDrawer.classList.add("hidden"); }

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
  els.connectBtn.addEventListener("click", openConnectDrawer);
  els.closeConnectBtn.addEventListener("click", closeConnectDrawer);
  els.connectDrawer.addEventListener("click", (event) => {
    if (event.target === els.connectDrawer) closeConnectDrawer();
  });
  els.connectForm.addEventListener("submit", submitConnection);
  els.jobForm?.addEventListener("submit", submitJob);
  els.refreshBtn.addEventListener("click", () => refreshNow().catch((error) => showBanner(error.message || String(error), "error")));
  els.tokenBtn.addEventListener("click", () => {
    const token = window.prompt("设置 Train Watch 令牌", state.token || "");
    if (token === null) return;
    state.token = token.trim();
    persistToken(state.token);
    fetchSnapshot().catch((error) => showBanner(error.message || String(error), "error"));
    loadSshAliases().catch(() => {});
    stream.connect();
  });
  els.alertsBtn.addEventListener("click", () => {
    state.unreadAlerts = 0;
    updateAlertBadge();
    renderEventsList(els.eventsList, state.events);
    if (state.events.length) {
      showBanner(state.events[0].message, ["completed"].includes(state.events[0].status) ? "info" : "error");
    }
  });
  els.closeDrawerBtn.addEventListener("click", () => els.detailDrawer.classList.add("hidden"));
  els.detailDrawer.addEventListener("click", (event) => {
    if (event.target === els.detailDrawer) els.detailDrawer.classList.add("hidden");
  });
  els.aliasSelect?.addEventListener("change", () => {
    const item = state.sshAliases.find((entry) => entry.alias === els.aliasSelect.value);
    els.aliasMeta.textContent = aliasDescription(item);
  });
  els.applyAliasBtn?.addEventListener("click", applySelectedAlias);
  els.refreshAliasesBtn?.addEventListener("click", () => loadSshAliases(true));
  await setupServiceWorker();
  await fetchSnapshot();
  await loadSshAliases();
  stream.connect();
}

bootstrap().catch((error) => showBanner(error.message || String(error), "error"));
