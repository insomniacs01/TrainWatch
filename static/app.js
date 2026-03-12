const state = {
  snapshot: null,
  token: localStorage.getItem("train_watch_token") || "",
  events: [],
  unreadAlerts: 0,
  ws: null,
  sshAliases: [],
  connectSubmitting: false,
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
  emptyConnectBtn: null,
};

function authHeaders(extra = {}) {
  return state.token ? { ...extra, "x-train-watch-token": state.token } : extra;
}

function statusClass(status) { return `status-${status || "unknown"}`; }
function fmtNumber(value, digits = 2) { return value === null || value === undefined || Number.isNaN(Number(value)) ? "--" : Number(value).toFixed(digits); }
function fmtInt(value) { return value === null || value === undefined || Number.isNaN(Number(value)) ? "--" : Math.round(Number(value)).toString(); }
function fmtGb(value) { return value === null || value === undefined || Number.isNaN(Number(value)) ? "--" : `${Number(value).toFixed(1)} GB`; }
function fmtGbFromMb(value) { return value === null || value === undefined || Number.isNaN(Number(value)) ? "--" : `${(Number(value) / 1024).toFixed(1)} GB`; }
function fmtDuration(seconds) {
  if (!seconds && seconds !== 0) return "--";
  const total = Math.max(0, Math.round(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}
function fmtDateTime(value) {
  if (!value) return "--";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}
function taskDisplay(run) { return run.task_name || run.task_command || run.label || "--"; }
function remainingDisplay(run) {
  if (run.remaining_seconds !== null && run.remaining_seconds !== undefined) return fmtDuration(run.remaining_seconds);
  return run.eta || fmtDuration(run.eta_seconds);
}
function progressDisplay(run) {
  return run.progress_percent === null || run.progress_percent === undefined ? "--" : `${fmtNumber(run.progress_percent, 1)}%`;
}
function etaHint(run) {
  if (run.remaining_seconds !== null && run.remaining_seconds !== undefined) return "";
  if (run.eta_seconds !== null && run.eta_seconds !== undefined) return "";
  if (run.status !== "running" && run.status !== "stalled") return "";
  return run.log_path ? "日志里还没有 ETA / step_total" : "未找到可读日志，暂时无法估算";
}
function latestLog(run) { return run.last_log_line || run.error || "暂无日志"; }
function noteChip(label, value) { return `<span class="chip">${label}: ${value}</span>`; }

function showBanner(text, tone = "error") {
  els.banner.textContent = text;
  els.banner.classList.remove("hidden");
  els.banner.style.borderColor = tone === "info" ? "rgba(56, 189, 248, 0.35)" : "rgba(239, 68, 68, 0.35)";
  els.banner.style.background = tone === "info" ? "rgba(12, 74, 110, 0.35)" : "rgba(127, 29, 29, 0.35)";
}
function clearBanner() { els.banner.classList.add("hidden"); }

function aliasDescription(item) {
  if (!item) return "还没有选择 SSH alias。";
  const parts = [
    item.hostname ? `HostName ${item.hostname}` : "",
    item.user ? `User ${item.user}` : "",
    item.port ? `Port ${item.port}` : "",
    item.proxyjump ? `ProxyJump ${item.proxyjump}` : "",
    item.identityfile ? `Key ${item.identityfile}` : "",
  ].filter(Boolean);
  return parts.length ? parts.join(" · ") : "这个 alias 没有额外字段，将直接按 ssh 配置解析。";
}

function renderAliasOptions() {
  if (!els.aliasSelect || !els.aliasMeta) return;
  const aliases = state.sshAliases || [];
  const previousValue = els.aliasSelect.value;
  els.aliasSelect.innerHTML = [`<option value="">选择一个 Host alias（可选）</option>`]
    .concat(aliases.map((item) => `<option value="${item.alias}">${item.alias}</option>`))
    .join("");
  if (aliases.some((item) => item.alias === previousValue)) {
    els.aliasSelect.value = previousValue;
  }
  const selected = aliases.find((item) => item.alias === els.aliasSelect.value);
  if (!aliases.length) {
    els.aliasMeta.textContent = "没有在当前环境里发现可用的 SSH aliases；你仍然可以手动输入 Host / User / Password。";
    return;
  }
  els.aliasMeta.textContent = aliasDescription(selected || aliases[0]);
}

async function loadSshAliases(showSuccessBanner = false) {
  try {
    const payload = await apiGet("/api/v1/ssh-aliases");
    state.sshAliases = payload.items || [];
    renderAliasOptions();
    if (showSuccessBanner) {
      showBanner(`已加载 ${state.sshAliases.length} 个 SSH aliases`, "info");
    }
  } catch (error) {
    state.sshAliases = [];
    renderAliasOptions();
    if (showSuccessBanner) {
      showBanner(error.message || String(error), "error");
    }
  }
}

function applySelectedAlias() {
  const alias = els.aliasSelect?.value || "";
  const item = state.sshAliases.find((entry) => entry.alias === alias);
  if (!item) {
    showBanner("请先选择一个 SSH alias", "error");
    return;
  }
  document.getElementById("hostInput").value = item.alias;
  document.getElementById("userInput").value = item.user || "";
  document.getElementById("portInput").value = item.port || 22;
  document.getElementById("keyPathInput").value = item.identityfile || "";
  document.getElementById("labelInput").value = document.getElementById("labelInput").value || item.alias;
  document.getElementById("passwordInput").value = "";
  els.aliasMeta.textContent = aliasDescription(item);
  showBanner(`已把 ${item.alias} 填入表单`, "info");
}

function renderSummary(snapshot) {
  const items = [
    ["节点", snapshot.summary.nodes_total],
    ["在线", snapshot.summary.nodes_online],
    ["异常", snapshot.summary.nodes_degraded + snapshot.summary.nodes_offline],
    ["运行任务", snapshot.summary.runs_running],
    ["告警任务", snapshot.summary.runs_alerting],
    ["CPU", `${fmtNumber(snapshot.summary.cpu_usage_avg, 1)}%`],
    ["内存", `${fmtNumber(snapshot.summary.memory_used_percent_avg, 1)}%`],
    ["磁盘", `${fmtNumber(snapshot.summary.disk_used_percent_avg, 1)}%`],
    ["GPU", snapshot.summary.gpus_total],
    ["忙碌 GPU", snapshot.summary.gpus_busy],
  ];
  els.summaryGrid.innerHTML = items.map(([label, value]) => `
    <article class="summary-item card">
      <span class="kicker">${label}</span>
      <strong>${value ?? 0}</strong>
    </article>
  `).join("");
}

function renderEvents() {
  if (!state.events.length) {
    els.eventsList.innerHTML = `<div class="event-item"><strong>暂无告警事件</strong><p class="subtle">连接真实 SSH 后，状态变化会显示在这里。</p></div>`;
    return;
  }
  els.eventsList.innerHTML = state.events.map((event) => `
    <article class="event-item">
      <strong class="${statusClass(event.status)}">${event.message}</strong>
      <p class="subtle">${event.at}</p>
    </article>
  `).join("");
}

function renderEmptyState() {
  els.nodeList.innerHTML = `
    <section class="node-card">
      <h2>还没有连接任何真实机器</h2>
      <p class="subtle">点右上角“连接 SSH”，输入主机信息后，系统会立刻开始轮询真实服务器状态。即使不填日志路径，也会自动尝试发现正在跑的训练任务和可读取日志。也支持直接输入本机 <code>~/.ssh/config</code> 里的 Host alias。</p>
      <div class="note-row" style="margin-top:16px;">
        <button id="emptyConnectBtn" class="secondary-button primary-button">现在连接 SSH</button>
      </div>
      <div class="note-row">
        <span class="chip">支持 password</span>
        <span class="chip">支持 key path</span>
        <span class="chip">支持 SSH alias / ProxyJump</span>
        <span class="chip">支持 CPU / RAM / Disk</span>
      </div>
    </section>
  `;
  els.emptyConnectBtn = document.getElementById("emptyConnectBtn");
  els.emptyConnectBtn?.addEventListener("click", openConnectDrawer);
}

function renderNodes(snapshot) {
  if (!snapshot.nodes.length) {
    renderEmptyState();
    return;
  }
  els.nodeList.innerHTML = snapshot.nodes.map((node) => {
    const metrics = node.metrics || {};
    const metricCards = [
      ["CPU", `${fmtNumber(metrics.cpu_usage_percent, 1)}%`],
      ["内存", `${fmtNumber(metrics.memory_used_percent, 1)}% · ${fmtGbFromMb(metrics.memory_used_mb)}`],
      ["磁盘", `${fmtNumber(metrics.disk_used_percent, 1)}% · ${fmtGb(metrics.disk_used_gb)}`],
      ["平均 GPU 利用率", `${fmtInt(metrics.gpu_utilization_avg)}%`],
      ["总显存", `${fmtInt(metrics.gpu_memory_used_mb_total)} MB`],
      ["平均温度", `${fmtInt(metrics.gpu_temperature_avg)}°C`],
      ["GPU 进程", fmtInt(metrics.gpu_process_count)],
    ].map(([label, value]) => `
      <div class="metric-card">
        <span class="kicker">${label}</span>
        <strong>${value}</strong>
      </div>
    `).join("");

    const gpuCards = (node.gpus || []).map((gpu) => `
      <article class="gpu-card">
        <div class="gpu-row">
          <strong>GPU ${gpu.index}</strong>
          <span class="chip">${gpu.name}</span>
        </div>
        <div class="metric-grid">
          <div class="metric-card"><span class="kicker">Util</span><strong>${fmtInt(gpu.utilization_gpu)}%</strong></div>
          <div class="metric-card"><span class="kicker">显存</span><strong>${fmtInt(gpu.memory_used_mb)} / ${fmtInt(gpu.memory_total_mb)}</strong></div>
          <div class="metric-card"><span class="kicker">温度</span><strong>${fmtInt(gpu.temperature_c)}°C</strong></div>
          <div class="metric-card"><span class="kicker">功耗</span><strong>${fmtInt(gpu.power_draw_w)}W</strong></div>
        </div>
      </article>
    `).join("");

    const runCards = (node.runs || []).map((run) => `
      <article class="run-card" data-node-id="${node.id}" data-run-id="${run.id}">
        <div class="run-row">
          <strong>${run.label}</strong>
          <span class="status-pill ${statusClass(run.status)}">${run.status}</span>
        </div>
        <p class="task-line"><span class="kicker">当前任务</span><code>${taskDisplay(run)}</code></p>
        <div class="metric-grid">
          <div class="metric-card"><span class="kicker">Loss</span><strong>${fmtNumber(run.loss, 4)}</strong></div>
          <div class="metric-card"><span class="kicker">已运行</span><strong>${fmtDuration(run.elapsed_seconds)}</strong></div>
          <div class="metric-card"><span class="kicker">预计剩余</span><strong>${remainingDisplay(run)}</strong></div>
          <div class="metric-card"><span class="kicker">Step</span><strong>${run.step ?? "--"}${run.step_total ? ` / ${run.step_total}` : ""}</strong></div>
          <div class="metric-card"><span class="kicker">最近更新</span><strong>${fmtDateTime(run.last_update_at)}</strong></div>
        </div>
        <div class="note-row">
          ${noteChip("parser", run.parser)}
          ${noteChip("proc", run.matched_processes?.length || 0)}
          ${noteChip("progress", progressDisplay(run))}
          ${run.estimated_end_at ? noteChip("finish", fmtDateTime(run.estimated_end_at)) : ""}
          ${etaHint(run) ? noteChip("eta", etaHint(run)) : ""}
          ${run.error ? noteChip("note", run.error) : ""}
        </div>
        <div class="log-tail">${latestLog(run)}</div>
      </article>
    `).join("");

    return `
      <section class="node-card">
        <div class="node-header">
          <div>
            <h2>${node.label}</h2>
            <p class="subtle">${node.hostname || node.host} · ${node.collected_at}</p>
          </div>
          <div class="node-actions">
            <span class="status-pill ${statusClass(node.status)}">${node.status}</span>
            <button class="secondary-button danger-button disconnect-button" data-remove-node-id="${node.id}" data-remove-node-label="${node.label}">移除连接</button>
            ${node.error ? `<p class="subtle connection-error">${node.error}</p>` : ""}
          </div>
        </div>
        <div class="metric-grid">${metricCards}</div>
        <div class="note-row">
          ${noteChip("load", `${fmtNumber(metrics.loadavg_1m, 2)} / ${fmtNumber(metrics.loadavg_5m, 2)} / ${fmtNumber(metrics.loadavg_15m, 2)}`)}
          ${noteChip("RAM", `${fmtGbFromMb(metrics.memory_used_mb)} / ${fmtGbFromMb(metrics.memory_total_mb)}`)}
          ${noteChip("Disk", `${fmtGb(metrics.disk_used_gb)} / ${fmtGb(metrics.disk_total_gb)}`)}
          ${noteChip("Swap", `${fmtNumber(metrics.swap_used_percent, 1)}%`)}
        </div>
        <div class="gpu-grid">${gpuCards || '<article class="gpu-card"><p class="subtle">暂无 GPU 数据</p></article>'}</div>
        <div class="run-grid">${runCards || '<article class="run-card"><p class="subtle">当前没有发现活跃训练任务。只连 SSH 也会自动尝试从 GPU 进程和 stdout/stderr 日志里发现任务；如果还没有显示，通常是机器上暂时没在训练，或训练输出没有落到可读取的日志文件。</p></article>'}</div>
      </section>
    `;
  }).join("");

  document.querySelectorAll(".run-card[data-node-id]").forEach((element) => {
    element.addEventListener("click", () => openRunDetail(element.dataset.nodeId, element.dataset.runId));
  });
  document.querySelectorAll(".disconnect-button[data-remove-node-id]").forEach((element) => {
    element.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      removeConnection(element.dataset.removeNodeId, element.dataset.removeNodeLabel);
    });
  });
}

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
  renderEvents();
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
  renderSummary(snapshot);
  renderNodes(snapshot);
  renderEvents();
  if (!snapshot.nodes.length) {
    showBanner("还没有连接真实 SSH 机器，点击“连接 SSH”开始。", "info");
  } else if (!els.banner.textContent.includes("→")) {
    clearBanner();
  }
}

async function responseError(response) {
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    try {
      const payload = await response.json();
      return payload.detail || payload.error || JSON.stringify(payload);
    } catch (_error) {}
  }
  return response.text();
}

function setConnectSubmitting(submitting) {
  state.connectSubmitting = submitting;
  if (els.submitConnectBtn) {
    els.submitConnectBtn.disabled = submitting;
    els.submitConnectBtn.textContent = submitting ? "连接中..." : "连接并开始监控";
  }
}

async function apiGet(path) {
  const response = await fetch(path, { headers: authHeaders() });
  if (response.status === 401) {
    const token = window.prompt("请输入 Train Watch 令牌", state.token || "");
    if (token !== null) {
      state.token = token.trim();
      localStorage.setItem("train_watch_token", state.token);
      return apiGet(path);
    }
  }
  if (!response.ok) throw new Error(await responseError(response));
  return response.json();
}

async function apiJson(method, path, body) {
  const response = await fetch(path, {
    method,
    headers: authHeaders({ "content-type": "application/json" }),
    body: body ? JSON.stringify(body) : undefined,
  });
  if (response.status === 401) {
    const token = window.prompt("请输入 Train Watch 令牌", state.token || "");
    if (token !== null) {
      state.token = token.trim();
      localStorage.setItem("train_watch_token", state.token);
      return apiJson(method, path, body);
    }
  }
  if (!response.ok) throw new Error(await responseError(response));
  return response.json();
}

async function fetchSnapshot() {
  const snapshot = await apiGet("/api/v1/snapshot");
  renderSnapshot(snapshot);
}

async function refreshNow() {
  const snapshot = await apiJson("POST", "/api/v1/refresh");
  renderSnapshot(snapshot);
}

function connectStream() {
  if (state.ws) state.ws.close();
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const tokenQuery = state.token ? `?token=${encodeURIComponent(state.token)}` : "";
  state.ws = new WebSocket(`${protocol}//${window.location.host}/api/v1/stream${tokenQuery}`);
  state.ws.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    if (payload.type === "snapshot") {
      renderSnapshot(payload.snapshot);
      handleEvents(payload.events || []);
    }
    if (payload.type === "error") {
      showBanner(payload.error || "实时通道异常", "error");
    }
  };
  state.ws.onclose = () => setTimeout(connectStream, 2500);
}

function drawChart(canvas, points, color, label) {
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#0f1530";
  ctx.fillRect(0, 0, width, height);
  ctx.strokeStyle = "rgba(148, 163, 184, 0.18)";
  for (let index = 0; index < 4; index += 1) {
    const y = 20 + index * 40;
    ctx.beginPath();
    ctx.moveTo(40, y);
    ctx.lineTo(width - 12, y);
    ctx.stroke();
  }
  if (!points.length) {
    ctx.fillStyle = "#94a3b8";
    ctx.font = "12px sans-serif";
    ctx.fillText("暂无数据", 40, 90);
    return;
  }
  const values = points.map((point) => Number(point.value));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((point, index) => {
    const x = 40 + (index / Math.max(points.length - 1, 1)) * (width - 56);
    const y = height - 24 - ((Number(point.value) - min) / range) * (height - 48);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
  ctx.fillStyle = "#e2e8f0";
  ctx.font = "12px sans-serif";
  ctx.fillText(`${label}: ${fmtNumber(values[values.length - 1], 3)}`, 40, 16);
}

function renderProcesses(run) {
  if (!run.matched_processes || !run.matched_processes.length) {
    els.drawerProcesses.innerHTML = `<div class="process-item"><p class="subtle">暂无匹配进程</p></div>`;
    return;
  }
  els.drawerProcesses.innerHTML = run.matched_processes.map((proc) => `
    <article class="process-item">
      <div class="process-row">
        <strong>PID ${proc.pid ?? "--"}</strong>
        <span class="chip">${fmtDuration(proc.elapsed_seconds)}</span>
      </div>
      <code>${proc.command || ""}</code>
    </article>
  `).join("");
}

async function openRunDetail(nodeId, runId) {
  const node = (state.snapshot?.nodes || []).find((item) => item.id === nodeId);
  const run = (node?.runs || []).find((item) => item.id === runId);
  if (!node || !run) return;
  els.drawerEyebrow.textContent = node.label;
  els.drawerTitle.textContent = run.label;
  els.drawerMeta.innerHTML = [
    ["状态", run.status],
    ["任务", taskDisplay(run)],
    ["PID", run.task_pid ?? "--"],
    ["已运行", fmtDuration(run.elapsed_seconds)],
    ["预计剩余", remainingDisplay(run)],
    ["预计完成", fmtDateTime(run.estimated_end_at)],
    ["开始时间", fmtDateTime(run.started_at)],
    ["进度", progressDisplay(run)],
    ["Parser", run.parser],
    ["Loss", fmtNumber(run.loss, 4)],
    ["ETA", run.eta || fmtDuration(run.eta_seconds)],
    ["Step", `${run.step ?? "--"}${run.step_total ? ` / ${run.step_total}` : ""}`],
    ["日志", run.log_path || "--"],
    ["最近更新", fmtDateTime(run.last_update_at)],
    ["错误", run.error || "--"],
  ].map(([label, value]) => `
    <article class="card">
      <span class="kicker">${label}</span>
      <strong>${value}</strong>
    </article>
  `).join("");
  els.drawerLog.textContent = latestLog(run);
  renderProcesses(run);
  els.detailDrawer.classList.remove("hidden");

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

async function removeConnection(nodeId, label) {
  if (!nodeId) return;
  if (!window.confirm(`确认移除 ${label || nodeId} 吗？`)) return;
  try {
    await apiJson("DELETE", `/api/v1/connections/${encodeURIComponent(nodeId)}`);
    showBanner(`已移除 ${label || nodeId}`, "info");
    await refreshNow();
  } catch (error) {
    showBanner(error.message || String(error), "error");
  }
}

async function submitConnection(event) {
  event.preventDefault();
  if (state.connectSubmitting) return;
  setConnectSubmitting(true);
  const form = new FormData(els.connectForm);
  const logPath = String(form.get("log_path") || "").trim();
  const logGlob = String(form.get("log_glob") || "").trim();
  const processMatch = String(form.get("process_match") || "").trim();
  const runLabel = String(form.get("run_label") || "Main Run").trim() || "Main Run";
  const runs = [];
  if (logPath || logGlob || processMatch) {
    runs.push({
      label: runLabel,
      log_path: logPath || null,
      log_glob: logGlob || null,
      process_match: processMatch,
      parser: String(form.get("parser") || "auto"),
      stall_after_seconds: Number(form.get("stall_after_seconds") || 900),
    });
  }
  const payload = {
    label: String(form.get("label") || "").trim() || null,
    host: String(form.get("host") || "").trim(),
    port: Number(form.get("port") || 22),
    user: String(form.get("user") || "").trim(),
    password: String(form.get("password") || "").trim() || null,
    key_path: String(form.get("key_path") || "").trim() || null,
    runs,
  };
  if (!payload.host) {
    showBanner("Host 不能为空", "error");
    return;
  }
  try {
    await apiJson("POST", "/api/v1/connections", payload);
    closeConnectDrawer();
    els.connectForm.reset();
    document.getElementById("portInput").value = 22;
    document.getElementById("stallInput").value = 900;
    renderAliasOptions();
    showBanner(`已将 ${payload.host} 加入监控队列，正在后台建立 SSH 连接并采集首轮状态。`, "info");
  } catch (error) {
    showBanner(error.message || String(error), "error");
  } finally {
    setConnectSubmitting(false);
  }
}

async function bootstrap() {
  els.connectBtn.addEventListener("click", openConnectDrawer);
  els.closeConnectBtn.addEventListener("click", closeConnectDrawer);
  els.connectDrawer.addEventListener("click", (event) => {
    if (event.target === els.connectDrawer) closeConnectDrawer();
  });
  els.connectForm.addEventListener("submit", submitConnection);
  els.refreshBtn.addEventListener("click", () => refreshNow().catch((error) => showBanner(error.message || String(error), "error")));
  els.tokenBtn.addEventListener("click", () => {
    const token = window.prompt("设置 Train Watch 令牌", state.token || "");
    if (token === null) return;
    state.token = token.trim();
    localStorage.setItem("train_watch_token", state.token);
    fetchSnapshot().catch((error) => showBanner(error.message || String(error), "error"));
    loadSshAliases().catch(() => {});
    connectStream();
  });
  els.alertsBtn.addEventListener("click", () => {
    state.unreadAlerts = 0;
    updateAlertBadge();
    renderEvents();
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
  connectStream();
}

bootstrap().catch((error) => showBanner(error.message || String(error), "error"));
