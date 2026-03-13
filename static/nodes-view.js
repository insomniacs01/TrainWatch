import { escapeAttr, escapeHtml } from "./html.js";
import {
  alertMessage,
  etaHint,
  fmtDateTime,
  fmtDuration,
  fmtGb,
  fmtGbFromMb,
  fmtGpuIndices,
  fmtInt,
  fmtNumber,
  latestLog,
  localizeMessage,
  noteChip,
  progressDisplay,
  remainingDisplay,
  statusClass,
  statusLabel,
  taskDisplay,
} from "./formatters.js";

function safeText(value) {
  return escapeHtml(value ?? "");
}

function safeAttr(value) {
  return escapeAttr(value ?? "");
}

function renderFoldSection({ title, meta = "", content = "", open = false, extraClass = "", foldId = "" } = {}) {
  return `
    <details class="fold-section ${safeAttr(extraClass)}" data-fold-id="${safeAttr(foldId)}" ${open ? "open" : ""}>
      <summary class="fold-summary">
        <div class="fold-title-group">
          <strong>${safeText(title)}</strong>
          ${meta ? `<span class="fold-meta">${safeText(meta)}</span>` : ""}
        </div>
        <span class="fold-caret" aria-hidden="true"></span>
      </summary>
      <div class="fold-body">${content}</div>
    </details>
  `;
}

function toInt(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

function commandSignature(command) {
  const text = String(command || "").trim();
  if (!text) return "";
  const parts = text.match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) || [];
  if (!parts.length) return "";
  const basename = (token) => String(token || "").replace(/^['"]|['"]$/g, "").replace(/\/+$/, "").split("/").pop() || "";
  const launcher = basename(parts[0]);
  let searchParts = parts.slice(1);
  if (launcher === "accelerate" && parts[1] === "launch") {
    searchParts = parts.slice(2);
  }
  const script = searchParts.find((item) => item.endsWith(".py") || item.endsWith(".sh"));
  if (script) return basename(script);
  const token = searchParts.find((item) => !item.startsWith("-") && !item.includes("="));
  if (token) return basename(token);
  return launcher;
}

function resolveRunGpuUsage(node, run) {
  const explicitIndices = Array.isArray(run.gpu_indices)
    ? Array.from(new Set(run.gpu_indices.map((item) => toInt(item)).filter((item) => item !== null))).sort((left, right) => left - right)
    : [];
  const explicitMemory = run.gpu_memory_used_mb === null || run.gpu_memory_used_mb === undefined || Number.isNaN(Number(run.gpu_memory_used_mb))
    ? null
    : Number(run.gpu_memory_used_mb);
  const gpuProcesses = Array.isArray(node.gpu_processes) ? node.gpu_processes : [];
  const matchedProcesses = Array.isArray(run.matched_processes) ? run.matched_processes : [];
  if (explicitIndices.length && explicitMemory !== null) {
    return { gpuIndices: explicitIndices, gpuMemoryUsedMb: explicitMemory };
  }

  const selected = new Map();
  const processesByPid = new Map();
  gpuProcesses.forEach((process) => {
    const pid = toInt(process.pid);
    if (pid === null) return;
    const bucket = processesByPid.get(pid) || [];
    bucket.push(process);
    processesByPid.set(pid, bucket);
  });

  matchedProcesses.forEach((process) => {
    const pid = toInt(process.pid);
    if (pid === null) return;
    (processesByPid.get(pid) || []).forEach((gpuProcess) => {
      selected.set(`${gpuProcess.gpu_uuid}:${gpuProcess.pid}:${gpuProcess.gpu_index}`, gpuProcess);
    });
  });

  matchedProcesses.forEach((process) => {
    const pid = toInt(process.pid);
    if (pid !== null && processesByPid.has(pid)) return;
    const cwd = String(process.cwd || "").trim();
    const signature = commandSignature(process.command);
    const elapsed = toInt(process.elapsed_seconds);
    if (!cwd || !signature) return;
    gpuProcesses.forEach((gpuProcess) => {
      if (String(gpuProcess.cwd || "").trim() !== cwd) return;
      if (commandSignature(gpuProcess.command) !== signature) return;
      const processElapsed = toInt(gpuProcess.elapsed_seconds);
      if (elapsed !== null && processElapsed !== null && Math.abs(processElapsed - elapsed) > 600) return;
      selected.set(`${gpuProcess.gpu_uuid}:${gpuProcess.pid}:${gpuProcess.gpu_index}`, gpuProcess);
    });
  });

  const derivedIndices = Array.from(
    new Set(
      Array.from(selected.values())
        .map((process) => toInt(process.gpu_index))
        .filter((item) => item !== null),
    ),
  ).sort((left, right) => left - right);
  const derivedMemory = Array.from(selected.values()).reduce((total, process) => {
    const memory = Number(process.used_gpu_memory_mb);
    return Number.isNaN(memory) ? total : total + memory;
  }, 0);

  return {
    gpuIndices: explicitIndices.length ? explicitIndices : derivedIndices,
    gpuMemoryUsedMb: explicitMemory !== null ? explicitMemory : (derivedMemory > 0 ? derivedMemory : null),
  };
}

function busyGpuIndicesForNode(node) {
  return new Set(
    (Array.isArray(node.gpus) ? node.gpus : [])
      .filter((gpu) => Boolean(gpu?.is_busy))
      .map((gpu) => toInt(gpu.index))
      .filter((index) => index !== null),
  );
}

export function renderSummaryCards(summaryEl, snapshot) {
  if (!summaryEl) return;
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
    ["外部排队", snapshot.summary.external_queue_total ?? 0],
  ];
  summaryEl.innerHTML = items.map(([label, value]) => `
    <article class="summary-item card">
      <span class="kicker">${safeText(label)}</span>
      <strong>${safeText(value ?? 0)}</strong>
    </article>
  `).join("");
}

export function renderEventsList(eventsEl, events = []) {
  if (!eventsEl) return;
  if (!events.length) {
    eventsEl.innerHTML = `<div class="event-item"><strong>当前没有活动告警</strong><p class="subtle">节点异常、训练失败或停滞后，会优先显示在这里。</p></div>`;
    return;
  }
  eventsEl.innerHTML = events.map((event) => `
    <article class="event-item">
      <strong class="${safeAttr(statusClass(event.status))}">${safeText(alertMessage(event))}</strong>
      <p class="subtle">${safeText(event.is_current ? "当前告警" : "最近变化")} · ${safeText(fmtDateTime(event.at))}</p>
    </article>
  `).join("");
}

function renderEmptyState(nodeListEl, onOpenConnect) {
  if (!nodeListEl) return;
  nodeListEl.innerHTML = `
    <section class="node-card">
      <h2>还没有连接任何真实机器</h2>
      <p class="subtle">点右上角“连接 SSH”，输入主机信息后，系统会立刻开始轮询真实服务器状态。即使不填日志路径，也会自动尝试发现正在跑的训练任务和可读取日志。也支持直接输入本机 <code>~/.ssh/config</code> 里的 SSH 别名。</p>
      <div class="note-row" style="margin-top:16px;">
        <button id="emptyConnectBtn" class="secondary-button primary-button">现在连接 SSH</button>
      </div>
      <div class="note-row">
        <span class="chip">支持密码登录</span>
        <span class="chip">支持私钥路径</span>
        <span class="chip">支持 SSH 别名 / ProxyJump</span>
        <span class="chip">支持 CPU / 内存 / 磁盘</span>
      </div>
    </section>
  `;
  nodeListEl.querySelector("#emptyConnectBtn")?.addEventListener("click", onOpenConnect);
}

export function renderNodesList({
  nodeListEl,
  snapshot,
  onOpenRunDetail,
  onRemoveConnection,
  onOpenConnect,
} = {}) {
  if (!nodeListEl) return;
  if (!snapshot.nodes.length) {
    renderEmptyState(nodeListEl, onOpenConnect);
    return;
  }

  nodeListEl.innerHTML = snapshot.nodes.map((node) => {
    const metrics = node.metrics || {};
    const busyGpuIndices = busyGpuIndicesForNode(node);
    const runPriority = {
      failed: 0,
      stalled: 1,
      running: 2,
      connecting: 3,
      starting: 4,
      unknown: 5,
      idle: 6,
      completed: 7,
    };
    const runUsageList = (node.runs || [])
      .map((run) => ({ run, usage: resolveRunGpuUsage(node, run) }))
      .sort((left, right) => {
        const leftPriority = runPriority[left.run.status] ?? 99;
        const rightPriority = runPriority[right.run.status] ?? 99;
        if (leftPriority !== rightPriority) return leftPriority - rightPriority;
        return String(left.run.label || "").localeCompare(String(right.run.label || ""), "zh-CN");
      });
    const activeRuns = runUsageList.filter(({ run }) => ["running", "stalled", "failed"].includes(run.status || ""));
    const runsByGpu = new Map();
    runUsageList.forEach(({ run, usage }) => {
      usage.gpuIndices.forEach((gpuIndex) => {
        const bucket = runsByGpu.get(gpuIndex) || [];
        bucket.push(run);
        runsByGpu.set(gpuIndex, bucket);
      });
    });
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
        <span class="kicker">${safeText(label)}</span>
        <strong>${safeText(value)}</strong>
      </div>
    `).join("");

    const gpuCards = (node.gpus || []).map((gpu) => {
      const occupyingRuns = runsByGpu.get(gpu.index) || [];
      const occupancyHtml = occupyingRuns.length
        ? `
          <div class="gpu-occupancy-list">
            ${occupyingRuns.map((run) => `
              <div class="gpu-occupancy-item">
                <div class="gpu-row">
                  <strong>${safeText(run.label)}</strong>
                  <span class="status-pill ${safeAttr(statusClass(run.status))}">${safeText(statusLabel(run.status))}</span>
                </div>
                <p class="subtle gpu-occupancy-task">${safeText(taskDisplay(run))}</p>
              </div>
            `).join("")}
          </div>
        `
        : `<p class="subtle gpu-occupancy-empty">当前没有识别到占用这个 GPU 的训练任务</p>`;
      return `
      <article class="gpu-card">
        <div class="gpu-row">
          <strong>${safeText(`GPU ${gpu.index}`)}</strong>
          <span class="chip">${safeText(gpu.name)}</span>
        </div>
        <div class="metric-grid">
          <div class="metric-card"><span class="kicker">利用率</span><strong>${safeText(`${fmtInt(gpu.utilization_gpu)}%`)}</strong></div>
          <div class="metric-card"><span class="kicker">显存</span><strong>${safeText(`${fmtInt(gpu.memory_used_mb)} / ${fmtInt(gpu.memory_total_mb)}`)}</strong></div>
          <div class="metric-card"><span class="kicker">温度</span><strong>${safeText(`${fmtInt(gpu.temperature_c)}°C`)}</strong></div>
          <div class="metric-card"><span class="kicker">功耗</span><strong>${safeText(`${fmtInt(gpu.power_draw_w)}W`)}</strong></div>
        </div>
        <div class="note-row">
          ${noteChip("任务数", occupyingRuns.length)}
          ${gpu.processes?.length ? noteChip("GPU 进程", gpu.processes.length) : ""}
        </div>
        ${occupancyHtml}
      </article>
    `;
    }).join("");

    const runCards = runUsageList.map(({ run, usage }) => {
      const runBusyGpuIndices = usage.gpuIndices.filter((gpuIndex) => busyGpuIndices.has(gpuIndex));
      return `
      <article class="run-card" data-node-id="${safeAttr(node.id)}" data-run-id="${safeAttr(run.id)}">
        <div class="run-row">
          <strong>${safeText(run.label)}</strong>
          <span class="status-pill ${safeAttr(statusClass(run.status))}">${safeText(statusLabel(run.status))}</span>
        </div>
        <p class="task-line"><span class="kicker">当前任务</span><code>${safeText(taskDisplay(run))}</code></p>
        <div class="metric-grid">
          <div class="metric-card"><span class="kicker">损失</span><strong>${safeText(fmtNumber(run.loss, 4))}</strong></div>
          <div class="metric-card"><span class="kicker">已运行</span><strong>${safeText(fmtDuration(run.elapsed_seconds))}</strong></div>
          <div class="metric-card"><span class="kicker">预计剩余</span><strong>${safeText(remainingDisplay(run))}</strong></div>
          <div class="metric-card"><span class="kicker">Step</span><strong>${safeText(`${run.step ?? "--"}${run.step_total ? ` / ${run.step_total}` : ""}`)}</strong></div>
          <div class="metric-card"><span class="kicker">最近更新</span><strong>${safeText(fmtDateTime(run.last_update_at))}</strong></div>
        </div>
        <div class="note-row">
          ${noteChip("解析器", run.parser)}
          ${noteChip("匹配进程", run.matched_processes?.length || 0)}
          ${usage.gpuIndices.length ? noteChip("归因 GPU", fmtGpuIndices(usage.gpuIndices)) : ""}
          ${runBusyGpuIndices.length ? noteChip("忙碌 GPU", fmtGpuIndices(runBusyGpuIndices)) : ""}
          ${usage.gpuMemoryUsedMb !== null ? noteChip("显存", fmtGbFromMb(usage.gpuMemoryUsedMb)) : ""}
          ${noteChip("进度", progressDisplay(run))}
          ${run.estimated_end_at ? noteChip("预计完成", fmtDateTime(run.estimated_end_at)) : ""}
          ${etaHint(run) ? noteChip("提示", etaHint(run)) : ""}
          ${run.error ? noteChip("异常", localizeMessage(run.error)) : ""}
        </div>
        <div class="log-tail">${safeText(latestLog(run))}</div>
      </article>
    `;
    }).join("");

    const busyGpuCountRaw = Number(metrics.gpus_busy);
    const busyGpuCount = Number.isNaN(busyGpuCountRaw) ? busyGpuIndices.size : Math.max(0, Math.round(busyGpuCountRaw));
    const overviewCards = [
      ["CPU", `${fmtNumber(metrics.cpu_usage_percent, 1)}%`],
      ["内存", `${fmtNumber(metrics.memory_used_percent, 1)}%`],
      [node.gpus?.length ? "忙碌 GPU" : "GPU", node.gpus?.length ? `${busyGpuCount}/${node.gpus.length}` : "--"],
      ["运行任务", String(runUsageList.length)],
    ].map(([label, value]) => `
      <div class="node-overview-card">
        <span class="kicker">${safeText(label)}</span>
        <strong>${safeText(value)}</strong>
      </div>
    `).join("");
    const metricsSection = renderFoldSection({
      title: "系统指标",
      meta: `${Object.keys(metrics).length} 项`,
      open: node.status !== "online",
      foldId: `node:${node.id}:metrics`,
      content: `<div class="metric-grid">${metricCards}</div>`,
    });
    const gpuSection = renderFoldSection({
      title: "GPU 详情",
      meta: node.gpus?.length ? `${busyGpuCount}/${node.gpus.length} 忙碌` : "暂无 GPU 数据",
      open: false,
      foldId: `node:${node.id}:gpus`,
      content: `<div class="gpu-grid">${gpuCards || '<article class="gpu-card"><p class="subtle">暂无 GPU 数据</p></article>'}</div>`,
    });
    const runsSection = renderFoldSection({
      title: "运行任务",
      meta: runUsageList.length ? `${runUsageList.length} 个任务` : "暂无任务",
      open: activeRuns.length > 0,
      foldId: `node:${node.id}:runs`,
      content: `<div class="run-grid">${runCards || '<article class="run-card"><p class="subtle">当前没有发现活跃训练任务。只连 SSH 也会自动尝试从 GPU 进程和 stdout/stderr 日志里发现任务；如果还没有显示，通常是机器上暂时没在训练，或训练输出没有落到可读取的日志文件。</p></article>'}</div>`,
    });

    return `
      <section id="node-${safeAttr(node.id)}" class="node-card">
        <div class="node-header">
          <div>
            <h2>${safeText(node.label)}</h2>
            <p class="subtle">${safeText(node.hostname || node.host)} · ${safeText(node.collected_at)}</p>
          </div>
          <div class="node-actions">
            <span class="status-pill ${safeAttr(statusClass(node.status))}">${safeText(statusLabel(node.status))}</span>
            <button class="secondary-button danger-button disconnect-button" data-remove-node-id="${safeAttr(node.id)}" data-remove-node-label="${safeAttr(node.label)}">移除连接</button>
            ${node.error ? `<p class="subtle connection-error">${safeText(localizeMessage(node.error))}</p>` : ""}
          </div>
        </div>
        <div class="node-overview-grid">${overviewCards}</div>
        <div class="note-row">
          ${noteChip("负载", `${fmtNumber(metrics.loadavg_1m, 2)} / ${fmtNumber(metrics.loadavg_5m, 2)} / ${fmtNumber(metrics.loadavg_15m, 2)}`)}
          ${noteChip("RAM", `${fmtGbFromMb(metrics.memory_used_mb)} / ${fmtGbFromMb(metrics.memory_total_mb)}`)}
          ${noteChip("磁盘", `${fmtGb(metrics.disk_used_gb)} / ${fmtGb(metrics.disk_total_gb)}`)}
          ${noteChip("Swap", `${fmtNumber(metrics.swap_used_percent, 1)}%`)}
          ${node.external_queue?.length ? noteChip("外部队列", node.external_queue.length) : ""}
          ${node.external_queue_source ? noteChip("来源", node.external_queue_source) : ""}
          ${node.gpus?.length ? noteChip("GPU", `${busyGpuCount}/${node.gpus.length}`) : ""}
          ${runUsageList.length ? noteChip("任务数", runUsageList.length) : ""}
        </div>
        ${node.external_queue_error ? `<div class="note-row external-source-note">${noteChip("队列错误", localizeMessage(node.external_queue_error))}</div>` : ""}
        ${metricsSection}
        ${gpuSection}
        ${runsSection}
      </section>
    `;
  }).join("");

  nodeListEl.querySelectorAll(".run-card[data-node-id]").forEach((element) => {
    element.addEventListener("click", () => {
      onOpenRunDetail?.(element.dataset.nodeId, element.dataset.runId);
    });
  });
  nodeListEl.querySelectorAll(".disconnect-button[data-remove-node-id]").forEach((element) => {
    element.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      onRemoveConnection?.(element.dataset.removeNodeId, element.dataset.removeNodeLabel);
    });
  });
}
