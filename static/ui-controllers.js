export function createBannerController({ bannerEl } = {}) {
  function showBanner(text, tone = "error", { kind = "general" } = {}) {
    if (!bannerEl) return;
    bannerEl.textContent = text;
    bannerEl.dataset.kind = kind;
    bannerEl.classList.remove("hidden");
    bannerEl.style.borderColor = tone === "info" ? "rgba(56, 189, 248, 0.35)" : "rgba(239, 68, 68, 0.35)";
    bannerEl.style.background = tone === "info" ? "rgba(12, 74, 110, 0.35)" : "rgba(127, 29, 29, 0.35)";
  }

  function clearBanner(kind = null) {
    if (!bannerEl) return;
    if (kind && bannerEl.dataset.kind !== kind) return;
    bannerEl.classList.add("hidden");
    bannerEl.dataset.kind = "";
  }

  return { showBanner, clearBanner };
}


export function createNavigationController({ jumpSelect, statusLabel } = {}) {
  function renderJumpOptions(snapshot) {
    if (!jumpSelect) return;
    const previousValue = jumpSelect.value;
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

    jumpSelect.replaceChildren(fragment);
    jumpSelect.value = options.some((item) => item.value === previousValue) ? previousValue : "";
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
    if (!jumpSelect) return;
    window.requestAnimationFrame(() => {
      jumpSelect.value = "";
    });
  }

  return { renderJumpOptions, jumpToTarget, resetJumpSelection };
}


export function createDrawerController({ state, connectDrawerEl, detailDrawerEl, hostInputEl, syncOverlayState } = {}) {
  function openConnectDrawer() {
    connectDrawerEl?.classList.remove("hidden");
    connectDrawerEl?.scrollIntoView({ behavior: "smooth", block: "start" });
    window.setTimeout(() => hostInputEl?.focus(), 50);
  }

  function closeConnectDrawer() {
    connectDrawerEl?.classList.add("hidden");
  }

  function closeDetailDrawer() {
    if (state) {
      state.detailRequestId += 1;
    }
    detailDrawerEl?.classList.add("hidden");
    syncOverlayState?.();
  }

  return { openConnectDrawer, closeConnectDrawer, closeDetailDrawer };
}


export function createRunDetailController({
  state,
  els,
  drawChart,
  renderRunDetail,
  apiGet,
  showBanner,
  withAuthRecovery,
  syncOverlayState,
} = {}) {
  const detailCharts = [
    { canvas: () => els.lossChart, color: "#38bdf8", label: "Loss" },
    { canvas: () => els.gpuChart, color: "#22c55e", label: "GPU Utilization" },
    { canvas: () => els.memChart, color: "#f59e0b", label: "Memory MB" },
    { canvas: () => els.tempChart, color: "#ef4444", label: "Temperature °C" },
  ];

  function resetDetailCharts() {
    detailCharts.forEach(({ canvas, color, label }) => {
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
    syncOverlayState?.();
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
      drawChart(els.lossChart, queries[0].points || [], "#38bdf8", "Loss");
      drawChart(els.gpuChart, queries[1].points || [], "#22c55e", "GPU Utilization");
      drawChart(els.memChart, queries[2].points || [], "#f59e0b", "Memory MB");
      drawChart(els.tempChart, queries[3].points || [], "#ef4444", "Temperature °C");
    } catch (error) {
      if (requestId !== state.detailRequestId) return;
      resetDetailCharts();
      showBanner(error.message || String(error), "error");
    }
  }

  return { openRunDetail, resetDetailCharts };
}


export async function setupServiceWorker() {
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
    }
    return;
  }
  navigator.serviceWorker.register("/service-worker.js").catch(() => {});
}
