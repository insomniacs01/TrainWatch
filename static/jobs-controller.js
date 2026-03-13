import { escapeAttr, escapeHtml } from "./html.js";
import { statusLabel } from "./formatters.js";

function safeText(value) {
  return escapeHtml(value ?? "");
}

function safeAttr(value) {
  return escapeAttr(value ?? "");
}

export function createJobsController({
  state,
  els,
  apiGet,
  apiJson,
  showBanner,
  renderJobsPanel,
  renderExternalJobsPanel,
  withAuthRecovery = null,
} = {}) {
  const guard = withAuthRecovery || (async (task) => task());

  function renderConnectionOptions() {
    if (!els.jobNodeSelect) return;
    const items = state.connections || [];
    const previousValue = els.jobNodeSelect.value;
    els.jobNodeSelect.innerHTML = items.length
      ? items.map((item) => {
        const summary = `${item.label} · ${statusLabel(item.status)}${item.jobs ? ` · 队列 ${item.jobs}` : ""}`;
        return `<option value="${safeAttr(item.id)}">${safeText(summary)}</option>`;
      }).join("")
      : `<option value="">请先连接 SSH 机器</option>`;
    if (items.some((item) => item.id === previousValue)) {
      els.jobNodeSelect.value = previousValue;
    }
  }

  function setJobSubmitting(submitting) {
    state.jobSubmitting = submitting;
    if (els.submitJobBtn) {
      els.submitJobBtn.disabled = submitting;
      els.submitJobBtn.textContent = submitting ? "加入中..." : "加入排队";
    }
  }

  async function loadConnections() {
    const payload = await guard(() => apiGet("/api/v1/connections"));
    state.connections = payload.items || [];
    renderConnectionOptions();
  }

  async function loadJobs() {
    const payload = await guard(() => apiGet("/api/v1/jobs"));
    state.jobs = payload.items || [];
    state.jobsSummary = payload.summary || {};
    state.externalJobs = payload.external_items || [];
    state.externalJobsSummary = payload.external_summary || {};
    renderJobsPanel({
      summary: state.jobsSummary,
      jobs: state.jobs,
      summaryEl: els.jobSummary,
      listEl: els.jobList,
      onCancel: cancelJob,
    });
    renderExternalJobsPanel({
      summary: state.externalJobsSummary,
      jobs: state.externalJobs,
      summaryEl: els.externalJobSummary,
      listEl: els.externalJobList,
    });
  }

  async function cancelJob(jobId) {
    if (!jobId) return;
    if (!window.confirm("确认取消这个排队任务吗？")) return;
    try {
      await guard(() => apiJson("DELETE", `/api/v1/jobs/${encodeURIComponent(jobId)}`));
      await loadJobs();
      showBanner("已取消排队任务", "info");
    } catch (error) {
      showBanner(error.message || String(error), "error");
    }
  }

  async function submitJob(event) {
    event.preventDefault();
    if (state.jobSubmitting) return;
    if (!state.connections.length) {
      showBanner("请先连接至少一台 SSH 机器", "error");
      return;
    }
    const form = new FormData(els.jobForm);
    const nodeId = String(form.get("node_id") || "").trim();
    const command = String(form.get("command") || "").trim();
    if (!nodeId) {
      showBanner("请选择目标连接", "error");
      return;
    }
    if (!command) {
      showBanner("启动命令不能为空", "error");
      return;
    }
    setJobSubmitting(true);
    try {
      await guard(() => apiJson("POST", "/api/v1/jobs", {
        node_id: nodeId,
        owner: String(form.get("owner") || "").trim() || "匿名",
        label: String(form.get("label") || "").trim() || null,
        command,
        gpu_count: Number(form.get("gpu_count") || 1),
        workdir: String(form.get("workdir") || "").trim(),
        parser: String(form.get("parser") || "auto"),
      }));
      els.jobForm.reset();
      const gpuCountInput = document.getElementById("jobGpuCountInput");
      if (gpuCountInput) gpuCountInput.value = 1;
      renderConnectionOptions();
      await loadJobs();
      showBanner("任务已加入队列，系统会按先来先服务在 GPU 空出后自动启动。", "info");
    } catch (error) {
      showBanner(error.message || String(error), "error");
    } finally {
      setJobSubmitting(false);
    }
  }

  return {
    cancelJob,
    loadConnections,
    loadJobs,
    renderConnectionOptions,
    setJobSubmitting,
    submitJob,
  };
}
