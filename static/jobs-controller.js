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

  function queueSummaryFromJobs(jobs) {
    const items = Array.isArray(jobs) ? jobs : [];
    const activeStatuses = new Set(["queued", "starting", "running"]);
    return {
      jobs_queued: items.filter((job) => job.status === "queued").length,
      jobs_starting: items.filter((job) => job.status === "starting").length,
      jobs_running: items.filter((job) => job.status === "running").length,
      jobs_failed: items.filter((job) => job.status === "failed").length,
      gpu_requested_active: items.reduce((total, job) => total + (activeStatuses.has(job.status) ? Number(job.gpu_count || 0) : 0), 0),
    };
  }

  function renderCurrentJobs() {
    renderJobsPanel({
      summary: state.jobsSummary,
      jobs: state.jobs,
      summaryEl: els.jobSummary,
      listEl: els.jobList,
      onCancel: cancelJob,
      cancelingJobIds: state.cancelingJobIds,
    });
  }

  function setJobCanceling(jobId, canceling) {
    if (!jobId) return;
    if (state.cancelingJobIds instanceof Set) {
      if (canceling) {
        state.cancelingJobIds.add(jobId);
      } else {
        state.cancelingJobIds.delete(jobId);
      }
    }
    renderCurrentJobs();
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
    renderCurrentJobs();
    renderExternalJobsPanel({
      summary: state.externalJobsSummary,
      jobs: state.externalJobs,
      summaryEl: els.externalJobSummary,
      listEl: els.externalJobList,
    });
  }

  async function cancelJob(jobId) {
    if (!jobId) return;
    if (state.cancelingJobIds instanceof Set && state.cancelingJobIds.has(jobId)) return;
    if (!window.confirm("\u786e\u8ba4\u53d6\u6d88\u8fd9\u4e2a\u6392\u961f\u4efb\u52a1\u5417\uff1f")) return;
    setJobCanceling(jobId, true);
    showBanner("\u6b63\u5728\u53d6\u6d88\u6392\u961f\u4efb\u52a1\uff0c\u53ef\u80fd\u9700\u8981\u51e0\u79d2...", "info");
    try {
      const payload = await guard(() => apiJson("DELETE", `/api/v1/jobs/${encodeURIComponent(jobId)}`));
      const item = payload?.item || null;
      if (item) {
        state.jobs = state.jobs.map((job) => (job.id === jobId ? item : job));
        state.jobsSummary = queueSummaryFromJobs(state.jobs);
        renderCurrentJobs();
      }
      showBanner("\u5df2\u53d6\u6d88\u6392\u961f\u4efb\u52a1", "info");
      loadJobs().catch(() => {});
    } catch (error) {
      showBanner(error.message || String(error), "error");
    } finally {
      setJobCanceling(jobId, false);
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
