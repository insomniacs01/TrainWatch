import { escapeAttr, escapeHtml } from "./html.js";
import { fmtDateTime, noteChip, statusClass } from "./formatters.js";

function safeText(value) {
  return escapeHtml(value ?? "");
}

function safeAttr(value) {
  return escapeAttr(value ?? "");
}

export function renderJobsPanel({ summary = {}, jobs = [], summaryEl, listEl, onCancel } = {}) {
  if (summaryEl) {
    summaryEl.innerHTML = [
      noteChip("queued", summary.jobs_queued ?? 0),
      noteChip("starting", summary.jobs_starting ?? 0),
      noteChip("running", summary.jobs_running ?? 0),
      noteChip("failed", summary.jobs_failed ?? 0),
      noteChip("gpu", summary.gpu_requested_active ?? 0),
    ].join("");
  }
  if (!listEl) return;
  if (!jobs.length) {
    listEl.innerHTML = `<div class="job-item"><strong>还没有排队任务</strong><p class="subtle">选择一台已连接的 GPU 机器，填写命令和所需 GPU 数，就可以加入 FIFO 队列。</p></div>`;
    return;
  }
  listEl.innerHTML = jobs.map((job) => `
    <article class="job-item">
      <div class="run-row">
        <strong>${safeText(job.label)}</strong>
        <span class="status-pill ${safeAttr(statusClass(job.status))}">${safeText(job.status || "unknown")}</span>
      </div>
      <p class="subtle">${safeText(job.owner || "Anonymous")} · ${safeText(job.node_label || job.node_id)}</p>
      <div class="job-meta">
        <div class="note-row">
          ${noteChip("GPU", job.gpu_count)}
          ${job.queue_position ? noteChip("pos", job.queue_position) : ""}
          ${job.run_status ? noteChip("run", job.run_status) : ""}
          ${job.allocated_gpu_indices?.length ? noteChip("alloc", job.allocated_gpu_indices.join(",")) : ""}
        </div>
        <div class="note-row">
          ${noteChip("created", fmtDateTime(job.created_at))}
          ${job.started_at ? noteChip("started", fmtDateTime(job.started_at)) : ""}
          ${job.finished_at ? noteChip("finished", fmtDateTime(job.finished_at)) : ""}
        </div>
        ${job.workdir ? `<div class="note-row">${noteChip("cwd", job.workdir)}</div>` : ""}
        ${job.error ? `<div class="log-tail">${safeText(job.error)}</div>` : ""}
      </div>
      <code>${safeText(job.command || "")}</code>
      <div class="job-actions">
        ${job.can_cancel ? `<button class="secondary-button danger-button cancel-job-button" data-job-id="${safeAttr(job.id)}">取消排队</button>` : ""}
      </div>
    </article>
  `).join("");
  listEl.querySelectorAll(".cancel-job-button[data-job-id]").forEach((element) => {
    element.addEventListener("click", (event) => {
      event.preventDefault();
      onCancel?.(element.dataset.jobId);
    });
  });
}

export function renderExternalJobsPanel({ summary = {}, jobs = [], summaryEl, listEl } = {}) {
  if (summaryEl) {
    summaryEl.innerHTML = [
      noteChip("queued", summary.jobs_queued ?? 0),
      noteChip("starting", summary.jobs_starting ?? 0),
      noteChip("running", summary.jobs_running ?? 0),
      noteChip("failed", summary.jobs_failed ?? 0),
      noteChip("gpu", summary.gpu_requested_active ?? 0),
    ].join("");
  }
  if (!listEl) return;
  if (!jobs.length) {
    listEl.innerHTML = `<div class="job-item"><strong>暂无外部排队任务</strong><p class="subtle">如果远端暴露了 Slurm 或自定义 JSON 队列探针，任务会显示在这里。</p></div>`;
    return;
  }
  listEl.innerHTML = jobs.map((job) => `
    <article class="job-item">
      <div class="run-row">
        <strong>${safeText(job.label || job.id || "External Job")}</strong>
        <span class="status-pill ${safeAttr(statusClass(job.status))}">${safeText(job.status || "unknown")}</span>
      </div>
      <p class="subtle">${safeText(job.owner || "Unknown")} · ${safeText(job.node_label || job.node_id)} · ${safeText(job.source || "external")}</p>
      <div class="job-meta">
        <div class="note-row">
          ${noteChip("GPU", job.gpu_count ?? "--")}
          ${job.submitted_at ? noteChip("submitted", fmtDateTime(job.submitted_at)) : ""}
          ${job.raw_status ? noteChip("raw", job.raw_status) : ""}
        </div>
        <div class="note-row">
          ${job.workdir ? noteChip("cwd", job.workdir) : ""}
          ${job.reason ? noteChip("reason", job.reason) : ""}
        </div>
      </div>
      ${job.command ? `<code>${safeText(job.command)}</code>` : ""}
    </article>
  `).join("");
}
