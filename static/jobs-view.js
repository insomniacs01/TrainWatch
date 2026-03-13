import { escapeAttr, escapeHtml } from "./html.js";
import { fmtDateTime, localizeMessage, noteChip, statusClass, statusLabel } from "./formatters.js";

function safeText(value) {
  return escapeHtml(value ?? "");
}

function safeAttr(value) {
  return escapeAttr(value ?? "");
}

export function renderJobsPanel({ summary = {}, jobs = [], summaryEl, listEl, onCancel } = {}) {
  if (summaryEl) {
    summaryEl.innerHTML = [
      noteChip("排队中", summary.jobs_queued ?? 0),
      noteChip("启动中", summary.jobs_starting ?? 0),
      noteChip("运行中", summary.jobs_running ?? 0),
      noteChip("失败", summary.jobs_failed ?? 0),
      noteChip("申请 GPU", summary.gpu_requested_active ?? 0),
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
        <span class="status-pill ${safeAttr(statusClass(job.status))}">${safeText(statusLabel(job.status))}</span>
      </div>
      <p class="subtle">${safeText(job.owner || "匿名")} · ${safeText(job.node_label || job.node_id)}</p>
      <div class="job-meta">
        <div class="note-row">
          ${noteChip("GPU 数", job.gpu_count)}
          ${job.queue_position ? noteChip("排队顺位", job.queue_position) : ""}
          ${job.run_status ? noteChip("运行状态", statusLabel(job.run_status)) : ""}
          ${job.allocated_gpu_indices?.length ? noteChip("分配 GPU", job.allocated_gpu_indices.join(",")) : ""}
        </div>
        <div class="note-row">
          ${noteChip("提交时间", fmtDateTime(job.created_at))}
          ${job.started_at ? noteChip("开始时间", fmtDateTime(job.started_at)) : ""}
          ${job.finished_at ? noteChip("结束时间", fmtDateTime(job.finished_at)) : ""}
        </div>
        ${job.workdir ? `<div class="note-row">${noteChip("工作目录", job.workdir)}</div>` : ""}
        ${job.error ? `<div class="log-tail">${safeText(localizeMessage(job.error))}</div>` : ""}
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
      noteChip("排队中", summary.jobs_queued ?? 0),
      noteChip("启动中", summary.jobs_starting ?? 0),
      noteChip("运行中", summary.jobs_running ?? 0),
      noteChip("失败", summary.jobs_failed ?? 0),
      noteChip("申请 GPU", summary.gpu_requested_active ?? 0),
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
        <strong>${safeText(job.label || job.id || "外部任务")}</strong>
        <span class="status-pill ${safeAttr(statusClass(job.status))}">${safeText(statusLabel(job.status))}</span>
      </div>
      <p class="subtle">${safeText(job.owner || "未知用户")} · ${safeText(job.node_label || job.node_id)} · ${safeText(job.source || "外部来源")}</p>
      <div class="job-meta">
        <div class="note-row">
          ${noteChip("GPU 数", job.gpu_count ?? "--")}
          ${job.submitted_at ? noteChip("提交时间", fmtDateTime(job.submitted_at)) : ""}
          ${job.raw_status ? noteChip("原始状态", job.raw_status) : ""}
        </div>
        <div class="note-row">
          ${job.workdir ? noteChip("工作目录", job.workdir) : ""}
          ${job.reason ? noteChip("原因", job.reason) : ""}
        </div>
      </div>
      ${job.command ? `<code>${safeText(job.command)}</code>` : ""}
    </article>
  `).join("");
}
