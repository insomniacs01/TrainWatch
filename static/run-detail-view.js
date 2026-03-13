import { escapeHtml } from "./html.js";
import {
  fmtDateTime,
  fmtDuration,
  fmtGbFromMb,
  fmtGpuIndices,
  fmtNumber,
  latestLog,
  localizeMessage,
  progressDisplay,
  remainingDisplay,
  statusLabel,
  taskDisplay,
} from "./formatters.js";

function safeText(value) {
  return escapeHtml(value ?? "");
}

export function renderProcessesList(containerEl, matchedProcesses = []) {
  if (!containerEl) return;
  if (!matchedProcesses.length) {
    containerEl.innerHTML = `<div class="process-item"><p class="subtle">暂无匹配到的进程</p></div>`;
    return;
  }
  containerEl.innerHTML = matchedProcesses.map((proc) => `
    <article class="process-item">
      <div class="process-row">
        <strong>${safeText(`PID ${proc.pid ?? "--"}`)}</strong>
        <span class="chip">${safeText(fmtDuration(proc.elapsed_seconds))}</span>
      </div>
      <code>${safeText(proc.command || "")}</code>
    </article>
  `).join("");
}

export function renderRunDetail({
  eyebrowEl,
  titleEl,
  metaEl,
  logEl,
  processesEl,
  drawerEl,
  node,
  run,
} = {}) {
  if (!node || !run) return;
  if (eyebrowEl) eyebrowEl.textContent = node.label || "";
  if (titleEl) titleEl.textContent = run.label || "";
  if (metaEl) {
    metaEl.innerHTML = [
      ["状态", statusLabel(run.status)],
      ["任务", taskDisplay(run)],
      ["PID", run.task_pid ?? "--"],
      ["已运行", fmtDuration(run.elapsed_seconds)],
      ["GPU", fmtGpuIndices(run.gpu_indices)],
      ["显存", fmtGbFromMb(run.gpu_memory_used_mb)],
      ["预计剩余", remainingDisplay(run)],
      ["预计完成", fmtDateTime(run.estimated_end_at)],
      ["开始时间", fmtDateTime(run.started_at)],
      ["进度", progressDisplay(run)],
      ["解析器", run.parser],
      ["损失", fmtNumber(run.loss, 4)],
      ["ETA", run.eta || fmtDuration(run.eta_seconds)],
      ["Step", `${run.step ?? "--"}${run.step_total ? ` / ${run.step_total}` : ""}`],
      ["日志", run.log_path || "--"],
      ["最近更新", fmtDateTime(run.last_update_at)],
      ["错误", localizeMessage(run.error) || "--"],
    ].map(([label, value]) => `
      <article class="card">
        <span class="kicker">${safeText(label)}</span>
        <strong>${safeText(value)}</strong>
      </article>
    `).join("");
  }
  if (logEl) {
    logEl.textContent = latestLog(run);
  }
  renderProcessesList(processesEl, run.matched_processes || []);
  drawerEl?.classList.remove("hidden");
}
