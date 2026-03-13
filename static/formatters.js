import { escapeHtml } from "./html.js";

export function statusClass(status) { return `status-${status || "unknown"}`; }
export function fmtNumber(value, digits = 2) { return value === null || value === undefined || Number.isNaN(Number(value)) ? "--" : Number(value).toFixed(digits); }
export function fmtInt(value) { return value === null || value === undefined || Number.isNaN(Number(value)) ? "--" : Math.round(Number(value)).toString(); }
export function fmtGb(value) { return value === null || value === undefined || Number.isNaN(Number(value)) ? "--" : `${Number(value).toFixed(1)} GB`; }
export function fmtGbFromMb(value) { return value === null || value === undefined || Number.isNaN(Number(value)) ? "--" : `${(Number(value) / 1024).toFixed(1)} GB`; }
export function fmtGpuIndices(indices) {
  if (!Array.isArray(indices) || !indices.length) return "--";
  return indices.map((index) => `GPU ${index}`).join(", ");
}
export function fmtDuration(seconds) {
  if (!seconds && seconds !== 0) return "--";
  const total = Math.max(0, Math.round(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}
export function fmtDateTime(value) {
  if (!value) return "--";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}
export function taskDisplay(run) { return run.task_name || run.task_command || run.label || "--"; }
export function remainingDisplay(run) {
  if (run.remaining_seconds !== null && run.remaining_seconds !== undefined) return fmtDuration(run.remaining_seconds);
  return run.eta || fmtDuration(run.eta_seconds);
}
export function progressDisplay(run) {
  return run.progress_percent === null || run.progress_percent === undefined ? "--" : `${fmtNumber(run.progress_percent, 1)}%`;
}
export function etaHint(run) {
  if (run.remaining_seconds !== null && run.remaining_seconds !== undefined) return "";
  if (run.eta_seconds !== null && run.eta_seconds !== undefined) return "";
  if (run.status !== "running" && run.status !== "stalled") return "";
  return run.log_path ? "日志里还没有 ETA / step_total" : "未找到可读日志，暂时无法估算";
}
export function latestLog(run) { return run.last_log_line || run.error || "暂无日志"; }
export function noteChip(label, value) {
  const safeValue = value === null || value === undefined || value === "" ? "--" : value;
  return `<span class="chip">${escapeHtml(label)}: ${escapeHtml(safeValue)}</span>`;
}
export function aliasDescription(item) {
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
