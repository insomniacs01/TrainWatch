import { escapeHtml } from "./html.js";

const STATUS_LABELS = {
  connecting: "连接中",
  starting: "启动中",
  queued: "排队中",
  online: "在线",
  running: "运行中",
  completed: "已完成",
  degraded: "异常",
  stalled: "停滞",
  idle: "空闲",
  offline: "离线",
  failed: "失败",
  unknown: "未知",
  canceled: "已取消",
};

const EXACT_MESSAGE_LABELS = {
  "Waiting for first SSH poll": "等待首次 SSH 采集",
  "SSH password was not persisted. Re-enter the password or set server.persist_passwords=true.": "SSH 密码未持久化，请重新输入密码或开启 server.persist_passwords。",
  "Completion pattern matched and process exited": "已匹配到完成标记，且进程已经退出。",
  "Error pattern matched in training log": "训练日志里匹配到了错误标记。",
  "Log is stale while matching process is still alive": "日志已经停止更新，但匹配到的进程仍在运行。",
  "No matching training process found": "没有匹配到训练进程。",
  "Log exists but no active process matched": "找到了日志，但没有匹配到活跃进程。",
  "Log file not found": "未找到日志文件。",
  "Waiting for first poll after launch": "任务已启动，正在等待首次采集。",
  "Queued job did not appear in monitoring within the startup timeout": "任务启动后超时，监控里仍然没有出现。",
  "Queued job failed": "队列任务运行失败。",
  "Queued job exited without a completion marker": "队列任务已退出，但没有检测到完成标记。",
  "Queued job became unreachable during startup": "队列任务在启动阶段失去连接。",
  "Canceled before launch": "任务在启动前已取消。",
  "Connection removed before queued job could finish": "连接已移除，队列任务未能执行完成。",
  "Target connection not found": "目标连接不存在。",
  "Connection not found": "连接不存在。",
  "Authentication token required": "需要提供访问令牌。",
  "Invalid token": "令牌无效。",
  "command is required": "启动命令不能为空。",
  "host is required": "主机不能为空。",
  "run.log_path or run.log_glob is required when adding a run": "填写训练识别规则时，必须提供日志路径或日志通配路径。",
  "user is required unless host is a local SSH config alias": "如果填写的不是本机 SSH 别名，就必须提供用户。",
};

const PREFIX_MESSAGE_LABELS = [
  ["Connection already exists:", "连接已存在："],
  ["parser must be one of:", "解析器必须是以下之一："],
  ["password auth is not supported for local SSH config aliases;", "本机 SSH 别名不支持密码认证；"],
  ["Target connection was not found", "目标连接不存在"],
  ["Only queued jobs can be canceled right now", "目前只有排队中的任务可以取消"],
  ["Queued jobs currently require an SSH connection", "队列任务当前只能提交到 SSH 连接"],
  ["Requested GPU count exceeds the GPUs visible on this node", "申请的 GPU 数量超过了节点当前可见的 GPU 数量"],
];

export function statusClass(status) { return `status-${status || "unknown"}`; }
export function statusLabel(status) { return STATUS_LABELS[status] || status || "未知"; }
export function localizeMessage(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (EXACT_MESSAGE_LABELS[text]) {
    return EXACT_MESSAGE_LABELS[text];
  }
  for (const [prefix, replacement] of PREFIX_MESSAGE_LABELS) {
    if (text.startsWith(prefix)) {
      return `${replacement}${text.slice(prefix.length)}`;
    }
  }
  return text;
}
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
export function alertMessage(event) {
  if (!event) return "--";
  if (event.kind === "run_status_changed" && (event.node_label || event.run_label)) {
    return `${event.node_label || "节点"} / ${event.run_label || "任务"}: ${statusLabel(event.previous_status)} -> ${statusLabel(event.status)}`;
  }
  return localizeMessage(event.message || "--");
}
export function noteChip(label, value) {
  const safeValue = value === null || value === undefined || value === "" ? "--" : value;
  return `<span class="chip">${escapeHtml(label)}: ${escapeHtml(safeValue)}</span>`;
}
export function aliasDescription(item) {
  if (!item) return "还没有选择 SSH alias。";
  const parts = [
    item.hostname ? `主机 ${item.hostname}` : "",
    item.user ? `用户 ${item.user}` : "",
    item.port ? `端口 ${item.port}` : "",
    item.proxyjump ? `跳板 ${item.proxyjump}` : "",
    item.identityfile ? `密钥 ${item.identityfile}` : "",
  ].filter(Boolean);
  return parts.length ? parts.join(" · ") : "这个 SSH alias 没有额外字段，会直接按 ssh 配置解析。";
}
