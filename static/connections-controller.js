import { escapeAttr, escapeHtml } from "./html.js";
import { aliasDescription } from "./formatters.js";

function safeText(value) {
  return escapeHtml(value ?? "");
}

function safeAttr(value) {
  return escapeAttr(value ?? "");
}

export function createConnectionsController({
  state,
  els,
  apiGet,
  apiJson,
  showBanner,
  closeConnectDrawer,
  onConnectionsChanged,
  refreshAll,
  withAuthRecovery = null,
} = {}) {
  const guard = withAuthRecovery || (async (task) => task());

  function renderAliasOptions() {
    if (!els.aliasSelect || !els.aliasMeta) return;
    const aliases = state.sshAliases || [];
    const previousValue = els.aliasSelect.value;
    els.aliasSelect.innerHTML = [`<option value="">选择一个 SSH 别名（可选）</option>`]
      .concat(aliases.map((item) => `<option value="${safeAttr(item.alias)}">${safeText(item.alias)}</option>`))
      .join("");
    if (aliases.some((item) => item.alias === previousValue)) {
      els.aliasSelect.value = previousValue;
    }
    const selected = aliases.find((item) => item.alias === els.aliasSelect.value);
    if (!aliases.length) {
      els.aliasMeta.textContent = "当前环境里没有发现可用的 SSH 别名；你仍然可以手动输入主机、用户和密码。";
      return;
    }
    els.aliasMeta.textContent = aliasDescription(selected || aliases[0]);
  }

  function setConnectSubmitting(submitting) {
    state.connectSubmitting = submitting;
    if (els.submitConnectBtn) {
      els.submitConnectBtn.disabled = submitting;
      els.submitConnectBtn.textContent = submitting ? "连接中..." : "开始连接并监控";
    }
  }

  async function loadSshAliases(showSuccessBanner = false) {
    try {
      const payload = await guard(() => apiGet("/api/v1/ssh-aliases"));
      state.sshAliases = payload.items || [];
      renderAliasOptions();
      if (showSuccessBanner) {
        showBanner(`已加载 ${state.sshAliases.length} 个 SSH 别名`, "info");
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
      showBanner("请先选择一个 SSH 别名", "error");
      return;
    }
    document.getElementById("hostInput").value = item.alias;
    document.getElementById("userInput").value = item.user || "";
    document.getElementById("portInput").value = item.port || 22;
    document.getElementById("keyPathInput").value = item.identityfile || "";
    document.getElementById("labelInput").value = document.getElementById("labelInput").value || item.alias;
    document.getElementById("passwordInput").value = "";
    els.aliasMeta.textContent = aliasDescription(item);
    showBanner(`已把 ${item.alias} 填入基础信息`, "info");
  }

  async function removeConnection(nodeId, label) {
    if (!nodeId) return;
    if (!window.confirm(`确认移除 ${label || nodeId} 吗？`)) return;
    try {
      await guard(() => apiJson("DELETE", `/api/v1/connections/${encodeURIComponent(nodeId)}`));
      showBanner(`已移除 ${label || nodeId}`, "info");
      await refreshAll?.();
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
    if (processMatch && !logPath && !logGlob) {
      showBanner("填写进程匹配规则时，请同时提供日志路径或日志通配路径。", "error");
      setConnectSubmitting(false);
      return;
    }
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
      queue_probe_command: String(form.get("queue_probe_command") || "").trim() || null,
      runs,
    };
    if (!payload.host) {
      showBanner("主机不能为空", "error");
      setConnectSubmitting(false);
      return;
    }
    try {
      await guard(() => apiJson("POST", "/api/v1/connections", payload));
      closeConnectDrawer?.();
      els.connectForm.reset();
      document.getElementById("portInput").value = 22;
      document.getElementById("stallInput").value = 900;
      renderAliasOptions();
      showBanner(`已开始连接 ${payload.host}，正在后台采集首轮状态。`, "info");
      await onConnectionsChanged?.();
    } catch (error) {
      showBanner(error.message || String(error), "error");
    } finally {
      setConnectSubmitting(false);
    }
  }

  return {
    applySelectedAlias,
    loadSshAliases,
    removeConnection,
    renderAliasOptions,
    setConnectSubmitting,
    submitConnection,
  };
}
