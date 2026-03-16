export function createAlertsController({
  state,
  els,
  showBanner,
  renderEventsList,
  alertMessage,
  localizeMessage,
  statusLabel,
} = {}) {
  function alertIdentity(item) {
    if (!item) return "";
    const stableId = String(item.id || item.dedupe_key || "").trim();
    if (stableId) return stableId;
    if (item.run_id) return `run:${item.kind || ""}:${item.node_id}:${item.run_id}:${item.status}`;
    return `node:${item.kind || ""}:${item.node_id}:${item.status}:${item.message || ""}`;
  }

  function dedupeAlerts(items = []) {
    const merged = [];
    const seen = new Set();
    items.forEach((item) => {
      const key = alertIdentity(item);
      if (!key || seen.has(key)) return;
      seen.add(key);
      merged.push(item);
    });
    return merged;
  }

  function unreadAlertCount() {
    return state.unreadAlertKeys instanceof Set ? state.unreadAlertKeys.size : Number(state.unreadAlerts || 0);
  }

  function updateAlertBadge() {
    const currentCount = dedupeAlerts(state.currentAlerts).length;
    const badgeCount = currentCount > 0 ? currentCount : unreadAlertCount();
    els.alertBadge.textContent = String(badgeCount);
    els.alertBadge.classList.toggle("hidden", badgeCount <= 0);
  }

  function playAlertTone() {
    try {
      const audioContext = new (window.AudioContext || window.webkitAudioContext)();
      const oscillator = audioContext.createOscillator();
      const gain = audioContext.createGain();
      oscillator.connect(gain);
      gain.connect(audioContext.destination);
      oscillator.type = "sine";
      oscillator.frequency.value = 880;
      gain.gain.value = 0.03;
      oscillator.start();
      oscillator.stop(audioContext.currentTime + 0.15);
    } catch (_error) {}
  }

  function buildCurrentAlerts(snapshot) {
    if (!snapshot) return [];
    if (Array.isArray(snapshot.current_alerts) && snapshot.current_alerts.length) {
      return snapshot.current_alerts.slice(0, 20);
    }
    const items = [];
    const sortOrder = { failed: 0, offline: 1, stalled: 2, degraded: 3 };
    (snapshot.nodes || []).forEach((node) => {
      const runs = Array.isArray(node.runs) ? node.runs : [];
      const runAlerts = runs.filter((run) => ["failed", "stalled"].includes(run.status));
      runAlerts.forEach((run) => {
        items.push({
          kind: "current_run_alert",
          is_current: true,
          node_id: node.id,
          node_label: node.label,
          run_id: run.id,
          run_label: run.label,
          status: run.status,
          at: run.last_update_at || node.collected_at || snapshot.generated_at,
          message: `${node.label} / ${run.label}: ${statusLabel(run.status)}${run.error ? ` 路 ${localizeMessage(run.error)}` : ""}`,
        });
      });

      const needsNodeAlert = node.status === "offline" || (node.status === "degraded" && !runAlerts.length && node.error);
      if (!needsNodeAlert) return;
      items.push({
        kind: "current_node_alert",
        is_current: true,
        node_id: node.id,
        node_label: node.label,
        run_id: "",
        run_label: "",
        status: node.status,
        at: node.collected_at || snapshot.generated_at,
        message: `${node.label}: ${statusLabel(node.status)}${node.error ? ` 路 ${localizeMessage(node.error)}` : ""}`,
      });
    });

    return items
      .sort((left, right) => {
        const leftPriority = sortOrder[left.status] ?? 99;
        const rightPriority = sortOrder[right.status] ?? 99;
        if (leftPriority !== rightPriority) return leftPriority - rightPriority;
        return String(right.at || "").localeCompare(String(left.at || ""));
      })
      .slice(0, 20);
  }

  function syncAlertFeed(snapshot, fallbackEvents = []) {
    const recentEvents = Array.isArray(snapshot?.recent_events) ? snapshot.recent_events : [];
    state.recentEvents = dedupeAlerts(recentEvents.length ? recentEvents : (fallbackEvents.length ? fallbackEvents.slice(0, 20) : [])).slice(0, 20);
    state.currentAlerts = dedupeAlerts(buildCurrentAlerts(snapshot)).slice(0, 20);
    state.alertFeed = dedupeAlerts(state.currentAlerts.concat(state.recentEvents)).slice(0, 20);
  }

  function renderAlertFeed() {
    renderEventsList(els.eventsList, state.alertFeed);
    updateAlertBadge();
  }

  function handleIncomingEvents(events) {
    if (!events || !events.length) {
      updateAlertBadge();
      return;
    }
    const alerting = dedupeAlerts(events.filter((event) => ["completed", "failed", "stalled"].includes(event.status)));
    if (!alerting.length) {
      updateAlertBadge();
      return;
    }
    if (!(state.unreadAlertKeys instanceof Set)) {
      state.unreadAlertKeys = new Set();
    }
    alerting.forEach((event) => {
      const key = alertIdentity(event);
      if (key) {
        state.unreadAlertKeys.add(key);
      }
    });
    state.unreadAlerts = state.unreadAlertKeys.size;
    updateAlertBadge();
    showBanner(alertMessage(alerting[0]), alerting[0].status === "completed" ? "info" : "error", { kind: "event" });
    playAlertTone();
  }

  return {
    handleIncomingEvents,
    renderAlertFeed,
    syncAlertFeed,
  };
}
