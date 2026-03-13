export const WS_AUTH_CLOSE_CODE = 4401;
const DEFAULT_RECONNECT_DELAY_MS = 2500;

export function createSnapshotStream({
  getToken,
  onSnapshot,
  onError,
  reconnectDelayMs = DEFAULT_RECONNECT_DELAY_MS,
} = {}) {
  let socket = null;
  let reconnectHandle = null;

  function clearReconnect() {
    if (reconnectHandle) {
      window.clearTimeout(reconnectHandle);
      reconnectHandle = null;
    }
  }

  function disconnect() {
    clearReconnect();
    if (!socket) return;
    socket._manualClose = true;
    socket.close();
    socket = null;
  }

  function scheduleReconnect() {
    clearReconnect();
    reconnectHandle = window.setTimeout(() => {
      reconnectHandle = null;
      connect();
    }, reconnectDelayMs);
  }

  function connect() {
    disconnect();
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const nextSocket = new WebSocket(`${protocol}//${window.location.host}/api/v1/stream`);
    socket = nextSocket;

    nextSocket.onopen = () => {
      nextSocket.send(JSON.stringify({ type: "auth", token: String(getToken?.() || "") }));
    };

    nextSocket.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === "snapshot") {
          onSnapshot?.(payload);
        } else if (payload.type === "error") {
          onError?.(payload.error || "实时通道异常", { kind: "server", payload });
        }
      } catch (_error) {
        onError?.("实时通道消息解析失败", { kind: "parse" });
      }
    };

    nextSocket.onclose = (event) => {
      if (socket === nextSocket) {
        socket = null;
      }
      if (nextSocket._manualClose) {
        return;
      }
      if (event.code === WS_AUTH_CLOSE_CODE) {
        onError?.("实时通道鉴权失败，请检查令牌。", { kind: "auth", event });
        return;
      }
      scheduleReconnect();
    };
  }

  return { connect, disconnect };
}
