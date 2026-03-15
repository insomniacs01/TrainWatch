from fastapi import APIRouter, WebSocket
from starlette.websockets import WebSocketDisconnect

from .deps import authenticate_websocket


router = APIRouter()


@router.websocket("/api/v1/stream")
async def stream(websocket: WebSocket) -> None:
    runtime = websocket.app.state.runtime
    await websocket.accept()
    principal = await authenticate_websocket(websocket, runtime)
    if principal is None:
        return
    await runtime.hub.connect(websocket, already_accepted=True)
    await websocket.send_json({"type": "snapshot", "snapshot": runtime.snapshot_dict(), "events": [], "user": principal.to_dict()})
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        runtime.hub.disconnect(websocket)
