import asyncio
import uuid
from datetime import datetime, timezone

from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    HTTPException,
)
from pydantic import BaseModel, Field

from backend.connection_manager import ConnectionManager


app = FastAPI(title="RemoteControlSystem")

manager = ConnectionManager()

# request_id -> device, action, future
pending_actions: dict[str, dict] = {}

# Completed results
action_results: dict[str, dict] = {}


class ActionRequest(BaseModel):
    action: str = Field(min_length=1, max_length=2000)


@app.get("/")
async def home():
    return {
        "status": "online",
        "message": "RemoteControlSystem is running",
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/devices")
async def get_devices():
    return {
        "connected_devices": manager.get_connected_devices()
    }


@app.post("/devices/{device_id}/actions")
async def send_action(
    device_id: str,
    request: ActionRequest,
):
    action = request.action

    if not manager.is_connected(device_id):
        raise HTTPException(
            status_code=404,
            detail=f"Device {device_id} is not connected",
        )

    request_id = str(uuid.uuid4())
    future = asyncio.get_running_loop().create_future()

    pending_actions[request_id] = {
        "device_id": device_id,
        "action": action,
        "future": future,
    }

    message = {
        "type": "run_approved_action",
        "request_id": request_id,
        "action": action,
    }

    try:
        sent = await manager.send_to_device(
            device_id,
            message,
        )

        if not sent:
            raise HTTPException(
                status_code=503,
                detail="Could not send action to the agent",
            )

        print(
            f"[WAITING] {request_id} | "
            f"device={device_id} | action={action}"
        )

        try:
            # Wait for the agent's matching result.
            result = await asyncio.wait_for(
                future,
                timeout=20,
            )
        except asyncio.TimeoutError:
            print(f"[TIMEOUT] No result for {request_id}")

            raise HTTPException(
                status_code=504,
                detail={
                    "error": "Agent did not return a result in time",
                    "request_id": request_id,
                    "device_id": device_id,
                    "action": action,
                    "hint": (
                        "Check the agent terminal for "
                        "'Received action' and 'Result sent'."
                    ),
                },
            )

        return {
            "status": "completed",
            "device_id": device_id,
            "request_id": request_id,
            "action": action,
            "result": result,
        }

    finally:
        pending_actions.pop(request_id, None)


@app.get("/actions/{request_id}")
async def get_action_result(request_id: str):
    result = action_results.get(request_id)

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Result not found",
        )

    return result


@app.websocket("/ws/{device_id}")
async def device_websocket(
    websocket: WebSocket,
    device_id: str,
):
    # Local development only; add authentication before deployment.
    connected = await manager.connect(device_id, websocket)

    if not connected:
        return

    try:
        while True:
            message = await websocket.receive_json()

            if not isinstance(message, dict):
                continue

            message_type = message.get("type")

            if message_type == "ping":
                await websocket.send_json({
                    "type": "pong",
                    "device_id": device_id,
                })
                continue

            if message_type != "action_result":
                print(
                    f"[IGNORED] Unexpected message from "
                    f"{device_id}: {message_type}"
                )
                continue

            request_id = message.get("request_id")
            action = message.get("action")

            if not isinstance(request_id, str):
                continue

            pending = pending_actions.get(request_id)

            # Verify this result belongs to this device and action.
            if pending is None:
                print(f"[IGNORED] Unknown request: {request_id}")
                continue

            if pending["device_id"] != device_id:
                print(f"[IGNORED] Wrong device: {request_id}")
                continue

            if pending["action"] != action:
                print(f"[IGNORED] Wrong action: {request_id}")
                continue

            future = pending["future"]

            if future.done():
                continue

            result = {
                "device_id": device_id,
                "request_id": request_id,
                "action": action,
                "result": message.get("result"),
                "received_at": datetime.now(
                    timezone.utc
                ).isoformat(),
            }

            action_results[request_id] = result
            future.set_result(result)

            print(f"[RESULT RECEIVED] {request_id}")

    except WebSocketDisconnect:
        print(f"[WEBSOCKET CLOSED] {device_id}")

    except Exception as error:
        print(f"[WEBSOCKET ERROR] {device_id}: {error}")

    finally:
        await manager.disconnect(device_id, websocket)