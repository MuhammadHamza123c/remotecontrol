import asyncio
import json
import os
import subprocess

import websockets
from dotenv import load_dotenv

load_dotenv()

SERVER_URL = os.getenv(
    "SERVER_WS_URL",
    "ws://127.0.0.1:8000",
).rstrip("/")

DEVICE_ID = os.getenv("DEVICE_ID", "C1")

def run_approved_action(action: str) -> dict:
    """Execute the requested command on this machine."""

    try:
        completed = subprocess.run(
            action,
            capture_output=True,
            text=True,
            timeout=10,
            shell=True,
            check=False,
        )

        return {
            "status": (
                "success"
                if completed.returncode == 0
                else "error"
            ),
            "return_code": completed.returncode,
            "stdout": completed.stdout[:10000],
            "stderr": completed.stderr[:3000],
        }

    except subprocess.TimeoutExpired:
        return {
            "status": "error",
            "message": "Command timed out",
        }

    except OSError as error:
        return {
            "status": "error",
            "message": str(error),
        }


async def handle_message(websocket, message: dict) -> None:
    """Process messages from FastAPI."""

    message_type = message.get("type")

    if message_type == "ping":
        await websocket.send(json.dumps({
            "type": "pong",
            "device_id": DEVICE_ID,
        }))
        return

    if message_type != "run_approved_action":
        print(f"[IGNORED] Message type: {message_type}")
        return

    request_id = message.get("request_id")
    action = message.get("action")

    if not isinstance(request_id, str) or not request_id:
        print("[ERROR] Invalid request ID")
        return

    if not isinstance(action, str):
        print("[ERROR] Invalid action")
        return

    print(f"[RECEIVED ACTION] {action}")
    print(f"[REQUEST ID] {request_id}")

    # Run the command without blocking the event loop.
    result = await asyncio.to_thread(
        run_approved_action,
        action,
    )

    response = {
        "type": "action_result",
        "request_id": request_id,
        "device_id": DEVICE_ID,
        "action": action,
        "result": result,
    }

    await websocket.send(json.dumps(response))

    print("[RESULT SENT]")
    print(json.dumps(response, indent=2))


async def run_agent() -> None:
    """Connect to FastAPI and reconnect if disconnected."""

    url = f"{SERVER_URL}/ws/{DEVICE_ID}"

    while True:
        try:
            print(f"[CONNECTING] Device: {DEVICE_ID}")
            print(f"[SERVER] {SERVER_URL}")

            async with websockets.connect(
                url,
                open_timeout=10,
                close_timeout=5,
                ping_interval=20,
                ping_timeout=20,
            ) as websocket:

                print(f"[CONNECTED] Device: {DEVICE_ID}")

                while True:
                    raw_message = await websocket.recv()

                    try:
                        message = json.loads(raw_message)
                    except json.JSONDecodeError:
                        print("[ERROR] Invalid JSON from server")
                        continue

                    if not isinstance(message, dict):
                        print("[ERROR] Invalid message format")
                        continue

                    await handle_message(
                        websocket,
                        message,
                    )

        except asyncio.CancelledError:
            raise

        except (
            OSError,
            asyncio.TimeoutError,
            websockets.exceptions.WebSocketException,
        ) as error:
            print(f"[CONNECTION ERROR] {error}")
            print("[RECONNECT] Retrying in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(run_agent())
    except KeyboardInterrupt:
        print("\n[STOPPED] Agent stopped by user")