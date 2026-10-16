from fastapi import WebSocket, WebSocketDisconnect


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(
        self,
        device_id: str,
        websocket: WebSocket,
    ) -> bool:
        await websocket.accept()

        # Avoid two agents using the same ID simultaneously.
        if device_id in self.active_connections:
            await websocket.close(
                code=1008,
                reason="Device ID is already connected",
            )
            return False

        self.active_connections[device_id] = websocket
        print(f"[CONNECTED] {device_id}")
        return True

    async def disconnect(
        self,
        device_id: str,
        websocket: WebSocket,
    ) -> None:
        # Remove only this connection, not a newer replacement.
        if self.active_connections.get(device_id) is websocket:
            self.active_connections.pop(device_id, None)
            print(f"[DISCONNECTED] {device_id}")

    def is_connected(self, device_id: str) -> bool:
        return device_id in self.active_connections

    def get_connected_devices(self) -> list[str]:
        return list(self.active_connections.keys())

    async def send_to_device(
        self,
        device_id: str,
        message: dict,
    ) -> bool:
        websocket = self.active_connections.get(device_id)

        if websocket is None:
            return False

        try:
            await websocket.send_json(message)
            print(f"[SENT] {device_id}: {message.get('type')}")
            return True

        except (WebSocketDisconnect, RuntimeError, OSError) as error:
            print(f"[SEND ERROR] {device_id}: {error}")
            await self.disconnect(device_id, websocket)
            return False