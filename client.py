import asyncio
import websockets
import datetime
import socket
import inspect
import sys
import os

class watchgod_websocket:
    def __init__(self, uri:str):
        self.ws = None
        self.host_n = socket.gethostname()
        self.prog_n = os.path.basename(sys.argv[0])
        self.req = ""
        self.msg = ""
        self.uri = uri

    async def connect(self):
        # print("Trying to connect to WS Server")
        try:
            self.ws = await websockets.connect(self.uri)
            # print("Success connect to WS Server")
            return True
        except:
            print("Failed connect to WS Server")
            return False

    async def disconnect(self):
        await self.ws.close()
        self.ws = None

    async def checkConnection(self) -> bool:
        try:
            await self.ws.send("")
            print("WS Server Connection Alive")
            return True
        except:
            print("WS Server Connection Dead")
            return False

    async def send_msg(self, func_n: str) -> bool:
        if not await self.connect():
            return False
        tnow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.msg = f"{tnow};{self.host_n};{self.prog_n};{func_n}"
        await self.ws.send(self.msg)
        # print(f"> {self.msg}")
        await self.recv_msg()
        await self.disconnect()
        return True

    async def recv_msg(self):
        self.req = await self.ws.recv()
        # print(f"< {self.req}")
        if not self.req == "GJ":
            self.send_msg()


if __name__ == "__main__":
    all_datas = {
        "uri": os.getenv("WS_URI", "ws://127.0.0.1:8080"),
    }
    ws = watchgod_websocket(all_datas["uri"])
    for _ in range(3):
        asyncio.run(ws.send_msg(func_n=inspect.currentframe().f_code.co_name))
    print("DONE")
