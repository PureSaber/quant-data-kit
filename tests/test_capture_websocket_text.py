import asyncio

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from quant_data_kit.capture_v2.transport import _WebsocketsConnection


def test_subscription_is_a_text_frame_on_the_wire():
    async def exercise():
        received = asyncio.get_running_loop().create_future()

        async def handle(connection):
            received.set_result(await connection.recv())

        async with serve(handle, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            async with connect(f"ws://127.0.0.1:{port}", proxy=None) as connection:
                wrapped = _WebsocketsConnection(connection)
                payload = b'{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}'
                await wrapped.send(payload)
                observed = await asyncio.wait_for(received, timeout=3)
                assert isinstance(observed, str)
                assert observed.encode("utf-8") == payload

    asyncio.run(exercise())
