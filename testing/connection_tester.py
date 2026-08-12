import asyncio
import struct
import random
import string


HOST = "127.0.0.1"
PORT = 8080


def random_string(length):
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))


async def send_message(writer, message):
    data = message.encode("utf-8")
    header = struct.pack("!Q", len(data))

    writer.write(header + data)
    await writer.drain()

    print(f"TX: {message}")


async def connection(name, actions):
    reader, writer = await asyncio.open_connection(HOST, PORT)

    print(f"[{name}] CONNECT")

    send_count = actions["send_count"]
    disconnect = actions["disconnect"]
    delay_ms = actions.get("delay_ms", 0)

    try:
        for i in range(send_count):
            await send_message(writer, random_string(15))

            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)

        # Jeśli disconnect=False, utrzymujemy połączenie
        if not disconnect:
            print(f"[{name}] KEEP ALIVE")

            # Czekamy tutaj w nieskończoność
            await asyncio.Event().wait()

    finally:
        writer.close()
        await writer.wait_closed()

        print(f"[{name}] DISCONNECT")


connections = [
    {
        "name": "client-1",
        "actions": {
            "send_count": 15,
            "delay_ms": 10,
            "disconnect": False,
        },
    },
    {
        "name": "client-2",
        "actions": {
            "send_count": 5,
            "delay_ms": 10,
            "disconnect": False,
        },
    },
    {
        "name": "client-2",
        "actions": {
            "send_count": 20,
            "delay_ms": 10,
            "disconnect": False,
        },
    },
]


async def main():
    await asyncio.gather(
        *(connection(conn["name"], conn["actions"]) for conn in connections)
    )


asyncio.run(main())
