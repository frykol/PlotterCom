import asyncio
import random
import string
import struct


HOST = "127.0.0.1"
PORT = 8080

MESSAGE_MAX_CHUNK = 1024

open_connections = []


def random_string(length):
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def make_frame(message):
    data = message.encode("utf-8")
    header = struct.pack("!Q", len(data))
    return header + data


async def create_client(name):
    reader, writer = await asyncio.open_connection(HOST, PORT)

    open_connections.append((name, writer))

    print(f"[{name}] CONNECT")

    return reader, writer


async def test_normal():
    _, writer = await create_client("NORMAL")

    for i in range(10):
        message = f"normal-{i}-{random_string(10)}"

        writer.write(make_frame(message))
        await writer.drain()

        print(f"[NORMAL] TX: {message}")

        await asyncio.sleep(0.01)


async def test_many_frames_one_send():
    _, writer = await create_client("MULTI FRAME")

    frames = []

    for i in range(20):
        message = f"multi-{i}-{random_string(10)}"

        print(f"[MULTI FRAME] ADD: {message}")

        frames.append(make_frame(message))

    packet = b"".join(frames)

    print(f"[MULTI FRAME] Sending {len(frames)} frames as {len(packet)} bytes")

    writer.write(packet)
    await writer.drain()


async def test_fragmented_frame():
    _, writer = await create_client("FRAGMENT")

    message = "fragmented-message-" + random_string(40)

    frame = make_frame(message)

    print(f"[FRAGMENT] Original message: {message}")
    print(f"[FRAGMENT] Frame size: {len(frame)} bytes")

    offset = 0

    while offset < len(frame):
        chunk_size = random.randint(1, 2)

        chunk = frame[offset : offset + chunk_size]

        writer.write(chunk)
        await writer.drain()

        offset += len(chunk)

        await asyncio.sleep(0.01)


async def test_random_fragmentation():
    _, writer = await create_client("RANDOM FRAGMENTATION")

    messages = [f"random-{i}-{random_string(random.randint(5, 30))}" for i in range(15)]

    stream = b"".join(make_frame(message) for message in messages)

    print(f"[RANDOM FRAGMENTATION] {len(messages)} messages, {len(stream)} bytes total")

    for message in messages:
        print(f"[RANDOM FRAGMENTATION] EXPECT: {message}")

    offset = 0

    while offset < len(stream):
        chunk_size = random.randint(1, 25)

        chunk = stream[offset : offset + chunk_size]

        writer.write(chunk)
        await writer.drain()

        print(f"[RANDOM FRAGMENTATION] TX chunk: {len(chunk)} bytes")

        offset += len(chunk)

        await asyncio.sleep(random.uniform(0, 0.02))


async def test_oversized_header():
    _, writer = await create_client("OVERSIZED")

    fake_size = MESSAGE_MAX_CHUNK * 10

    header = struct.pack("!Q", fake_size)

    print(f"[OVERSIZED] Claiming payload size: {fake_size} bytes")

    writer.write(header)
    await writer.drain()


async def close_all_connections():
    print("\nClosing all clients...")

    for name, writer in open_connections:
        writer.close()
        await writer.wait_closed()

        print(f"[{name}] DISCONNECT")


async def main():
    print("=== PlotterCom TCP parser tests ===\n")

    await test_normal()
    await test_many_frames_one_send()
    await test_fragmented_frame()
    await test_random_fragmentation()
    await test_oversized_header()

    print("\n=== ALL DATA SENT ===")
    print("All clients are still connected.")
    print("You can now inspect queues using GET in PlotterCom.")

    await asyncio.to_thread(input, "\nPress ENTER to disconnect all clients...")

    await close_all_connections()

    print("\n=== TESTS FINISHED ===")


asyncio.run(main())
