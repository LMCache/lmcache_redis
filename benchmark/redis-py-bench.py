# This is a benchmarking client built with redis-py
import redis.asyncio as redis
import asyncio
import os
import argparse
import time
import uvloop
# 32 MB
read_write_size = 32 * 1024 ** 2
concurrent = False

def make_chunks(size: int, num_chunks: int) -> list[bytes]:
    """
    size: MB
    num_chunks: # chunks
    """ 
    num_bytes = size * 1024 ** 2
    chunks = [os.urandom(num_bytes) for _ in range(num_chunks)]
    return chunks

async def instrumented_set(client: redis.Redis, key: str, value: bytes):
    print(f"Starting set for {key}")
    start_time = time.time()
    await client.set(key, value)
    end_time = time.time()
    print(f"Set {key} in {end_time - start_time} seconds")

async def warmup_pool(client, max_connections: int):
    print(f"warming up the pool with {max_connections} connections")
    # try to saturate the connections to force them to initialize (redis' pool is lazy)
    # dummy_tasks = [client.set(f"dummy_{i}", os.urandom(2)) for i in range(max_connections*5)]
    dummy_tasks = [instrumented_set(client, f"dummy_{i}", os.urandom(2)) for i in range(max_connections*5)]
    await asyncio.gather(*dummy_tasks)

class MultiRedisClient: 
    def __init__(self, clients): 
        self.clients = clients
        self.counter = 0
        self._lock = asyncio.Lock()
    async def get(self, *args, **kwargs): 
        async with self._lock:
            client = self.clients[self.counter]
            self.counter = (self.counter + 1) % len(self.clients)
        return await client.get(*args, **kwargs)
    async def set(self, *args, **kwargs): 
        async with self._lock:
            client = self.clients[self.counter]
            self.counter = (self.counter + 1) % len(self.clients)
        return await client.set(*args, **kwargs)

async def get_redis_client(type: str):
    if type == "single-pool": 
        print("Using Single Client Pool")
        max_connections = 100
        pool = redis.ConnectionPool.from_url("redis://localhost", max_connections=max_connections)
        client = redis.Redis.from_pool(pool)
        # await warmup_pool(client, max_connections)
        return client
    elif type == "multi-pool":
        print("Using Multi Client Pool")
        max_connections = 100
        pool = redis.ConnectionPool.from_url("redis://localhost", max_connections=max_connections)
        clients = [redis.Redis.from_pool(pool) for _ in range(max_connections)]
        return MultiRedisClient(clients)

async def writer(client: redis.Redis, write_chunks: list[bytes], total_size: int):
    start_time = time.time()
    if concurrent:
        tasks = [client.set(f"write_chunk_{i}", chunk) for i, chunk in enumerate(write_chunks)]
        await asyncio.gather(*tasks)
    else:
        for i, chunk in enumerate(write_chunks):
            await client.set(f"write_chunk_{i}", chunk)
        await client.set(f"write_chunk_{i}", chunk)
    end_time = time.time()
    gb = total_size / 1024 ** 3
    print(f"Wrote {gb} GB in {end_time - start_time} seconds, which is: {gb / (end_time - start_time)} GB/s")

async def reader(client: redis.Redis, num_chunks: int, total_size: int):
    start_time = time.time()
    if concurrent:
        tasks = [client.get(f"read_chunk_{i}") for i in range(num_chunks)]
        await asyncio.gather(*tasks)
    else:
        for i in range(num_chunks):
            await client.get(f"read_chunk_{i}")
        await client.get(f"read_chunk_{i}")
    end_time = time.time()
    gb = total_size / 1024 ** 3
    print(f"Read {gb} GB in {end_time - start_time} seconds, which is: {gb / (end_time - start_time)} GB/s")

async def read_prepopulate(client: redis.Redis, read_chunks: list[bytes]):
    for i, chunk in enumerate(read_chunks):
        await client.set(f"read_chunk_{i}", chunk)

async def main(args):
    client = await get_redis_client(args.client_type)
    coros = []
    total_size = args.size_mb * 1024 ** 2 * args.num_chunks
    if args.write:
        write_chunks = make_chunks(args.size_mb, args.num_chunks)
        coros.append(writer(client, write_chunks, total_size))
    if args.read:
        read_chunks = make_chunks(args.size_mb, args.num_chunks)
        await read_prepopulate(client, read_chunks)
        coros.append(reader(client, args.num_chunks, total_size))
    await asyncio.gather(*coros)

def create_arg_parser(): 
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-type", type=str, default="pool")
    parser.add_argument("--size-mb", type=int, default=32)
    parser.add_argument("--num-chunks", type=int, default=1000)
    parser.add_argument("--read", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--concurrent", action="store_true")
    return parser

if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    args = create_arg_parser().parse_args()
    concurrent = args.concurrent
    asyncio.run(main(args))