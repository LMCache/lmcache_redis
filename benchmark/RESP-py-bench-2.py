import argparse
import socket
import time
import queue
import threading
from concurrent.futures import Future
"""
Variance can be up to 1-2 GB/s

python benchmark/RESP-py-bench-2.py --pool-size 4096
Running single threaded benchmark
Wrote 16.0 GB in 6.99776029586792 seconds, which is: 2.2864458517459902 GB/s
Read 16.0 GB in 4.4660069942474365 seconds, which is: 3.582618661504391 GB/s


python benchmark/RESP-py-bench-2.py --pool-size 4096 --num-threads 1
Running multi threaded benchmark with 1 threads
Wrote 16.00 GB in 6.885 s  →  2.324 GB/s
Read  16.00 GB in 4.438 s  →  3.605 GB/s

python benchmark/RESP-py-bench-2.py --pool-size 4096 --num-threads 4
Running multi threaded benchmark with 4 threads
Wrote 16.00 GB in 3.892 s  →  4.111 GB/s
Read  16.00 GB in 2.157 s  →  7.419 GB/s

python benchmark/RESP-py-bench-2.py --pool-size 4096 --num-threads 8
Running multi threaded benchmark with 8 threads
Wrote 16.00 GB in 3.803 s  →  4.207 GB/s
Read  16.00 GB in 2.767 s  →  5.783 GB/s
"""
# in between: 
# redis-cli -p 6379 FLUSHALL
# redis-cli -p 6379 DBSIZE

class RedisClient: 
    """
    Fully optimized deterministic-size RESP client.

    Assumptions:
      - Every SET stores exactly buffer_size bytes.
      - Every GET returns exactly buffer_size bytes.
      - Keys always exist (no $-1).
      - RESP header is fixed-length: $<size>\r\n
    """
    def __init__(self, host: str, port: int, buffer_size: int):
        self.buffer_size = buffer_size

        # Pre-compute the deterministic RESP bulk header
        self.header = f"${buffer_size}\r\n".encode()
        self.header_len = len(self.header)
        self.trailer_len = 2  # b"\r\n"

        self._get_prefix = [
            memoryview(b"*2\r\n"),
            memoryview(b"$3\r\nGET\r\n"),
        ]

        self._set_prefix = [
            memoryview(b"*3\r\n"),
            memoryview(b"$3\r\nSET\r\n"),
        ]

        self._ok = memoryview(b"+OK\r\n")

        # Create and configure socket
        self.sock = socket.create_connection((host, port))
        
        # nagle's algo disabled does NOT help since we want to batch more for raw throughput
        # self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # could look into the opposite config of TCP_NODELAY: TCP_CORK
        # self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)

    def _recv_exactly(self, n: int, into: memoryview | None = None):
        """
        Reads exactly n bytes.
        If 'into' is provided, fills that buffer; else returns a new buffer.
        """
        if into is not None:
            view = into
        else:
            buf = bytearray(n)
            view = memoryview(buf)

        total = 0
        while total < n:
            m = self.sock.recv_into(view[total:total + (n - total)])
            if m == 0:
                raise ConnectionError("Socket closed during recv_exactly")
            total += m

        return view if into is None else None

    def _send_all_via_sendmsg(self, parts: list[memoryview]):
        """
        Zero-copy scatter/gather write with correct partial-write handling.
        """
        while parts:
            n_sent = self.sock.sendmsg(parts)
            if n_sent == 0:
                raise ConnectionError("Broken connection during sendmsg")

            sent = 0
            while parts and sent < n_sent:
                p = parts[0]
                p_len = len(p)
                remain = n_sent - sent

                if remain >= p_len:
                    parts.pop(0)
                    sent += p_len
                else:
                    parts[0] = p[remain:]
                    break

    def get(self, key: str, recv_buf: memoryview) -> int:
        """
        Retrieves exactly buffer_size bytes into recv_buf.
        Uses deterministic header length (no parsing).
        """

        key_b = key.encode()
        key_len_hdr = f"${len(key_b)}\r\n".encode()

        # Build scatter-gather message
        parts = [
            *self._get_prefix,
            memoryview(key_len_hdr),
            memoryview(key_b),
            memoryview(b"\r\n"),
        ]

        # Send GET command
        self._send_all_via_sendmsg(parts)

        # 1. Read deterministic header: $<buffer_size>\r\n
        tmp_header = bytearray(self.header_len)
        self._recv_exactly(self.header_len, memoryview(tmp_header))

        if tmp_header != self.header:
            raise ValueError(f"Unexpected GET header: {tmp_header!r}")

        # 2. Read VALUE directly into caller buffer (zero-copy)
        if len(recv_buf) < self.buffer_size:
            raise ValueError("recv_buf too small")

        self._recv_exactly(self.buffer_size, recv_buf)

        # 3. Read trailing CRLF
        tmp = bytearray(2)
        self._recv_exactly(2, memoryview(tmp))

        if tmp != b"\r\n":
            raise ValueError("Missing final CRLF after GET body")

        return self.buffer_size

    def set(self, key: str, send_buf: memoryview):
        """
        Sends exactly buffer_size bytes as the value.
        """

        if len(send_buf) != self.buffer_size:
            raise ValueError("send_buf must be exactly buffer_size bytes.")

        key_b = key.encode()
        key_len_hdr = f"${len(key_b)}\r\n".encode()

        parts = [
            *self._set_prefix,
            memoryview(key_len_hdr),
            memoryview(key_b),
            memoryview(b"\r\n"),
            memoryview(self.header),   # $<buffer_size>\r\n
            send_buf,          # actual payload (zero-copy)
            memoryview(b"\r\n")
        ]

        self._send_all_via_sendmsg(parts)

        # Expect "+OK\r\n"
        tmp = bytearray(5)
        self._recv_exactly(5, memoryview(tmp))

        if tmp != b"+OK\r\n":
            raise ValueError(f"Unexpected SET reply: {tmp!r}")

class MultiThreadedRedisClient:
    def __init__(self, host, port, buffer_size, num_threads):
        self.num_threads = num_threads
        self.next = 0  # round-robin index

        # One queue per thread
        self.queues = [queue.Queue() for _ in range(num_threads)]

        # One RedisClient per thread
        self.clients = [
            RedisClient(host, port, buffer_size) for _ in range(num_threads)
        ]

        # Start worker threads
        self.threads = []
        for i in range(num_threads):
            t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            t.start()
            self.threads.append(t)

    def _worker_loop(self, idx):
        client = self.clients[idx]
        q = self.queues[idx]

        while True:
            op, key, buf, future = q.get()

            try:
                if op == "set":
                    client.set(key, buf)
                    if future:
                        future.set_result(None)
                else:  # GET
                    n = client.get(key, buf)
                    future.set_result(n)
            except Exception as e:
                if future:
                    future.set_exception(e)

    def _dispatch(self, item):
        i = self.next
        self.next = (self.next + 1) % self.num_threads
        self.queues[i].put(item)

    def set(self, key, buf):
        f = Future()
        self._dispatch(("set", key, buf, f))
        return f

    def get(self, key, buf):
        f = Future()
        self._dispatch(("get", key, buf, f))
        return f

class BufferPool: 
    # may need thread-safety
    def __init__(self, pool_size: int, buffer_size: int): 
        self.buffer_size = buffer_size
        self.pool_size = pool_size
        self.pool = [memoryview(bytearray(buffer_size)) for _ in range(pool_size)]

    def lease_buffers(self, num: int) -> list[memoryview]:
        return [self.pool.pop() for _ in range(num)]
    
    @property
    def total_size(self) -> int: 
        return self.pool_size * self.buffer_size
    
def benchmark_write(client: RedisClient, pool: BufferPool):
    """
    write all of the buffers in the pool to the server
    """
    start_time = time.time()
    for i in range(pool.pool_size):
        buf = pool.pool[i]
        client.set(f"chunk_{i}", buf)
    end_time = time.time()
    print(f"Wrote {pool.total_size / 1024 ** 3} GB in {end_time - start_time} seconds, which is: {pool.total_size / (end_time - start_time) / 1024 ** 3} GB/s")

def benchmark_write_concurrent(client: MultiThreadedRedisClient, pool: BufferPool):
    start_time = time.time()
    futures = []
    for i in range(pool.pool_size):
        buf = pool.pool[i]
        fut = client.set(f"chunk_{i}", buf)
        futures.append(fut)
    for fut in futures:
        fut.result()
    end_time = time.time()
    secs = end_time - start_time
    gb = pool.total_size / 1024**3
    print(f"Wrote {gb:.2f} GB in {secs:.3f} s  →  {gb/secs:.3f} GB/s")
    
def benchmark_read(client: RedisClient, pool: BufferPool):
    """
    read all of the buffers in the pool from the server

    should be called AFTER benchmark_write
    """
    start_time = time.time()
    for i in range(pool.pool_size):
        buf = pool.pool[i]
        client.get(f"chunk_{i}", buf)
    end_time = time.time()
    print(f"Read {pool.total_size / 1024 ** 3} GB in {end_time - start_time} seconds, which is: {pool.total_size / (end_time - start_time) / 1024 ** 3} GB/s")

def benchmark_read_concurrent(client: MultiThreadedRedisClient, pool: BufferPool):
    start_time = time.time()
    futures = []
    for i in range(pool.pool_size):
        buf = pool.pool[i]
        fut = client.get(f"chunk_{i}", buf)
        futures.append(fut)
    for fut in futures:
        fut.result()
    end_time = time.time()
    secs = end_time - start_time
    gb = pool.total_size / 1024**3
    print(f"Read  {gb:.2f} GB in {secs:.3f} s  →  {gb/secs:.3f} GB/s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool-size", type=int, default=1024)
    parser.add_argument("--buffer-size", type=int, default=4194304) # 4 MB
    parser.add_argument("--num-threads", type=int, required=False)
    args = parser.parse_args()
    pool = BufferPool(pool_size=args.pool_size, buffer_size=args.buffer_size)


    # single threaded
    if args.num_threads is None:
        print("Running single threaded benchmark")
        client = RedisClient(host="localhost", port=6379, buffer_size=args.buffer_size)
        benchmark_write(client, pool)
        benchmark_read(client, pool)

    # multi threaded codepath
    else:
        print(f"Running multi threaded benchmark with {args.num_threads} threads")
        client = MultiThreadedRedisClient(host="localhost", port=6379, buffer_size=args.buffer_size, num_threads=args.num_threads)
        benchmark_write_concurrent(client, pool)
        benchmark_read_concurrent(client, pool)    