import argparse
import socket
import time
import queue
import threading
from concurrent.futures import Future

"""
error:

(jiayis-dad) tensormesh@GPU-H100-lccn11:~/jiayis-dad/lmcache_redis$ python benchmark/RESP-py-bench-2.py --pool-size 4096
Running single threaded benchmark
Traceback (most recent call last):
  File "/home/tensormesh/jiayis-dad/lmcache_redis/benchmark/RESP-py-bench-2.py", line 376, in <module>
    benchmark_write(client, pool)
  File "/home/tensormesh/jiayis-dad/lmcache_redis/benchmark/RESP-py-bench-2.py", line 318, in benchmark_write
    client.set(f"chunk_{i}", buf)
  File "/home/tensormesh/jiayis-dad/lmcache_redis/benchmark/RESP-py-bench-2.py", line 200, in set
    self._send_multipart(parts)
  File "/home/tensormesh/jiayis-dad/lmcache_redis/benchmark/RESP-py-bench-2.py", line 115, in _send_multipart
    n_sent = self.sock.sendmsg(parts)
             ^^^^^^^^^^^^^^^^^^^^^^^^
ConnectionResetError: [Errno 104] Connection reset by peer

"""

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
    A client implementing RESP2 only for GET, SET, and EXISTS
    Should be wrapped with MultiRESPClient

    Primary Assumption (for "chunked" parsing and reusing payloads): 
    The size of payloads (KV cache object) is always fixed. The retrieval
    helper `_recv_exactly(n, buf)` can be used to retrieve payloads without
    having to scan for \r\n
    

    Optimizations: 
    - zero copy retrieval (through recv_into) ** not supported by redis-py **
    - scatter-gather sending (through sendmsg)
    """
    def __init__(self, host: str, port: int, buffer_size: int):
        """
        the chunk_size must be known beforehand (save_unfull_chunk = False)
        for this client to work
        """
        self.chunk_size = buffer_size
        self._generate_reusables(buffer_size)
        self.sock = socket.create_connection((host, port))


    def _generate_reusables(self, chunk_size: int):
        # some cached objects for scatter-gather sending
        # and response parsing
        self.size_header = f"${chunk_size}\r\n".encode()
        self.size_header_len = len(self.size_header)

        self.crlf = memoryview(b"\r\n")
        self.crlf_len = len(self.crlf)

        self._get_prefix = [
            memoryview(b"*2\r\n"),
            memoryview(b"$3\r\nGET\r\n"),
        ]

        self._set_prefix = [
            memoryview(b"*3\r\n"),
            memoryview(b"$3\r\nSET\r\n"),
        ]

        self._exists_prefix = [
            memoryview(b"*2\r\n"),
            memoryview(b"$6\r\nEXISTS\r\n"),
        ]

        # simple string response for set
        self._ok = memoryview(b"+OK\r\n")
        self._ok_len = len(self._ok)

        # integer response for exists
        self._one = memoryview(b":1\r\n")
        self._zero = memoryview(b":0\r\n")
        # assumes int < 256
        self._int_len = len(self._one) # len(self._zero)

    # --- recv and send (optimized for zero copy) ---

    def _recv_exactly_into(self, n: int, into: memoryview):
        """
        Reads exactly n bytes.
        """
        assert into is not None
        total = 0
        while total < n:
            m = self.sock.recv_into(into[total:total + (n - total)])
            if m == 0:
                raise ConnectionError("Socket closed during recv_exactly")
            total += m

    def _send_multipart(self, parts: list[memoryview]):
        """
        Zero-copy scatter/gather write with correct partial-write handling.
        """
        # parts will be "consumed" (popped) as they are sent
        while parts:
            # bytes sent
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

    # only support 3 commands
    # GET
    # SET
    # EXISTS

    def make_key_header(self, key: str) -> tuple[memoryview, memoryview]:
        key_b = key.encode()
        key_len_hdr = f"${len(key_b)}\r\n".encode()
        return memoryview(key_b), memoryview(key_len_hdr)

    def get(self, key: str, recv_buf: memoryview):
        """
        assumption:
        both recv_buf and the payload stored in redis for key 
        should be of size chunk_size

        recv_buf should be a direct reference to the buffer inside
        of a MemoryObj for zero-copy retrieval
        """ 
        assert len(recv_buf) == self.chunk_size, "recv_buf is not of size chunk_size"

        key_b, key_len_hdr = self.make_key_header(key)

        # build scatter gather msg
        parts = [
            *self._get_prefix,
            key_len_hdr,
            key_b,
            self.crlf,
        ]

        self._send_multipart(parts)

        # 1. read size header (validation)
        # we could discard the header but validating it is safer
        size_hdr = bytearray(self.size_header_len)
        self._recv_exactly_into(self.size_header_len, memoryview(size_hdr))

        assert size_hdr == self.size_header, "GET command returned invalid size header"

        # 2. read the payload / KV Cache directly into the recv_buf
        self._recv_exactly_into(self.chunk_size, recv_buf)

        # 3. read the trailer (validation)
        # we could discard the trailer but validating it is safer
        trailer = bytearray(self.crlf_len)
        self._recv_exactly_into(self.crlf_len, memoryview(trailer))
        assert trailer == self.crlf, "GET command returned invalid trailer"

    def set(self, key: str, send_buf: memoryview): 
        """
        assumption: send_buf is of size chunk_size
        """
        assert len(send_buf) == self.chunk_size, "send_buf is not of size chunk_size"

        key_b, key_len_hdr = self.make_key_header(key)

        # build scatter gather msg
        parts = [
            *self._set_prefix,
            key_len_hdr,
            key_b,
            self.crlf,
            self.size_header,
            send_buf,
            self.crlf,
        ]

        self._send_multipart(parts)

        # expect the ok response
        ret = bytearray(self._ok_len)
        self._recv_exactly_into(self._ok_len, memoryview(ret))
        assert ret == self._ok, "SET command returned invalid response"
    
    def exists(self, key: str) -> bool: 
        """
        check key existence
        """
        key_b, key_len_hdr = self.make_key_header(key)

        parts = [
            *self._exists_prefix,
            key_len_hdr,
            key_b,
            self.crlf,
        ]

        self._send_multipart(parts)

        # read the response
        ret = bytearray(self._int_len)
        self._recv_exactly_into(self._int_len, memoryview(ret))
        if ret == self._one:
            return True
        elif ret == self._zero:
            return False
        else:
            raise ValueError("EXISTS command returned invalid response")


class MultiThreadedRedisClient:
    """
    Multithreaded wrapper around RedisClient

    Please pass in keys with string serialization
    """
    def __init__(self, host: str, port: int, buffer_size: int, num_threads: int): 
        self.num_threads = num_threads
        self.i = 0 # round robin index for the dispatcher

        self.queues = [queue.Queue() for _ in range(num_threads)]
        self.clients = [RedisClient(host, port, buffer_size) for _ in range(num_threads)]

        self.threads = [
            threading.Thread(target=self.worker_loop, args=(self.clients[i], self.queues[i]), daemon=True)
            for i in range(num_threads)
        ]
        for thread in self.threads:
            thread.start()

    def worker_loop(self, client: RedisClient, q: queue.Queue):
        while True: 
            op, key, buf, future = q.get()
            try: 
                # opcodes: get, set, exists
                if op == "get":
                    client.get(key, buf)
                    future.set_result(None)
                
                elif op == "set":
                    client.set(key, buf)
                    future.set_result(None)
                
                elif op == "exists":
                    exists = client.exists(key)
                    future.set_result(exists)
                
                elif op == "close":
                    client.close()
                
                else:
                    raise ValueError(f"Invalid operation: {op}")
            except Exception as e:
                if future:
                    future.set_exception(e)
            finally:
                q.task_done()
        
    def _dispatch(self, item): 
        """
        Dispatch a job to a worker RESPClient
        """
        # item: (op, key, buf, future)
        i = self.i
        self.i = (i + 1) % self.num_threads
        self.queues[i].put(item)
    
    def set(self, key, buf):
        f = Future()
        self._dispatch(("set", key, buf, f))
        return f

    def get(self, key, buf):
        f = Future()
        self._dispatch(("get", key, buf, f))
        return f

    def exists(self, key):
        f = Future()
        self._dispatch(("exists", key, None, f))
        return f

    def close(self): 
        for i in range(self.num_threads):
            self._dispatch(("close", None, None, None))
        for thread in self.threads:
            thread.join()

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

def test_exists(client: RedisClient, pool: BufferPool):
    """
    Should be called AFTER benchmark_write
    """
    start_time = time.time()
    for i in range(pool.pool_size):
        exists = client.exists(f"chunk_{i}")
        assert exists, f"chunk_{i} does not exist after write"
    end_time = time.time()
    print(f"Tested {pool.pool_size} exists in {end_time - start_time} seconds")


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
        test_exists(client, pool)
        benchmark_read(client, pool)

    # multi threaded codepath
    else:
        print(f"Running multi threaded benchmark with {args.num_threads} threads")
        client = MultiThreadedRedisClient(host="localhost", port=6379, buffer_size=args.buffer_size, num_threads=args.num_threads)
        benchmark_write_concurrent(client, pool)
        test_exists(client, pool)
        benchmark_read_concurrent(client, pool)    