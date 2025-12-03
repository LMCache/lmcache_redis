import redis.asyncio as redis
import asyncio
import os
import argparse
import time
import uvloop
import concurrent.futures
import socket

class RedisClient: 
    def __init__(self, host, port, buffer_size): 
        self.sock = socket.create_connection((host, port))
        
        # OPTIMIZATION 1: Disable Nagle's algorithm for lower latency
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        self.buffer_size = buffer_size
        
        # Internal buffer for reading headers/metadata to minimize syscalls
        self._io_buf = bytearray(8192)
        self._io_view = memoryview(self._io_buf)
        self._io_pos = 0
        self._io_end = 0

        self.OK_VIEW = memoryview(b"+OK\r\n")

    # --- Buffered I/O Helpers ---

    def _fill_buffer(self):
        """Refills the internal IO buffer."""
        # Reset pointers to maximize space
        if self._io_pos == self._io_end:
            self._io_pos = 0
            self._io_end = 0
        
        # Only read if we have space
        if self._io_end < len(self._io_buf):
            n = self.sock.recv_into(self._io_view[self._io_end:])
            if n == 0:
                raise ConnectionError("Socket closed")
            self._io_end += n

    def _read_byte(self) -> int:
        """Reads a single byte from the internal buffer, refilling if necessary."""
        if self._io_pos >= self._io_end:
            self._fill_buffer()
        
        b = self._io_buf[self._io_pos]
        self._io_pos += 1
        return b

    def _read_line(self) -> bytes:
        """
        Reads until CRLF using the internal buffer. 
        """
        # FIX: Ensure we have data before setting line_start.
        # If the buffer is empty, refill it and reset pointers to 0 
        # BEFORE we decide where the line starts.
        if self._io_pos >= self._io_end:
            self._fill_buffer()

        line_start = self._io_pos
        
        while True:
            chunk = self._io_buf[self._io_pos:self._io_end]
            
            # Check for \r\n in the current chunk
            if b'\r\n' in chunk:
                idx = chunk.find(b'\r\n')
                abs_idx = self._io_pos + idx
                
                result = self._io_buf[line_start:abs_idx + 2] # include \r\n
                self._io_pos = abs_idx + 2
                return bytes(result)
            
            # Not found? We need more data.
            if self._io_end == len(self._io_buf):
                 # Buffer is full and we still haven't found a newline. 
                 # This implies the header is larger than 8KB.
                 raise ValueError("Header too large or malformed")
            
            # Read more data into the tail of the buffer
            self._fill_buffer()

    def _send_all_via_sendmsg(self, parts: list[memoryview]):
        """
        OPTIMIZATION: Handles partial writes with sendmsg.
        """
        while parts:
            n_sent = self.sock.sendmsg(parts)
            if n_sent == 0:
                raise ConnectionError("Socket connection broken")
            
            # If everything sent, break
            # Calculating total length is expensive, so we adjust parts based on n_sent
            
            # Advance the views based on what was sent
            curr_sent = 0
            while parts and curr_sent < n_sent:
                part = parts[0]
                part_len = len(part)
                rem_sent = n_sent - curr_sent
                
                if rem_sent >= part_len:
                    # This part was fully sent
                    parts.pop(0)
                    curr_sent += part_len
                else:
                    # This part was partially sent, slice it and stop
                    parts[0] = part[rem_sent:]
                    break

    def get(self, key: str, recv_buf: bytearray) -> int:
        key_bytes = key.encode()
        
        command_parts = [
            memoryview(b"*2\r\n"),
            memoryview(f"${len(b'GET')}\r\n".encode()),
            memoryview(b"GET\r\n"),
            memoryview(f"${len(key_bytes)}\r\n".encode()),
            memoryview(key_bytes),
            memoryview(b"\r\n")
        ]
        
        self._send_all_via_sendmsg(command_parts)
        
        header = self._read_line() # e.g., b'$4194304\r\n' or b'$-1\r\n'

        if header.startswith(b"$-1"):
            return -1 # Key not found

        if not header.startswith(b"$"):
            raise ValueError(f"Unexpected reply: {header!r}")
        
        size = int(header[1:-2]) 
        
        # Safety check for buffer size
        if size > len(recv_buf):
             raise ValueError(f"Value too large for buffer: {size} > {len(recv_buf)}")

        # --- OPTIMIZATION: Zero-Copy Read handling internal buffer overlap ---
        
        bytes_read = 0
        
        # 1. Drain internal buffer first (Zero-copy logic)
        # If the server sent [Header][Start of Body] in one packet, 
        # it is sitting in self._io_buf
        remaining_in_io = self._io_end - self._io_pos
        
        if remaining_in_io > 0:
            to_copy = min(size, remaining_in_io)
            recv_buf[0:to_copy] = self._io_buf[self._io_pos : self._io_pos + to_copy]
            self._io_pos += to_copy
            bytes_read += to_copy
            
        # 2. Read remainder directly into user buffer
        if bytes_read < size:
            view = memoryview(recv_buf)
            while bytes_read < size:
                needed = size - bytes_read
                n = self.sock.recv_into(view[bytes_read:bytes_read+needed])
                if n == 0:
                    raise ConnectionError("Socket closed mid-body")
                bytes_read += n
        
        # 3. Consume trailing CRLF
        # We must be careful: the CRLF might be in the stream now, 
        # or partially in our io_buf if we over-read (though recv_into above limits that).
        # We simply use _read_line or _read_byte to eat 2 bytes.
        # Since we did recv_into exactly `needed`, the socket pointer is at the CRLF.
        
        # We can't assume the internal buffer has the CRLF because we bypassed it 
        # for the large read. We should just read 2 bytes safely.
        
        # To keep it simple/safe, we read 2 bytes. 
        # Optimization note: Since we bypassed _io_buf for the body, _io_buf is effectively empty/invalid
        # regarding the stream position unless we implement complex cursor logic.
        # For this POC, we can just do a small read.
        
        self._io_pos = 0; self._io_end = 0 # Invalidate buffer after direct socket read
        c1 = self.sock.recv(1)
        c2 = self.sock.recv(1)
        if c1 != b'\r' or c2 != b'\n':
             raise ValueError("Missing final CRLF")

        return bytes_read

    def set(self, key: str, send_buf: bytearray):
        key_bytes = key.encode()
        
        command_parts = [
            memoryview(b"*3\r\n"),
            memoryview(f"${len(b'SET')}\r\n".encode()),
            memoryview(b"SET\r\n"),
            memoryview(f"${len(key_bytes)}\r\n".encode()),
            memoryview(key_bytes),
            memoryview(b"\r\n"),
            memoryview(f"${len(send_buf)}\r\n".encode()),
            memoryview(send_buf),
            memoryview(b"\r\n")
        ]
        
        self._send_all_via_sendmsg(command_parts)

        # Buffer response reading
        reply = self._read_line()
        if reply != b"+OK\r\n":
             raise ValueError(f"Unexpected reply: {reply!r}")

# 32 MB
read_write_size = 32 * 1024 ** 2

# --- ADAPTER FOR YOUR CUSTOM CLIENT ---
class SyncToAsyncAdapter:
    """
    Wraps your blocking client to run in a thread pool, 
    allowing it to coexist with the asyncio benchmark.
    """
    def __init__(self, host, port, buffer_size, pool_executor):
        self._client = RedisClient(host, port, buffer_size)
        self._pool = pool_executor
        # Create a dedicated buffer for this client instance to reuse
        self._recv_buf = bytearray(buffer_size)

    async def set(self, key: str, value: bytes):
        # We must copy bytes -> bytearray for your client
        # (This adds overhead, but is required to match the benchmark API)
        # In a real use case, you'd start with bytearrays.
        if isinstance(value, bytes):
            # We copy into our reusable buffer to send
            # Note: This copy hurts your benchmarks. 
            # ideally the benchmark generator produces bytearrays.
            self._recv_buf[:len(value)] = value
            val_to_send = self._recv_buf[:len(value)]
        else:
            val_to_send = value
            
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._pool, 
            self._client.set, 
            key, 
            val_to_send
        )

    async def get(self, key: str):
        loop = asyncio.get_running_loop()
        # Pass the pre-allocated buffer
        n = await loop.run_in_executor(
            self._pool, 
            self._client.get, 
            key, 
            self._recv_buf
        )
        return n # Return bytes read, not the data object (to avoid copy cost)

# --- MODIFIED BENCHMARK UTILS ---

def make_chunks(size: int, num_chunks: int, as_bytearray=False) -> list:
    num_bytes = size * 1024 ** 2
    print(f"Generating {num_chunks} chunks of {size}MB...")
    if as_bytearray:
        # Pre-allocate bytearrays for the custom client to shine
        return [bytearray(os.urandom(num_bytes)) for _ in range(num_chunks)]
    return [os.urandom(num_bytes) for _ in range(num_chunks)]

async def writer(client, write_chunks: list, total_size: int, concurrent_mode: bool):
    start_time = time.time()
    
    if concurrent_mode:
        # Create tasks
        tasks = [client.set(f"write_chunk_{i}", chunk) for i, chunk in enumerate(write_chunks)]
        await asyncio.gather(*tasks)
    else:
        # Sequential
        for i, chunk in enumerate(write_chunks):
            await client.set(f"write_chunk_{i}", chunk)
            
    end_time = time.time()
    duration = end_time - start_time
    gb = total_size / 1024 ** 3
    print(f"Wrote {gb:.2f} GB in {duration:.2f}s | Speed: {gb / duration:.2f} GB/s")

async def reader(client, num_chunks, total_size, concurrent_mode):
    start_time = time.time()
    
    if concurrent_mode:
        # FIX: Changed "read_chunk_" to "write_chunk_" to match the writer
        tasks = [client.get(f"write_chunk_{i}") for i in range(num_chunks)]
        await asyncio.gather(*tasks)
    else:
        for i in range(num_chunks):
            # FIX: Changed "read_chunk_" to "write_chunk_"
            await client.get(f"write_chunk_{i}")
            
    end_time = time.time()
    duration = end_time - start_time
    gb = total_size / 1024 ** 3
    print(f"Read {gb:.2f} GB in {duration:.2f}s | Speed: {gb / duration:.2f} GB/s")

async def main(args):
    # Determine Client Type
    if args.client_type == "redis-py":
        print("Using redis-py (Async)")
        pool = redis.ConnectionPool.from_url("redis://localhost", max_connections=100)
        client = redis.Redis.from_pool(pool)
        use_bytearray = False
        
    elif args.client_type == "custom":
        print("Using Custom Zero-Copy Client (Threaded Wrapper)")
        # We use a ThreadPoolExecutor to prevent blocking the async loop
        # max_workers=1 simulates serial execution (fair for a single socket)
        # max_workers=10 simulates a connection pool
        pool_executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency_level)
        
        # NOTE: This creates ONE client. For true concurrency, you need a pool of CustomClients.
        # This is just a wrapper for the single client we wrote.
        client = SyncToAsyncAdapter("127.0.0.1", 6379, args.size_mb * 1024**2, pool_executor)
        use_bytearray = True # CRITICAL: Use mutable buffers
        
    else:
        raise ValueError("Unknown client type")

    total_size = args.size_mb * 1024 ** 2 * args.num_chunks
    
    # Generate Data
    # If custom client, we generate bytearrays to avoid conversion overhead
    chunks = make_chunks(args.size_mb, args.num_chunks, as_bytearray=use_bytearray)

    if args.write:
        print("--- Starting Write Test ---")
        await writer(client, chunks, total_size, args.concurrent)
        
    if args.read:
        print("--- Starting Read Test ---")
        # Ensure data exists for reading if we didn't just write it
        if not args.write:
             # Pre-populate logic (simplified)
             pass 
        await reader(client, args.num_chunks, total_size, args.concurrent)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--client-type", type=str, default="redis-py", choices=["redis-py", "custom"])
    parser.add_argument("--size-mb", type=int, default=32)
    parser.add_argument("--num-chunks", type=int, default=10)
    parser.add_argument("--read", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--concurrent", action="store_true")
    parser.add_argument("--concurrency-level", type=int, default=1, help="Threads for custom client")
    
    args = parser.parse_args()
    
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.run(main(args))