# thread based
import threading
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
import os



import socket
import argparse

# Redis client taken from references/RESP.py
class RedisClient: 
    def __init__(self, host, port, buffer_size): 
        self.sock = socket.create_connection((host, port))
        self.buffer_size = buffer_size
        
        # Pre-allocate a small buffer (e.g., 64 bytes) for header parsing
        # RESP headers are small, so this should be sufficient.
        self.header_buf = bytearray(64) 
        self.OK_VIEW = memoryview(b"+OK\r\n")

    # --- Low-Level I/O Helpers ---

    def _recv_into_exactly(self, buf: bytearray, size) -> int:
        """Reads exactly 'size' bytes directly into the given bytearray."""
        view = memoryview(buf)[:size]
        total = 0
        while total < size:
            n = self.sock.recv_into(view[total:])
            if n == 0:
                raise ConnectionError("socket closed before all data received")
            total += n
        return total
    
    def _recv_exactly(self, size) -> memoryview: 
        """Utility for reading fixed-size responses (e.g., +OK\r\n) into a new buffer."""
        buf = bytearray(size)
        self._recv_into_exactly(buf, size)
        return memoryview(buf)
    
    def _read_header(self) -> bytes:
        """
        Optimized header read using recv_into.
        Finds the end of the header (\r\n) by reading one byte at a time
        into the pre-allocated header_buf, avoiding iterative concatenation copies.
        Returns the header as a copy (since it's small metadata).
        """
        
        # NOTE: Header parsing must read until CRLF, which is a stream operation.
        # This is the most complex part to make truly zero-copy.
        
        view = memoryview(self.header_buf)
        total = 0
        while True:
            # Read one byte into the next slot in the header buffer
            # This avoids the header = b"" + chunk copy from the original code
            n = self.sock.recv_into(view[total:total + 1])
            if n == 0:
                raise ConnectionError("closed before header complete")
            
            # Check for \r\n terminator
            if total >= 1 and self.header_buf[total-1] == 13 and self.header_buf[total] == 10: # 13=CR, 10=LF
                return bytes(self.header_buf[:total+1]) # Small copy for metadata is acceptable
            
            total += n
            if total >= len(self.header_buf):
                 raise ValueError("Header buffer too small for response header")

    # --- Zero-Copy I/O Operations (Optimized) ---

    def get(self, key: str, recv_buf: bytearray) -> int:
        key_bytes = key.encode()
        
        # 1. Prepare Command Buffers for sendmsg (Zero-Copy Send)
        # We use memoryview() on the *small* parts to allow sendmsg to gather them.
        # The key bytes are automatically gathered.
        command_parts = [
            memoryview(b"*2\r\n"),                                   # Array header
            memoryview(f"${len(b'GET')}\r\n".encode()),              # GET bulk length
            memoryview(b"GET\r\n"),                                  # GET bulk data
            memoryview(f"${len(key_bytes)}\r\n".encode()),           # Key bulk length
            memoryview(key_bytes),                                   # Key bulk data
            memoryview(b"\r\n")                                      # Final CRLF for key
        ]
        
        # Use sendmsg for scatter-gather I/O. This avoids creating one large 'get_cmd' copy.
        self.sock.sendmsg(command_parts)
        
        # 2. Consume Header (Minimized Copy Receive)
        header = self._read_header()

        if not header.startswith(b"$"):
            raise ValueError(f"Unexpected reply: {header!r}")
        
        # Header is like $12345\r\n. We strip the first ($) and the last two (\r\n)
        size = int(header[1:-2]) 
        
        # Assertion to maintain POC structure
        assert size == self.buffer_size, f"Expected {self.buffer_size} bytes, got {size}"

        # 3. Consume Body (Zero-Copy Receive)
        n = self._recv_into_exactly(recv_buf, size)

        # 4. Consume trailing CRLF
        self._recv_exactly(2)

        return n

    def set(self, key: str, send_buf: bytearray):
        key_bytes = key.encode()
        
        # 1. Prepare Command Buffers for sendmsg (Zero-Copy Send)
        # memoryview(send_buf) ensures the large data payload is not copied
        command_parts = [
            memoryview(b"*3\r\n"),                                   # Array header
            memoryview(f"${len(b'SET')}\r\n".encode()),              # SET bulk length
            memoryview(b"SET\r\n"),                                  # SET bulk data
            memoryview(f"${len(key_bytes)}\r\n".encode()),           # Key bulk length
            memoryview(key_bytes),                                   # Key bulk data
            memoryview(b"\r\n"),                                     # CRLF after key
            memoryview(f"${len(send_buf)}\r\n".encode()),            # Value bulk length
            memoryview(send_buf),                                    # Value data (The Zero-Copy Payload)
            memoryview(b"\r\n")                                      # Final CRLF
        ]
        
        # Use sendmsg to send all parts without concatenating them in Python memory
        self.sock.sendmsg(command_parts)

        # 2. Expect +OK\r\n (Fixed-size Zero-Copy into temporary buffer)
        reply = self._recv_exactly(5)
        if reply != self.OK_VIEW:
            raise ValueError(f"Unexpected reply: {reply!r}")






# Global settings (simplified, can be passed via args if needed)
HOST = "127.0.0.1"
PORT = 6379
MAX_THREADS = 100

def make_chunks(size_mb: int, num_chunks: int) -> List[Tuple[str, bytearray]]:
    """Generates key-bytearray pairs for SET operations."""
    num_bytes = size_mb * 1024 ** 2
    chunks = []
    for i in range(num_chunks):
        # We use bytearray here so we can pass it to the RedisClient
        data = bytearray(os.urandom(num_bytes)) 
        chunks.append((f"chunk_{i}", data))
    return chunks

def run_writer_task(client: RedisClient, key: str, data: bytearray):
    """Worker function for SET operations in a thread pool."""
    client.set(key, data)

def run_reader_task(client: RedisClient, key: str, buffer_size: int):
    """Worker function for GET operations in a thread pool."""
    recv_buf = bytearray(buffer_size) # Each thread/task needs its own buffer
    client.get(key, recv_buf)
    # The read data is now in recv_buf, ready for use/validation

def prepopulate_reader_data(clients: List[RedisClient], read_chunks: List[Tuple[str, bytearray]]):
    """Synchronously writes all data needed for the read benchmark."""
    print("Prepopulating read data...")
    for i, (key, data) in enumerate(read_chunks):
        client = clients[i % len(clients)]
        client.set(key, data)
    print("Prepopulation complete.")

def benchmark_io(mode: str, num_chunks: int, size_mb: int, concurrent: bool):
    """
    Runs the benchmark using a ThreadPoolExecutor.
    mode: 'read' or 'write'
    """
    total_size = size_mb * 1024 ** 2 * num_chunks
    gb = total_size / 1024 ** 3

    # Setup clients (one connection per thread/pool)
    print(f"Setting up {MAX_THREADS} synchronous client connections...")
    clients = [RedisClient(HOST, PORT) for _ in range(MAX_THREADS)]
    
    # Data preparation
    chunks_to_process = make_chunks(size_mb, num_chunks)
    
    if mode == 'read':
        # Prepopulate data using the established connections
        prepopulate_reader_data(clients, chunks_to_process)

    print(f"Starting {mode} benchmark ({num_chunks} chunks of {size_mb} MB, {gb:.2f} GB total)...")
    start_time = time.time()
    
    # Use ThreadPoolExecutor for concurrent execution
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        if mode == 'write':
            # Run SET tasks concurrently
            futures = [
                executor.submit(
                    run_writer_task, 
                    clients[i % MAX_THREADS], # Cycle through clients
                    key, 
                    data
                ) 
                for i, (key, data) in enumerate(chunks_to_process)
            ]
        elif mode == 'read':
            # Run GET tasks concurrently
            futures = [
                executor.submit(
                    run_reader_task, 
                    clients[i % MAX_THREADS], 
                    key, 
                    size_mb * 1024 ** 2 # Buffer size needed
                ) 
                for i, (key, data) in enumerate(chunks_to_process)
            ]
            
        # Wait for all tasks to complete and check for exceptions
        for future in futures:
            future.result() 

    end_time = time.time()
    
    for client in clients:
        client.close()
        
    duration = end_time - start_time
    rate = gb / duration
    print(f"Finished {mode} operation.")
    print(f"Total time: {duration:.2f} seconds")
    print(f"Achieved rate: {rate:.2f} GB/s")

def main():
    parser = argparse.ArgumentParser(description="Zero-Copy Redis Benchmark")
    parser.add_argument("--size-mb", type=int, default=32, help="Size of each chunk in MB.")
    parser.add_argument("--num-chunks", type=int, default=1000, help="Number of chunks/operations.")
    parser.add_argument("--read", action="store_true", help="Run read (GET) benchmark.")
    parser.add_argument("--write", action="store_true", help="Run write (SET) benchmark.")
    
    args = parser.parse_args()
    
    if args.write:
        benchmark_io('write', args.num_chunks, args.size_mb, args.concurrent)
    if args.read:
        benchmark_io('read', args.num_chunks, args.size_mb, args.concurrent)

if __name__ == "__main__":
    main()