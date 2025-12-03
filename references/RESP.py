# this is a proof of concept of a minimal GET/SET client that implements zero-copy (to the extent possible with python)
# which is not natively provided by redis-py

# RESP: https://redis.io/docs/latest/develop/reference/protocol-spec/
# "In RESP, the first byte of data determines its type."
# "Clients send commands to a Redis server as an array of bulk strings"
# "The \r\n (CRLF) is the protocol's terminator, which always separates its parts."
# types needed for this client: 
# 1. simple strings: "Simple strings are encoded as a plus (+) character, followed by a string. The string mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n)."
# example: +OK\r\n
# 2. simple errors: same but with a minus (-) character
# example: -ERRMSG\r\n
# 3. integers: :[<+|->]<value>\r\n (e.g. :1000\r\n)
# 4. bulk strings (~binaries/blobs): $<length>\r\n<data>\r\n (e.g. $5\r\nhello\r\n)
# null str: $-1\r\n

# GET and SET
# keys must be a strings

# GET: https://redis.io/docs/latest/commands/get/
# GET key
# ret val: nil or value
# RESP: *2\r\n$3\r\nGET\r\n$<len(key)>\r\n<key>\r\n

# SET: https://redis.io/docs/latest/commands/set/
# SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
# ret val: "OK" 
# RESP: *3\r\n$3\r\nSET\r\n$<len(key)>\r\n<key>\r\n$<len(value)>\r\n<value>\r\n

import socket
import argparse

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

class BufferPool: 
    # default is 1024 buffers of 4 MB each
    # around 4 GB of memory
    def __init__(self, pool_size: int = 1024, buffer_size: int = 4194304): 
        self.pool = [bytearray(buffer_size) for _ in range(pool_size)]

    def lease_buffers(self, num: int) -> list[bytearray]:
        # TODO: make this threadsafe
        return [self.pool.pop() for _ in range(num)]


if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=4194304)
    args = parser.parse_args()
    client = RedisClient("127.0.0.1", 6379, args.size)
    pool = BufferPool(pool_size=1024, buffer_size=args.size)

    buffers = pool.lease_buffers(2)
    # a unit test
    client.set("test", buffers[0])
    n = client.get("test", buffers[1])
    assert n == args.size
    assert buffers[0] == buffers[1]

    print("RESP POC passed")