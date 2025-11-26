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