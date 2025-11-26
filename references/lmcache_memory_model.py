# this is a file that gives a reference implementation of the memory model used by LMCache
import torch
import math
import ctypes
from typing import Optional, Any, deque
from dataclasses import dataclass

@dataclass
class MemoryObjMetadata: 
    shape: torch.Size
    dtype: torch.dtype
    # the physical address of the tensor
    address: int
    phys_size: int

    def get_size(self) -> int: 
        num_elements = math.prod(self.shape)
        element_size = self.dtype.itemsize
        size_in_bytes = num_elements * element_size
        return size_in_bytes

class TensorMemoryObj:
    def __init__(
        self,
        raw_data: torch.Tensor,
        metadata: MemoryObjMetadata,
        parent_allocator: Any
    ): 
        self.raw_data = raw_data
        self.metadata = metadata
        self.parent_allocator = parent_allocator

    def get_size(self) -> int: 
        return self.metadata.get_size()
    
    @property
    def tensor(self) -> torch.Tensor: 
        return self.raw_data[: self.get_size()].view(self.metadata.dtype).view(self.metadata.shape)

    @property
    def byte_array(self) -> bytes: 
        """Returns a COPY of the data as bytes. Use get_writable_buffer for direct access."""
        num_bytes = self.raw_data.numel() * self.raw_data.element_size()
        ptr = self.raw_data.data_ptr()
        ubyte_ptr = ctypes.cast(ptr, ctypes.POINTER(ctypes.c_ubyte))
        # Keep reference to underlying memory
        byte_array = (ctypes.c_ubyte * num_bytes).from_address(
            ctypes.addressof(ubyte_ptr.contents)
        )
        return memoryview(byte_array)

    @property
    def data_ptr(self) -> int:
        return self.raw_data.data_ptr()
    
    def write_bytes(self, data: bytes, offset: int = 0):
        """Writes bytes directly to the underlying memory at the specified offset."""
        ctypes.memmove(self.data_ptr + offset, data, len(data))

# hardcoded (meta llama 8B)
# https://huggingface.co/meta-llama/Meta-Llama-3-8B/blob/main/config.json
kv_dtype = torch.bfloat16
# (num_layer, k and v, chunk_size, num_kv_heads, head_size)
kv_shape = (32, 2, 256, 8, 4096)

class MockLMCacheMemoryPool:
    def __init__(self, size: int, shape: torch.Size, dtype: torch.dtype):
        self.shape = shape
        self.dtype = dtype
        # Calculate size of one object
        self.obj_size = math.prod(shape) * dtype.itemsize
        
        self.tensor = torch.empty(size, dtype=torch.uint8, pin_memory=True)
        self.tensor.fill_(0)
        self.buffer = self.tensor.view(torch.uint8).flatten()
        self.buffer_size = self.buffer.numel() * self.buffer.element_size()

        # Align to object size
        self.paged_buffers = torch.split(self.buffer, self.obj_size, dim=0)

        self.free_blocks: deque[TensorMemoryObj] = deque()

        for buf in self.paged_buffers: 
            # Skip incomplete blocks at the end
            if buf.numel() * buf.element_size() < self.obj_size:
                continue
                
            metadata = MemoryObjMetadata(
                shape=self.shape,
                dtype=self.dtype,
                address=buf.data_ptr(),
                phys_size=buf.numel() * buf.element_size(),
            )
            mem_obj = TensorMemoryObj(
                raw_data=buf,
                metadata=metadata,
                parent_allocator=self,
            )
            self.free_blocks.append(mem_obj)

    def allocate(self) -> Optional[TensorMemoryObj]:
        if not self.free_blocks:
            print("No free blocks available")
            return None
        
        mem_obj = self.free_blocks.popleft()
        return mem_obj
    
    def free(self, mem_obj: TensorMemoryObj):
        self.free_blocks.append(mem_obj)

    def close(self):
        pass # torch handles freeing