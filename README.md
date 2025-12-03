# Motivation / Work Steps

We know that redis server and client can reach ~6 GB throughput.

## 1. `memtier_benchmark` and `redis-benchmark` baseline

Server Deployment:
```bash
git clone https://github.com/redis/redis.git
cd redis
git checkout 8.2
make -j
./src/redis-server --protected-mode no --save '' --appendonly no --io-threads 4
```

Benchmark: 
https://github.com/RedisLabs/memtier_benchmark
```bash
# write first
memtier_benchmark --ratio=1:0 -n allkeys --data-size=4194304 --key-minimum=1 --key-maximum=10000 --key-pattern=P:P --pipeline=64 --clients=10 --threads=1 --hide-histogram -h localhost

Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec 
Sets          372.97          ---          ---      1666.60255      1671.16700      2916.35100      3751.93500   1527716.54 
# 1.527716 GB / s

# then read
memtier_benchmark --ratio=0:1 -n 500 --key-minimum=1 --key-maximum=10000 --key-pattern=R:R --clients=20 --threads=4 --hide-histogram -h localhost

Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec 
Gets         1427.53      1427.53         0.00        55.52572        51.19900       142.33500       258.04700   5847236.14 
# 5.847236 GB /s

# simultaneous
memtier_benchmark --ratio=1:1 -n 1000 --data-size=4194304 --key-minimum=1 --key-maximum=10000 --key-pattern=R:R --clients=20 --threads=4 --pipeline=10 --hide-histogram -h localhost
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec 
Sets          843.74          ---          ---       465.03350       456.70300       897.02300      1638.39900   3456009.11 
Gets          843.74       843.74         0.00       465.42984       456.70300       897.02300      1744.89500   3456005.01 
# 3.456009 GB / s
```

Redis-benchmark: 
```bash
redis-benchmark -n 1000 -r 1000 -d 33554432 -t set -P 1 -c 100
139.20 requests per second
# WRITE: 4.6707769344 GB / s


redis-benchmark -n 1000 -r 1000 -d 33554432 -t get -P 1 -c 100
43.97 requests per second
# READ: 1.47538837504  GB / s
```

## 2. `redis-py` benchmark (what lmcache currently uses and is farily slow)

Use same server setup. 

An example implementation is in `benchmark/lmcache-redis-bench.py` that mimics what LMCache is currently using:

```bash
python redis-py-bench.py --client-type single-pool --size-mb 32 --num-chunks 100 --read --concurrent
Using Single Client Pool
Read 3.125 GB in 4.816915512084961 seconds, which is: 0.6487554104197627 GB/s
```

## 3. A new simple wrapper around code stolen from `memtier_benchmark`

We want something faster than `redis-py` and truly zero copy. `memtier_benchmark` tells us that it should be possible to 
write a better client. 

We only need to implement GET and SET, using RESP. A python reference is in `RESP.py`. However, this reference is limited by python's `memoryview` interface. 

```bash
pip install lmcache_redis
```

This SDK will be considered complete once we can implement `benchmark/lmcache_redis-bench.py`

The minimal interface will be something like: 
```python
# ***This is the wheel that we are trying to build***
import lmcache_redis
"""
the minimal python interface we want to implement:

redis_client = lmcache_redis.RedisClient(host="localhost", port=6379)
redis_client.set("key", buffer: bytes)
redis_client.get("key", buffer: bytes)
"""
```

### Useful refrences are in `references`:
1. `lmcache_memory_model.py`: this is a mock up of the PagedTensorAllocator. the zero copy should happen with / to the memory objects in this pool.
2. `RESP.py`: this is a python example of RESP. 

### Project Structure: 
`lmcache_redis/`:
- `pyproject.toml`: establish the pybindings
- `csrc/`: take source files from `memtier_benchmark`

### Possible Escape Hatches: (that make this SDK unecessary)
- some configuration / client using `redis-py` achieves top performance
- some benchmarking tool built with some deriviation of `RESP.py` achieves top performance (work on `RESP-py-bench.py`)

Current: 

```bash
python redis-py-bench.py --size-mb 32 --num-chunks 100 --read
# 
python redis-py-bench.py --size-mb 32 --num-chunks 100 --write
# 
python redis-py-bench.py --size-mb 32 --num-chunks 100 --read --write
#
```

# Scratch Paper / Documentation

`RESP-py-bench.py`


```bash
# custom client is about twice as fast
python RESP-py-bench.py --size-mb 32 --num-chunks 100 --read --client-type custom

(lmcache_redis) tensormesh@GPU-H100-lccn11:~/jiayis-dad/lmcache_redis/benchmark$ python RESP-py-bench.py --size-mb 32 --num-chunks 100 --read --client-type custom
Using Custom Zero-Copy Client (Threaded Wrapper)
Generating 100 chunks of 32MB...
--- Starting Read Test ---
Read 3.12 GB in 2.64s | Speed: 1.18 GB/s
(lmcache_redis) tensormesh@GPU-H100-lccn11:~/jiayis-dad/lmcache_redis/benchmark$ python RESP-py-bench.py --size-mb 32 --num-chunks 100 --read --client-type redis-py
Using redis-py (Async)
Generating 100 chunks of 32MB...
--- Starting Read Test ---
Read 3.12 GB in 6.98s | Speed: 0.45 GB/s
```