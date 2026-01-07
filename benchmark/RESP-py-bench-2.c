#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>
#include <stdbool.h>

#define DEFAULT_POOL_SIZE 1024
#define DEFAULT_BUFFER_SIZE (4 * 1024 * 1024)  // 4 MB
#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 6379
#define MAX_KEY_LEN 256
#define MAX_SIZE_HEADER_LEN 32

// ============================================================================
// STRUCTURES
// ============================================================================

typedef struct {
    char *data;
    size_t size;
} Buffer;

typedef struct {
    Buffer *buffers;
    int pool_size;
    size_t buffer_size;
} BufferPool;

typedef struct {
    int sockfd;
    size_t chunk_size;
    
    // Reusable headers for zero-copy operations
    char size_header[MAX_SIZE_HEADER_LEN];
    size_t size_header_len;
    
    // Pre-formatted command prefixes
    char get_prefix[32];
    size_t get_prefix_len;
    
    char set_prefix[32];
    size_t set_prefix_len;
    
    char exists_prefix[32];
    size_t exists_prefix_len;
} RedisClient;

typedef struct {
    RedisClient *clients;
    pthread_t *threads;
    pthread_mutex_t *mutexes;
    int num_threads;
    int current_idx;
    pthread_mutex_t dispatch_mutex;
} MultiThreadedRedisClient;

typedef struct {
    RedisClient *client;
    BufferPool *pool;
    int start_idx;
    int end_idx;
    const char *operation;  // "write" or "read"
} WorkerArgs;

// ============================================================================
// BUFFER POOL
// ============================================================================

BufferPool* buffer_pool_create(int pool_size, size_t buffer_size) {
    BufferPool *pool = malloc(sizeof(BufferPool));
    if (!pool) {
        perror("Failed to allocate buffer pool");
        return NULL;
    }
    
    pool->pool_size = pool_size;
    pool->buffer_size = buffer_size;
    pool->buffers = malloc(sizeof(Buffer) * pool_size);
    
    if (!pool->buffers) {
        perror("Failed to allocate buffer array");
        free(pool);
        return NULL;
    }
    
    for (int i = 0; i < pool_size; i++) {
        pool->buffers[i].data = malloc(buffer_size);
        pool->buffers[i].size = buffer_size;
        
        if (!pool->buffers[i].data) {
            perror("Failed to allocate buffer");
            // Clean up previously allocated buffers
            for (int j = 0; j < i; j++) {
                free(pool->buffers[j].data);
            }
            free(pool->buffers);
            free(pool);
            return NULL;
        }
        
        // Fill with some data (optional, for testing)
        memset(pool->buffers[i].data, 'A' + (i % 26), buffer_size);
    }
    
    return pool;
}

void buffer_pool_destroy(BufferPool *pool) {
    if (!pool) return;
    
    for (int i = 0; i < pool->pool_size; i++) {
        free(pool->buffers[i].data);
    }
    free(pool->buffers);
    free(pool);
}

size_t buffer_pool_total_size(BufferPool *pool) {
    return (size_t)pool->pool_size * pool->buffer_size;
}

// ============================================================================
// REDIS CLIENT - SOCKET HELPERS
// ============================================================================

int redis_connect(const char *host, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sockfd);
        return -1;
    }
    
    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sockfd);
        return -1;
    }
    
    return sockfd;
}

ssize_t recv_exactly(int sockfd, void *buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = recv(sockfd, (char*)buf + total, len - total, 0);
        if (n <= 0) {
            if (n == 0) {
                fprintf(stderr, "Connection closed during recv_exactly\n");
            } else {
                perror("recv");
            }
            return -1;
        }
        total += n;
    }
    return total;
}

ssize_t send_multipart(int sockfd, struct iovec *iov, int iovcnt) {
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = iovcnt;
    
    size_t total_to_send = 0;
    for (int i = 0; i < iovcnt; i++) {
        total_to_send += iov[i].iov_len;
    }
    
    size_t total_sent = 0;
    int current_iov = 0;
    size_t iov_offset = 0;
    
    while (total_sent < total_to_send && current_iov < iovcnt) {
        // Update msg structure with remaining iovecs
        msg.msg_iov = &iov[current_iov];
        msg.msg_iovlen = iovcnt - current_iov;
        
        // Adjust first iovec if we have an offset
        struct iovec temp_iov;
        if (iov_offset > 0) {
            temp_iov.iov_base = (char*)iov[current_iov].iov_base + iov_offset;
            temp_iov.iov_len = iov[current_iov].iov_len - iov_offset;
            
            struct iovec *remaining_iovs = malloc(sizeof(struct iovec) * (iovcnt - current_iov));
            remaining_iovs[0] = temp_iov;
            for (int i = 1; i < iovcnt - current_iov; i++) {
                remaining_iovs[i] = iov[current_iov + i];
            }
            
            msg.msg_iov = remaining_iovs;
            ssize_t n = sendmsg(sockfd, &msg, 0);
            free(remaining_iovs);
            
            if (n <= 0) {
                if (n == 0) {
                    fprintf(stderr, "Connection closed during sendmsg\n");
                } else {
                    perror("sendmsg");
                }
                return -1;
            }
            
            total_sent += n;
            
            // Update position
            size_t consumed = n;
            if (consumed >= temp_iov.iov_len) {
                consumed -= temp_iov.iov_len;
                current_iov++;
                iov_offset = 0;
                
                while (consumed > 0 && current_iov < iovcnt) {
                    if (consumed >= iov[current_iov].iov_len) {
                        consumed -= iov[current_iov].iov_len;
                        current_iov++;
                    } else {
                        iov_offset = consumed;
                        consumed = 0;
                    }
                }
            } else {
                iov_offset += consumed;
            }
        } else {
            ssize_t n = sendmsg(sockfd, &msg, 0);
            
            if (n <= 0) {
                if (n == 0) {
                    fprintf(stderr, "Connection closed during sendmsg\n");
                } else {
                    perror("sendmsg");
                }
                return -1;
            }
            
            total_sent += n;
            
            // Update position
            size_t consumed = n;
            while (consumed > 0 && current_iov < iovcnt) {
                if (consumed >= iov[current_iov].iov_len) {
                    consumed -= iov[current_iov].iov_len;
                    current_iov++;
                } else {
                    iov_offset = consumed;
                    consumed = 0;
                }
            }
        }
    }
    
    return total_sent;
}

// ============================================================================
// REDIS CLIENT - INITIALIZATION
// ============================================================================

RedisClient* redis_client_create(const char *host, int port, size_t buffer_size) {
    RedisClient *client = malloc(sizeof(RedisClient));
    if (!client) {
        perror("Failed to allocate redis client");
        return NULL;
    }
    
    client->sockfd = redis_connect(host, port);
    if (client->sockfd < 0) {
        free(client);
        return NULL;
    }
    
    client->chunk_size = buffer_size;
    
    // Generate size header
    client->size_header_len = snprintf(client->size_header, MAX_SIZE_HEADER_LEN, 
                                       "$%zu\r\n", buffer_size);
    
    // Generate command prefixes
    client->get_prefix_len = snprintf(client->get_prefix, 32, "*2\r\n$3\r\nGET\r\n");
    client->set_prefix_len = snprintf(client->set_prefix, 32, "*3\r\n$3\r\nSET\r\n");
    client->exists_prefix_len = snprintf(client->exists_prefix, 32, "*2\r\n$6\r\nEXISTS\r\n");
    
    return client;
}

void redis_client_destroy(RedisClient *client) {
    if (!client) return;
    if (client->sockfd >= 0) {
        close(client->sockfd);
    }
    free(client);
}

// ============================================================================
// REDIS CLIENT - COMMANDS
// ============================================================================

int redis_set(RedisClient *client, const char *key, const char *value, size_t value_len) {
    assert(value_len == client->chunk_size);
    
    char key_header[MAX_SIZE_HEADER_LEN];
    size_t key_len = strlen(key);
    size_t key_header_len = snprintf(key_header, MAX_SIZE_HEADER_LEN, "$%zu\r\n", key_len);
    
    // Build scatter-gather message
    struct iovec iov[7];
    int iovcnt = 0;
    
    // *3\r\n$3\r\nSET\r\n
    iov[iovcnt].iov_base = client->set_prefix;
    iov[iovcnt].iov_len = client->set_prefix_len;
    iovcnt++;
    
    // $<key_len>\r\n
    iov[iovcnt].iov_base = key_header;
    iov[iovcnt].iov_len = key_header_len;
    iovcnt++;
    
    // <key>
    iov[iovcnt].iov_base = (void*)key;
    iov[iovcnt].iov_len = key_len;
    iovcnt++;
    
    // \r\n
    iov[iovcnt].iov_base = "\r\n";
    iov[iovcnt].iov_len = 2;
    iovcnt++;
    
    // $<value_len>\r\n
    iov[iovcnt].iov_base = client->size_header;
    iov[iovcnt].iov_len = client->size_header_len;
    iovcnt++;
    
    // <value>
    iov[iovcnt].iov_base = (void*)value;
    iov[iovcnt].iov_len = value_len;
    iovcnt++;
    
    // \r\n
    iov[iovcnt].iov_base = "\r\n";
    iov[iovcnt].iov_len = 2;
    iovcnt++;
    
    if (send_multipart(client->sockfd, iov, iovcnt) < 0) {
        return -1;
    }
    
    // Expect +OK\r\n response
    char response[5];
    if (recv_exactly(client->sockfd, response, 5) < 0) {
        return -1;
    }
    
    if (memcmp(response, "+OK\r\n", 5) != 0) {
        fprintf(stderr, "SET command returned invalid response\n");
        return -1;
    }
    
    return 0;
}

int redis_get(RedisClient *client, const char *key, char *recv_buf, size_t recv_buf_len) {
    assert(recv_buf_len == client->chunk_size);
    
    char key_header[MAX_SIZE_HEADER_LEN];
    size_t key_len = strlen(key);
    size_t key_header_len = snprintf(key_header, MAX_SIZE_HEADER_LEN, "$%zu\r\n", key_len);
    
    // Build scatter-gather message
    struct iovec iov[4];
    int iovcnt = 0;
    
    // *2\r\n$3\r\nGET\r\n
    iov[iovcnt].iov_base = client->get_prefix;
    iov[iovcnt].iov_len = client->get_prefix_len;
    iovcnt++;
    
    // $<key_len>\r\n
    iov[iovcnt].iov_base = key_header;
    iov[iovcnt].iov_len = key_header_len;
    iovcnt++;
    
    // <key>
    iov[iovcnt].iov_base = (void*)key;
    iov[iovcnt].iov_len = key_len;
    iovcnt++;
    
    // \r\n
    iov[iovcnt].iov_base = "\r\n";
    iov[iovcnt].iov_len = 2;
    iovcnt++;
    
    if (send_multipart(client->sockfd, iov, iovcnt) < 0) {
        return -1;
    }
    
    // Read size header
    char size_header_response[MAX_SIZE_HEADER_LEN];
    if (recv_exactly(client->sockfd, size_header_response, client->size_header_len) < 0) {
        return -1;
    }
    
    if (memcmp(size_header_response, client->size_header, client->size_header_len) != 0) {
        fprintf(stderr, "GET command returned invalid size header\n");
        return -1;
    }
    
    // Read payload directly into recv_buf (zero-copy)
    if (recv_exactly(client->sockfd, recv_buf, client->chunk_size) < 0) {
        return -1;
    }
    
    // Read trailer \r\n
    char trailer[2];
    if (recv_exactly(client->sockfd, trailer, 2) < 0) {
        return -1;
    }
    
    if (memcmp(trailer, "\r\n", 2) != 0) {
        fprintf(stderr, "GET command returned invalid trailer\n");
        return -1;
    }
    
    return 0;
}

bool redis_exists(RedisClient *client, const char *key) {
    char key_header[MAX_SIZE_HEADER_LEN];
    size_t key_len = strlen(key);
    size_t key_header_len = snprintf(key_header, MAX_SIZE_HEADER_LEN, "$%zu\r\n", key_len);
    
    // Build scatter-gather message
    struct iovec iov[4];
    int iovcnt = 0;
    
    // *2\r\n$6\r\nEXISTS\r\n
    iov[iovcnt].iov_base = client->exists_prefix;
    iov[iovcnt].iov_len = client->exists_prefix_len;
    iovcnt++;
    
    // $<key_len>\r\n
    iov[iovcnt].iov_base = key_header;
    iov[iovcnt].iov_len = key_header_len;
    iovcnt++;
    
    // <key>
    iov[iovcnt].iov_base = (void*)key;
    iov[iovcnt].iov_len = key_len;
    iovcnt++;
    
    // \r\n
    iov[iovcnt].iov_base = "\r\n";
    iov[iovcnt].iov_len = 2;
    iovcnt++;
    
    if (send_multipart(client->sockfd, iov, iovcnt) < 0) {
        return false;
    }
    
    // Read response :0\r\n or :1\r\n
    char response[4];
    if (recv_exactly(client->sockfd, response, 4) < 0) {
        return false;
    }
    
    if (memcmp(response, ":1\r\n", 4) == 0) {
        return true;
    } else if (memcmp(response, ":0\r\n", 4) == 0) {
        return false;
    } else {
        fprintf(stderr, "EXISTS command returned invalid response\n");
        return false;
    }
}

// ============================================================================
// BENCHMARK FUNCTIONS - SINGLE THREADED
// ============================================================================

void benchmark_write(RedisClient *client, BufferPool *pool) {
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    
    for (int i = 0; i < pool->pool_size; i++) {
        char key[MAX_KEY_LEN];
        snprintf(key, MAX_KEY_LEN, "chunk_%d", i);
        
        if (redis_set(client, key, pool->buffers[i].data, pool->buffers[i].size) < 0) {
            fprintf(stderr, "Failed to SET chunk_%d\n", i);
            return;
        }
    }
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    double gb = buffer_pool_total_size(pool) / (1024.0 * 1024.0 * 1024.0);
    double throughput = gb / elapsed;
    
    printf("Wrote %.2f GB in %.3f seconds, which is: %.3f GB/s\n", gb, elapsed, throughput);
}

void benchmark_read(RedisClient *client, BufferPool *pool) {
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    
    for (int i = 0; i < pool->pool_size; i++) {
        char key[MAX_KEY_LEN];
        snprintf(key, MAX_KEY_LEN, "chunk_%d", i);
        
        if (redis_get(client, key, pool->buffers[i].data, pool->buffers[i].size) < 0) {
            fprintf(stderr, "Failed to GET chunk_%d\n", i);
            return;
        }
    }
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    double gb = buffer_pool_total_size(pool) / (1024.0 * 1024.0 * 1024.0);
    double throughput = gb / elapsed;
    
    printf("Read %.2f GB in %.3f seconds, which is: %.3f GB/s\n", gb, elapsed, throughput);
}

void test_exists(RedisClient *client, BufferPool *pool) {
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    
    for (int i = 0; i < pool->pool_size; i++) {
        char key[MAX_KEY_LEN];
        snprintf(key, MAX_KEY_LEN, "chunk_%d", i);
        
        if (!redis_exists(client, key)) {
            fprintf(stderr, "chunk_%d does not exist after write\n", i);
            return;
        }
    }
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    printf("Tested %d exists in %.3f seconds\n", pool->pool_size, elapsed);
}

// ============================================================================
// MULTI-THREADED CLIENT
// ============================================================================

void* worker_thread_write(void *arg) {
    WorkerArgs *args = (WorkerArgs*)arg;
    
    for (int i = args->start_idx; i < args->end_idx; i++) {
        char key[MAX_KEY_LEN];
        snprintf(key, MAX_KEY_LEN, "chunk_%d", i);
        
        if (redis_set(args->client, key, args->pool->buffers[i].data, 
                      args->pool->buffers[i].size) < 0) {
            fprintf(stderr, "Failed to SET chunk_%d\n", i);
            return NULL;
        }
    }
    
    return NULL;
}

void* worker_thread_read(void *arg) {
    WorkerArgs *args = (WorkerArgs*)arg;
    
    for (int i = args->start_idx; i < args->end_idx; i++) {
        char key[MAX_KEY_LEN];
        snprintf(key, MAX_KEY_LEN, "chunk_%d", i);
        
        if (redis_get(args->client, key, args->pool->buffers[i].data,
                      args->pool->buffers[i].size) < 0) {
            fprintf(stderr, "Failed to GET chunk_%d\n", i);
            return NULL;
        }
    }
    
    return NULL;
}

MultiThreadedRedisClient* multi_threaded_client_create(const char *host, int port, 
                                                        size_t buffer_size, int num_threads) {
    MultiThreadedRedisClient *client = malloc(sizeof(MultiThreadedRedisClient));
    if (!client) {
        perror("Failed to allocate multi-threaded client");
        return NULL;
    }
    
    client->num_threads = num_threads;
    client->current_idx = 0;
    pthread_mutex_init(&client->dispatch_mutex, NULL);
    
    client->clients = malloc(sizeof(RedisClient) * num_threads);
    client->threads = malloc(sizeof(pthread_t) * num_threads);
    client->mutexes = malloc(sizeof(pthread_mutex_t) * num_threads);
    
    if (!client->clients || !client->threads || !client->mutexes) {
        perror("Failed to allocate client resources");
        free(client->clients);
        free(client->threads);
        free(client->mutexes);
        free(client);
        return NULL;
    }
    
    // Create a connection for each thread
    for (int i = 0; i < num_threads; i++) {
        RedisClient *rc = redis_client_create(host, port, buffer_size);
        if (!rc) {
            fprintf(stderr, "Failed to create redis client for thread %d\n", i);
            // Clean up
            for (int j = 0; j < i; j++) {
                redis_client_destroy(&client->clients[j]);
            }
            free(client->clients);
            free(client->threads);
            free(client->mutexes);
            free(client);
            return NULL;
        }
        client->clients[i] = *rc;
        free(rc);  // We copied the struct, free the wrapper
        pthread_mutex_init(&client->mutexes[i], NULL);
    }
    
    return client;
}

void multi_threaded_client_destroy(MultiThreadedRedisClient *client) {
    if (!client) return;
    
    for (int i = 0; i < client->num_threads; i++) {
        if (client->clients[i].sockfd >= 0) {
            close(client->clients[i].sockfd);
        }
        pthread_mutex_destroy(&client->mutexes[i]);
    }
    
    pthread_mutex_destroy(&client->dispatch_mutex);
    free(client->clients);
    free(client->threads);
    free(client->mutexes);
    free(client);
}

void benchmark_write_concurrent(MultiThreadedRedisClient *client, BufferPool *pool) {
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    
    WorkerArgs *args = malloc(sizeof(WorkerArgs) * client->num_threads);
    
    int chunk_per_thread = pool->pool_size / client->num_threads;
    int remainder = pool->pool_size % client->num_threads;
    
    int start_idx = 0;
    for (int i = 0; i < client->num_threads; i++) {
        int count = chunk_per_thread + (i < remainder ? 1 : 0);
        
        args[i].client = &client->clients[i];
        args[i].pool = pool;
        args[i].start_idx = start_idx;
        args[i].end_idx = start_idx + count;
        args[i].operation = "write";
        
        pthread_create(&client->threads[i], NULL, worker_thread_write, &args[i]);
        
        start_idx += count;
    }
    
    // Wait for all threads
    for (int i = 0; i < client->num_threads; i++) {
        pthread_join(client->threads[i], NULL);
    }
    
    free(args);
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    double gb = buffer_pool_total_size(pool) / (1024.0 * 1024.0 * 1024.0);
    double throughput = gb / elapsed;
    
    printf("Wrote %.2f GB in %.3f s  →  %.3f GB/s\n", gb, elapsed, throughput);
}

void benchmark_read_concurrent(MultiThreadedRedisClient *client, BufferPool *pool) {
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    
    WorkerArgs *args = malloc(sizeof(WorkerArgs) * client->num_threads);
    
    int chunk_per_thread = pool->pool_size / client->num_threads;
    int remainder = pool->pool_size % client->num_threads;
    
    int start_idx = 0;
    for (int i = 0; i < client->num_threads; i++) {
        int count = chunk_per_thread + (i < remainder ? 1 : 0);
        
        args[i].client = &client->clients[i];
        args[i].pool = pool;
        args[i].start_idx = start_idx;
        args[i].end_idx = start_idx + count;
        args[i].operation = "read";
        
        pthread_create(&client->threads[i], NULL, worker_thread_read, &args[i]);
        
        start_idx += count;
    }
    
    // Wait for all threads
    for (int i = 0; i < client->num_threads; i++) {
        pthread_join(client->threads[i], NULL);
    }
    
    free(args);
    
    clock_gettime(CLOCK_MONOTONIC, &end);
    
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
    double gb = buffer_pool_total_size(pool) / (1024.0 * 1024.0 * 1024.0);
    double throughput = gb / elapsed;
    
    printf("Read  %.2f GB in %.3f s  →  %.3f GB/s\n", gb, elapsed, throughput);
}

// ============================================================================
// MAIN
// ============================================================================

void print_usage(const char *prog) {
    printf("Usage: %s [OPTIONS]\n", prog);
    printf("Options:\n");
    printf("  --pool-size NUM      Number of buffers in pool (default: %d)\n", DEFAULT_POOL_SIZE);
    printf("  --buffer-size NUM    Size of each buffer in bytes (default: %d)\n", DEFAULT_BUFFER_SIZE);
    printf("  --num-threads NUM    Number of threads for concurrent benchmark\n");
    printf("  --help               Show this help message\n");
}

int main(int argc, char **argv) {
    int pool_size = DEFAULT_POOL_SIZE;
    size_t buffer_size = DEFAULT_BUFFER_SIZE;
    int num_threads = 0;  // 0 means single-threaded
    
    // Parse arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--pool-size") == 0 && i + 1 < argc) {
            pool_size = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--buffer-size") == 0 && i + 1 < argc) {
            buffer_size = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--num-threads") == 0 && i + 1 < argc) {
            num_threads = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }
    
    // Create buffer pool
    BufferPool *pool = buffer_pool_create(pool_size, buffer_size);
    if (!pool) {
        fprintf(stderr, "Failed to create buffer pool\n");
        return 1;
    }
    
    if (num_threads == 0) {
        // Single-threaded benchmark
        printf("Running single threaded benchmark\n");
        
        RedisClient *client = redis_client_create(DEFAULT_HOST, DEFAULT_PORT, buffer_size);
        if (!client) {
            fprintf(stderr, "Failed to create redis client\n");
            buffer_pool_destroy(pool);
            return 1;
        }
        
        benchmark_write(client, pool);
        test_exists(client, pool);
        benchmark_read(client, pool);
        
        redis_client_destroy(client);
    } else {
        // Multi-threaded benchmark
        printf("Running multi threaded benchmark with %d threads\n", num_threads);
        
        MultiThreadedRedisClient *client = multi_threaded_client_create(
            DEFAULT_HOST, DEFAULT_PORT, buffer_size, num_threads
        );
        
        if (!client) {
            fprintf(stderr, "Failed to create multi-threaded redis client\n");
            buffer_pool_destroy(pool);
            return 1;
        }
        
        benchmark_write_concurrent(client, pool);
        // Note: test_exists requires single client, skipping for multi-threaded
        benchmark_read_concurrent(client, pool);
        
        multi_threaded_client_destroy(client);
    }
    
    buffer_pool_destroy(pool);
    
    return 0;
}

