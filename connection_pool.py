#!/usr/bin/env python3
"""Connection Pool — bounded pool with health checks and timeouts."""
import threading, time, queue

class Connection:
    _id = 0
    def __init__(self):
        Connection._id += 1; self.id = Connection._id
        self.created = time.monotonic(); self.last_used = self.created; self.alive = True
    def ping(self): return self.alive
    def close(self): self.alive = False

class ConnectionPool:
    def __init__(self, factory=Connection, min_size=2, max_size=10, max_idle=60):
        self.factory = factory; self.min_size = min_size; self.max_size = max_size
        self.max_idle = max_idle; self.pool = queue.Queue(max_size)
        self.active = 0; self.lock = threading.Lock()
        self.stats = {'acquired': 0, 'released': 0, 'created': 0, 'evicted': 0}
        for _ in range(min_size): self._add_connection()
    def _add_connection(self):
        conn = self.factory(); self.pool.put(conn); self.stats['created'] += 1
    def acquire(self, timeout=5.0):
        try:
            conn = self.pool.get(timeout=timeout)
            if not conn.ping() or time.monotonic() - conn.last_used > self.max_idle:
                conn.close(); self.stats['evicted'] += 1
                conn = self.factory(); self.stats['created'] += 1
            conn.last_used = time.monotonic()
            with self.lock: self.active += 1; self.stats['acquired'] += 1
            return conn
        except queue.Empty: raise TimeoutError("Pool exhausted")
    def release(self, conn):
        conn.last_used = time.monotonic()
        with self.lock: self.active -= 1; self.stats['released'] += 1
        self.pool.put(conn)
    def close_all(self):
        while not self.pool.empty():
            try: self.pool.get_nowait().close()
            except queue.Empty: break

if __name__ == "__main__":
    pool = ConnectionPool(min_size=3, max_size=5)
    conns = [pool.acquire() for _ in range(3)]
    print(f"Active: {pool.active}, Available: {pool.pool.qsize()}")
    for c in conns: pool.release(c)
    print(f"After release: active={pool.active}, available={pool.pool.qsize()}")
    print(f"Stats: {pool.stats}")
