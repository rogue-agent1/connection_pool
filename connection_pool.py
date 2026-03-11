#!/usr/bin/env python3
"""connection_pool — Generic connection pool with health checks. Zero deps."""
import time, threading
from collections import deque

class Connection:
    _id = 0
    def __init__(self):
        Connection._id += 1
        self.id = Connection._id
        self.created = time.monotonic()
        self.last_used = self.created
        self.healthy = True
    def __repr__(self): return f"Conn#{self.id}"

class ConnectionPool:
    def __init__(self, factory, min_size=2, max_size=10, max_idle=60, health_check=None):
        self.factory = factory
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle = max_idle
        self.health_check = health_check or (lambda c: c.healthy)
        self.pool = deque()
        self.in_use = set()
        self.lock = threading.Lock()
        self.total_created = 0
        self.total_reused = 0
        for _ in range(min_size):
            self._create()

    def _create(self):
        conn = self.factory()
        self.total_created += 1
        self.pool.append(conn)
        return conn

    def acquire(self, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                while self.pool:
                    conn = self.pool.popleft()
                    if self.health_check(conn):
                        self.in_use.add(conn.id)
                        conn.last_used = time.monotonic()
                        self.total_reused += 1
                        return conn
                if len(self.in_use) < self.max_size:
                    conn = self._create()
                    self.pool.popleft()  # remove from pool
                    self.in_use.add(conn.id)
                    return conn
            time.sleep(0.01)
        raise TimeoutError("Pool exhausted")

    def release(self, conn):
        with self.lock:
            self.in_use.discard(conn.id)
            conn.last_used = time.monotonic()
            self.pool.append(conn)

    def evict_idle(self):
        with self.lock:
            now = time.monotonic()
            keep = deque()
            evicted = 0
            for conn in self.pool:
                if now - conn.last_used > self.max_idle and len(keep) >= self.min_size:
                    evicted += 1
                else:
                    keep.append(conn)
            self.pool = keep
            return evicted

    @property
    def stats(self):
        return {
            'available': len(self.pool), 'in_use': len(self.in_use),
            'created': self.total_created, 'reused': self.total_reused,
            'hit_rate': self.total_reused / (self.total_reused + self.total_created) if self.total_created else 0
        }

def main():
    pool = ConnectionPool(Connection, min_size=2, max_size=5)
    print("Connection Pool:\n")
    conns = []
    for i in range(4):
        c = pool.acquire()
        print(f"  Acquired: {c}")
        conns.append(c)
    print(f"  Stats: {pool.stats}")
    for c in conns:
        pool.release(c)
    c2 = pool.acquire()
    print(f"  Reused: {c2}")
    pool.release(c2)
    print(f"  Final stats: {pool.stats}")

if __name__ == "__main__":
    main()
