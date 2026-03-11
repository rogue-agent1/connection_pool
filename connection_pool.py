#!/usr/bin/env python3
"""Connection pool — thread-safe resource pool with health checks.

One file. Zero deps. Does one thing well.

Generic pool for database connections, HTTP clients, etc. Supports min/max
sizing, idle timeout, health checks, and wait queue. Used in every web framework.
"""
import threading, time, sys, queue
from contextlib import contextmanager

class PooledConnection:
    __slots__ = ('id', 'created_at', 'last_used', 'in_use', 'healthy')
    _counter = 0

    def __init__(self):
        PooledConnection._counter += 1
        self.id = PooledConnection._counter
        self.created_at = time.time()
        self.last_used = self.created_at
        self.in_use = False
        self.healthy = True

    def execute(self, query):
        self.last_used = time.time()
        return f"[conn-{self.id}] executed: {query}"

    def ping(self):
        return self.healthy

    def close(self):
        self.healthy = False

    def __repr__(self):
        age = time.time() - self.created_at
        return f"Conn(id={self.id}, age={age:.1f}s, in_use={self.in_use})"

class ConnectionPool:
    def __init__(self, min_size=2, max_size=10, max_idle_sec=300,
                 acquire_timeout=5.0, health_check_interval=60):
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle = max_idle_sec
        self.acquire_timeout = acquire_timeout
        self.hc_interval = health_check_interval
        self._lock = threading.Lock()
        self._available = queue.Queue()
        self._all = []
        self._closed = False
        # Pre-fill minimum connections
        for _ in range(min_size):
            conn = self._create()
            self._available.put(conn)

    def _create(self):
        conn = PooledConnection()
        with self._lock:
            self._all.append(conn)
        return conn

    def acquire(self, timeout=None):
        timeout = timeout or self.acquire_timeout
        # Try to get an available connection
        try:
            conn = self._available.get(timeout=0.01)
            if conn.ping():
                conn.in_use = True
                conn.last_used = time.time()
                return conn
            else:
                self._remove(conn)
        except queue.Empty:
            pass
        # Create new if under max
        with self._lock:
            if len(self._all) < self.max_size:
                conn = PooledConnection()
                self._all.append(conn)
                conn.in_use = True
                return conn
        # Wait for one to be returned
        try:
            conn = self._available.get(timeout=timeout)
            conn.in_use = True
            conn.last_used = time.time()
            return conn
        except queue.Empty:
            raise TimeoutError(f"Pool exhausted (max={self.max_size}), waited {timeout}s")

    def release(self, conn):
        conn.in_use = False
        conn.last_used = time.time()
        if not self._closed and conn.healthy:
            self._available.put(conn)
        else:
            self._remove(conn)

    def _remove(self, conn):
        conn.close()
        with self._lock:
            if conn in self._all:
                self._all.remove(conn)

    @contextmanager
    def connection(self):
        conn = self.acquire()
        try:
            yield conn
        finally:
            self.release(conn)

    def evict_idle(self):
        now = time.time()
        evicted = 0
        with self._lock:
            to_remove = [c for c in self._all
                        if not c.in_use and now - c.last_used > self.max_idle
                        and len(self._all) > self.min_size]
        for c in to_remove:
            self._remove(c)
            evicted += 1
        return evicted

    def health_check(self):
        unhealthy = 0
        with self._lock:
            for c in list(self._all):
                if not c.in_use and not c.ping():
                    self._remove(c)
                    unhealthy += 1
        # Refill to min_size
        with self._lock:
            deficit = self.min_size - len(self._all)
        for _ in range(max(0, deficit)):
            conn = self._create()
            self._available.put(conn)
        return unhealthy

    def stats(self):
        with self._lock:
            total = len(self._all)
            in_use = sum(1 for c in self._all if c.in_use)
        return {"total": total, "in_use": in_use, "available": total - in_use,
                "max": self.max_size}

    def close(self):
        self._closed = True
        with self._lock:
            for c in self._all:
                c.close()
            self._all.clear()

def main():
    pool = ConnectionPool(min_size=2, max_size=5)
    print(f"Initial: {pool.stats()}")

    # Use connections
    with pool.connection() as conn:
        print(conn.execute("SELECT 1"))
    print(f"After use: {pool.stats()}")

    # Concurrent usage
    results = []
    def worker(i):
        with pool.connection() as conn:
            results.append(conn.execute(f"query-{i}"))
            time.sleep(0.01)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()
    print(f"\nConcurrent: {len(results)} queries completed")
    print(f"Pool: {pool.stats()}")

    # Health check
    with pool._lock:
        if pool._all:
            pool._all[0].healthy = False
    removed = pool.health_check()
    print(f"\nHealth check: {removed} unhealthy removed, {pool.stats()}")

    pool.close()
    print("Pool closed ✓")

if __name__ == "__main__":
    main()
