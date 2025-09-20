# redis_helper.py
import redis
import os

_pool = None

def get_redis() -> redis.Redis:
    global _pool
    if _pool is None:
        _pool = redis.ConnectionPool.from_url(os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"))
    return redis.Redis(connection_pool=_pool)

def get_pubsub():
    r = get_redis()
    p = r.pubsub()
    return r, p
