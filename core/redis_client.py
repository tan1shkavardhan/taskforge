import os
import redis
from dotenv import load_dotenv
from core.config import REDIS_HOST, REDIS_PORT, REDIS_DB

load_dotenv()

def get_redis():
    pool = redis.ConnectionPool(
        host=os.getenv("REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        decode_responses=True
    )
    client = redis.Redis(connection_pool=pool)

    # Fail fast if Memurai isn't reachable
    try:
        client.ping()
    except redis.exceptions.ConnectionError:
        raise RuntimeError("Cannot connect to Memurai (Redis). Is the service running?")

    return client