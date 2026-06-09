import asyncpg
import logging

from config.config import Config

logger = logging.getLogger(__name__)

# Module-level pool — None until init_pool() is called at service startup.
# A module-level variable is a true singleton: every import of this module
# in the same Python process shares the exact same _pool reference.
_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    """
    Creates and starts the asyncpg connection pool.

    Must be called ONCE at service startup (in consumer.py's main()) before
    any database operations are attempted.

    asyncpg pools maintain a set of live PostgreSQL connections and lend them
    to callers on demand, then reclaim them when the caller is done. This means
    zero TCP connection setup/teardown overhead per query — every handler simply
    borrows a connection, executes its query, and returns the connection to the
    pool for the next caller.

    min_size: connections kept open even when idle (avoids cold-start latency).
    max_size: upper bound — prevents overwhelming the database under burst load.
    """
    global _pool
    config = Config()

    _pool = await asyncpg.create_pool(
        host=config.DB_HOST,
        port=int(config.DB_PORT),
        database=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        min_size=2,
        max_size=10,
    )
    logger.info(
        f"DB connection pool initialized: "
        f"host={config.DB_HOST}:{config.DB_PORT} db={config.DB_NAME} "
        f"pool_size=2–10"
    )
    return _pool


async def close_pool() -> None:
    """
    Gracefully drains and closes all connections in the pool.

    Must be called during service shutdown to avoid leaving dangling
    connections open on the database server side.
    """
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("DB connection pool closed.")


def get_pool() -> asyncpg.Pool:
    """
    Returns the active connection pool.

    Raises RuntimeError if init_pool() was not called first. Failing loudly
    at startup is far preferable to a silent AttributeError deep inside a
    message handler at runtime.
    """
    if _pool is None:
        raise RuntimeError(
            "Database pool has not been initialized. "
            "Call `await init_pool()` in your service's main() before processing messages."
        )
    return _pool
