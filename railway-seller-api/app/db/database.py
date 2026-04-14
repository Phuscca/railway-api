import os
from pathlib import Path
import asyncpg

_pool = None


def get_settings():
    return {
        'database_url': os.getenv('DATABASE_URL', ''),
        'api_key': os.getenv('API_KEY', 'change-me'),
        'bot_username': os.getenv('BOT_USERNAME', 'Phuc_bdstrongtamtay_bot'),
        'auto_init_schema': os.getenv('AUTO_INIT_SCHEMA', 'false').lower() == 'true',
    }


async def connect_db():
    global _pool
    settings = get_settings()
    if not settings['database_url']:
        raise RuntimeError('DATABASE_URL is not configured')
    _pool = await asyncpg.create_pool(dsn=settings['database_url'], min_size=1, max_size=5)
    if settings['auto_init_schema']:
        await init_schema()
    return _pool


async def close_db():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool():
    if _pool is None:
        raise RuntimeError('Database pool is not initialized')
    return _pool


async def init_schema():
    schema_path = Path(__file__).resolve().parents[2] / 'schema.sql'
    sql = schema_path.read_text(encoding='utf-8')
    async with get_pool().acquire() as conn:
        await conn.execute(sql)
