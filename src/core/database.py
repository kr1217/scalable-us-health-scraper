import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from motor.motor_asyncio import AsyncIOMotorClient
from .config import settings

# --- PostgreSQL (SqlAlchemy v2 + asyncpg) ---
# Create async engine with single connection pool for stability on Windows
pg_engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
    pool_size=5,          # Increse pool size for batch operations
    max_overflow=10,      # Allow some overflow
    pool_pre_ping=True,   # Check connection health before use
    pool_recycle=3600,
    connect_args={"server_settings": {"application_name": "scraper"}}
)

# Async session factory
AsyncSessionLocal = async_sessionmaker(
    bind=pg_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

# Base class for SQLAlchemy models
Base = declarative_base()

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from .config import settings

# --- MongoDB (Motor) ---
def get_mongo_client():
    "Returns a new Motor client using settings."
    return AsyncIOMotorClient(settings.MONGODB_URI)

async def get_mongo_db(client: AsyncIOMotorClient):
    "Utility to get the mongo database from a given client."
    return client[settings.MONGODB_DB_NAME]

async def create_tables():
    "Create database tables defined in SQLAlchemy models."
    from . import orm_models  # Import models to register them with Base.metadata
    async with pg_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
