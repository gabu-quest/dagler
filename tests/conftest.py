"""Shared fixtures for dagler tests."""

import asyncio
import contextlib

import pytest_asyncio
from qler.queue import Queue
from qler.worker import Worker
from sqler import AsyncSQLerDB


@pytest_asyncio.fixture
async def db():
    """In-memory sqler database for testing."""
    _db = AsyncSQLerDB.in_memory(shared=False)
    await _db.connect()
    yield _db
    await _db.close()


@pytest_asyncio.fixture
async def queue(db):
    """Initialized qler Queue bound to the in-memory database."""
    q = Queue(db)
    await q.init_db()
    yield q


@pytest_asyncio.fixture
async def worker(queue):
    """qler Worker with fast test defaults.  Auto-stopped after test."""
    w = Worker(queue, poll_interval=0.01, concurrency=1, shutdown_timeout=2.0)
    worker_task = asyncio.create_task(w.run())
    yield w
    w._running = False
    await asyncio.sleep(0.05)
    worker_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await worker_task
