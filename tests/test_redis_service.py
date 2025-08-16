import asyncio
import time
from typing import AsyncGenerator, Awaitable, Callable, Generator

import fakeredis.aioredis
import pytest
import pytest_asyncio
from pydantic import AnyHttpUrl

from mush_finder import redis_service as svc
from mush_finder.schemas import HashTask, TaskStatus
from mush_finder.settings import settings


class _TestSettings:
    tasks_key = "tasks"
    queue_key = "queue"
    result_ttl = 3600
    task_timeout = 2
    max_retry_count = 3
    redis_url = "redis://localhost:6379/0"


@pytest.fixture(autouse=True)
def patch_settings(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    monkeypatch.setattr(svc, "settings", _TestSettings())
    monkeypatch.setattr(svc, "SLEEP_INTERVAL", 0.05)
    yield


@pytest_asyncio.fixture
async def fake_redis(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[fakeredis.aioredis.FakeRedis, None]:
    r = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(svc, "redis_client", r)
    await r.ping()
    await r.flushall()
    yield r
    await r.flushall()
    await r.aclose()  # type: ignore[attr-defined]
    monkeypatch.setattr(svc, "redis_client", None)


def _task_key(p_hash: str) -> str:
    return f"{svc.settings.tasks_key}:{p_hash}"


async def _seed_task(r: fakeredis.aioredis.FakeRedis, t: HashTask) -> None:
    await r.set(_task_key(t.p_hash), t.model_dump_json(), ex=svc.settings.result_ttl)


P_HASH = "0123456789abcdef" * 4
IMAGE_URL = AnyHttpUrl("https://notfound.image.com/")


@pytest.mark.asyncio
async def test_create_new_task(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """Successful creation of a new task"""

    new = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.queued,
        result=[],
        processed_at=0,
        retry_count=0,
    )

    res = await svc.new_redis_task(new)
    assert res.p_hash == P_HASH
    assert res.status == TaskStatus.queued

    data = await fake_redis.get(_task_key(P_HASH))
    assert data is not None

    q_item = await fake_redis.lpop(svc.settings.queue_key)
    assert q_item == P_HASH


@pytest.mark.asyncio
async def test_existing_done_returns_cached(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """There is an existing task that is done"""

    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.done,
        result=[],
        processed_at=int(time.time()),
        retry_count=0,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.new_redis_task(existing)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_existing_error_exceeded_retry_returns_cached(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """There is an existing task that is error and has exceeded the retry limit"""

    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.error,
        result=[],
        processed_at=int(time.time()),
        retry_count=svc.settings.max_retry_count,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.new_redis_task(existing)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_existing_error_should_not_retry_in_new_task(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """There is an existing task that is error but has not exceeded the retry limit."""

    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.error,
        result=[],
        processed_at=int(time.time()),
        retry_count=1,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.new_redis_task(existing)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_existing_queued_returns_cached(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """There is an existing task that is queued"""

    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.queued,
        result=[],
        processed_at=0,
        retry_count=0,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.new_redis_task(existing)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_existing_processing_timeout_should_not_retry_in_new_task(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    """There is an existing task that is processing and has timed out."""

    old_ts = int(time.time()) - (svc.settings.task_timeout + 5)
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.processing,
        result=[],
        processed_at=old_ts,
        retry_count=1,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.new_redis_task(existing)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_existing_processing_timeout_exceeded_retry_returns_cached(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    """There is an existing task that is processing and has timed out."""

    old_ts = int(time.time()) - (svc.settings.task_timeout + 5)
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.processing,
        result=[],
        processed_at=old_ts,
        retry_count=svc.settings.max_retry_count,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.new_redis_task(existing)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_existing_processing_not_timeout_returns_cached(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """There is an existing task that is processing and has not timed out."""

    recent_ts = int(time.time())
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.processing,
        result=[],
        processed_at=recent_ts,
        retry_count=0,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.new_redis_task(existing)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_get_or_retry_done_returns_cached(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """There is a done task"""
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.done,
        result=[],
        processed_at=int(time.time()),
        retry_count=0,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.get_or_retry_task(P_HASH)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_get_or_retry_error_exceeded_retry_returns_cached(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """error and retry is exceeded"""
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.error,
        result=[],
        processed_at=int(time.time()),
        retry_count=settings.max_retry_count,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.get_or_retry_task(P_HASH)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_get_or_retry_error_should_retry(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """error, and retry"""
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.error,
        result=[],
        processed_at=int(time.time()),
        retry_count=1,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.get_or_retry_task(P_HASH)
    assert res.p_hash == P_HASH
    assert res.status == TaskStatus.queued
    assert res.retry_count == existing.retry_count + 1
    assert await fake_redis.llen(svc.settings.queue_key) == 1
    assert await fake_redis.lpop(svc.settings.queue_key) == P_HASH


@pytest.mark.asyncio
async def test_get_or_retry_queued_returns_cached(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """queued, just return"""
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.queued,
        result=[],
        processed_at=0,
        retry_count=0,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.get_or_retry_task(P_HASH)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_get_or_retry_processing_timeout_should_retry(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """processing, timeout, retry"""
    old_ts = int(time.time()) - (svc.settings.task_timeout + 5)
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.processing,
        result=[],
        processed_at=old_ts,
        retry_count=1,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.get_or_retry_task(P_HASH)
    assert res.p_hash == P_HASH
    assert res.status == TaskStatus.queued
    assert res.retry_count == existing.retry_count + 1
    assert await fake_redis.llen(svc.settings.queue_key) == 1
    assert await fake_redis.lpop(svc.settings.queue_key) == P_HASH


@pytest.mark.asyncio
async def test_get_or_retry_processing_timeout_exceeded_retry_returns_cached(
    fake_redis: fakeredis.aioredis.FakeRedis,
) -> None:
    """processing, time out, retry exceeded"""
    old_ts = int(time.time()) - (svc.settings.task_timeout + 5)
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.processing,
        result=[],
        processed_at=old_ts,
        retry_count=svc.settings.max_retry_count,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.get_or_retry_task(P_HASH)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_get_or_retry_processing_not_timeout_returns_cached(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """processing, not timeout, just return"""
    recent_ts = int(time.time())
    existing = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.processing,
        result=[],
        processed_at=recent_ts,
        retry_count=0,
    )
    await _seed_task(fake_redis, existing)

    res = await svc.get_or_retry_task(P_HASH)
    assert res == existing
    assert await fake_redis.llen(svc.settings.queue_key) == 0


@pytest.mark.asyncio
async def test_get_or_retry_task_not_found(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    """not existing task, should return not_found status"""
    res = await svc.get_or_retry_task(P_HASH)
    assert res.status == TaskStatus.not_found
    assert await fake_redis.llen(svc.settings.queue_key) == 0


async def _run_worker_until(
    cond: Callable[[], Awaitable[bool]],
    handler: Callable[[HashTask], Awaitable[HashTask]],
) -> None:
    worker = asyncio.create_task(svc.consume_task(handler))
    try:
        for _ in range(300):
            if await cond():
                return
            await asyncio.sleep(0.01)
        pytest.fail("condition not met before timeout")
    finally:
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_consume_task_success(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    task = HashTask(
        p_hash=P_HASH, img_url=IMAGE_URL, status=TaskStatus.queued, result=[], processed_at=0, retry_count=0
    )
    await _seed_task(fake_redis, task)
    await fake_redis.rpush(svc.settings.queue_key, task.p_hash)

    async def handler(t: HashTask) -> HashTask:
        t.status = TaskStatus.done
        t.result = []
        return t

    async def cond() -> bool:
        raw = await fake_redis.get(_task_key(P_HASH))
        obj = svc._validate_task(raw) if raw else None
        return bool(obj and obj.status == TaskStatus.done and obj.result == [])

    await _run_worker_until(cond, handler)

    assert (await fake_redis.llen(svc.settings.queue_key) or 0) == 0


@pytest.mark.asyncio
async def test_consume_task_error_then_retry_once(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    task = HashTask(
        p_hash=P_HASH, img_url=IMAGE_URL, status=TaskStatus.queued, result=[], processed_at=0, retry_count=0
    )
    await _seed_task(fake_redis, task)
    await fake_redis.rpush(svc.settings.queue_key, task.p_hash)

    async def handler(t: HashTask) -> HashTask:
        raise RuntimeError("boom")

    async def cond() -> bool:
        raw = await fake_redis.get(_task_key(P_HASH))
        obj = svc._validate_task(raw) if raw else None
        qlen = await fake_redis.llen(svc.settings.queue_key)
        return bool(
            obj and obj.status == TaskStatus.error and obj.retry_count >= svc.settings.max_retry_count and qlen == 0
        )

    await _run_worker_until(cond, handler)


@pytest.mark.asyncio
async def test_consume_task_error_retry_exhausted(fake_redis: fakeredis.aioredis.FakeRedis) -> None:
    task = HashTask(
        p_hash=P_HASH,
        img_url=IMAGE_URL,
        status=TaskStatus.queued,
        result=[],
        processed_at=0,
        retry_count=svc.settings.max_retry_count,
    )
    await _seed_task(fake_redis, task)
    await fake_redis.rpush(svc.settings.queue_key, P_HASH)

    async def handler(t: HashTask) -> HashTask:
        raise RuntimeError("still failing")

    async def cond() -> bool:
        raw = await fake_redis.get(_task_key(P_HASH))
        obj = svc._validate_task(raw) if raw else None
        qlen = await fake_redis.llen(svc.settings.queue_key)
        return bool(
            obj and obj.status == TaskStatus.error and obj.retry_count == svc.settings.max_retry_count and qlen == 0
        )

    await _run_worker_until(cond, handler)
