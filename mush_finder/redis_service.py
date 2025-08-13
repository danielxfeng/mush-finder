import asyncio
import time
from typing import Awaitable, Callable, Literal, cast

from redis.asyncio.client import Redis
from redis.exceptions import WatchError

from mush_finder.schemas import HashTask, TaskStatus
from mush_finder.settings import settings

redis_client: Redis | None = None
SLEEP_INTERVAL = 10


async def init_redis() -> None:
    global redis_client

    redis_client = Redis.from_url(
        cast(str, settings.redis_url),
        decode_responses=True,
        health_check_interval=30,
        retry_on_timeout=True,
        socket_timeout=5,
    )


async def close_redis() -> None:
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None


def get_redis() -> Redis:
    if redis_client is None:
        raise RuntimeError("Redis client not initialized")
    return redis_client


async def health_check() -> dict:
    r = get_redis()
    await r.set("status", "ok")
    return {"message": await r.get("ok")}


async def _add_or_retry_task(task: HashTask, mode: Literal["add", "retry"]) -> HashTask | None:
    r = get_redis()
    key = settings.tasks_key

    while True:
        try:
            await r.watch(key)

            data = await r.hget(key, task.p_hash)

            if mode == "add" and data:
                return HashTask.model_validate_json(data)
            elif mode == "retry" and not data:
                return None

            if mode == "retry":
                task.status = TaskStatus.queued
                task.retry_count += 1

            pipe = r.pipeline()
            pipe.hset(key, task.p_hash, task.model_dump_json())
            pipe.expire(key, settings.result_ttl)
            pipe.lpush(settings.queue_key, task.p_hash)

            await pipe.execute()
            return None if mode == "add" else task

        except WatchError:
            continue
        finally:
            await r.unwatch()


async def get_or_retry_task(p_hash: str) -> HashTask | None:
    r = get_redis()
    key = settings.tasks_key

    data = await r.hget(key, p_hash)
    if not data:
        return HashTask(p_hash=p_hash, status=TaskStatus.not_found, result=[], retry_count=0, processed_at=0)

    task = HashTask.model_validate_json(data)

    now = int(time.time())
    is_task_timeout = task.status == TaskStatus.processing and now - task.processed_at >= settings.task_timeout
    is_task_final_err = (
        task.status == TaskStatus.error or is_task_timeout
    ) and task.retry_count >= settings.max_retry_count

    if task.status in (TaskStatus.done, TaskStatus.queued) or is_task_final_err:
        return task

    if task.status == TaskStatus.error or is_task_timeout:
        return await _add_or_retry_task(task, mode="retry")

    raise ValueError(f"Unexpected task status: {task.status}")


async def get_or_add_task(task: HashTask) -> HashTask | None:
    return await _add_or_retry_task(task, mode="add")


async def dispatch_tasks_with_limit() -> None:
    r = get_redis()
    while True:
        if await r.llen(settings.worker_queue_key) >= settings.workers:
            await asyncio.sleep(SLEEP_INTERVAL)
            continue

        p_hash = await r.lpop(settings.queue_key)
        if not p_hash:
            await asyncio.sleep(SLEEP_INTERVAL)
            continue

        await r.lpush(settings.worker_queue_key, p_hash)


TaskHandler = Callable[[HashTask], Awaitable[HashTask]]


async def perform_tasks_with_timeout(task_handler: TaskHandler) -> None:
    r = get_redis()
    while True:
        p_hash = await r.lpop(settings.worker_queue_key)
        if not p_hash:
            await asyncio.sleep(SLEEP_INTERVAL)
            continue

        data = await r.hget(settings.tasks_key, p_hash)
        if not data:
            continue

        task = HashTask.model_validate_json(data)
        if task.status != TaskStatus.queued:
            continue

        task.status = TaskStatus.processing
        task.processed_at = int(time.time())

        await r.hset(settings.tasks_key, p_hash, task.model_dump_json())

        try:
            result = await asyncio.wait_for(task_handler(task), timeout=settings.task_timeout)
            await r.hset(settings.tasks_key, p_hash, result.model_dump_json())
        except Exception:
            task.status = TaskStatus.error
            await r.hset(settings.tasks_key, p_hash, task.model_dump_json())
            await _add_or_retry_task(task, mode="retry")


__all__ = [
    "get_redis",
    "health_check",
    "get_or_retry_task",
    "get_or_add_task",
    "dispatch_tasks_with_limit",
    "perform_tasks_with_timeout",
]
