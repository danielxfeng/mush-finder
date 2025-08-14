from contextlib import asynccontextmanager
from typing import Annotated, AsyncGenerator

from fastapi import FastAPI, Path

from mush_finder.redis_service import close_redis, get_or_retry_task, health_check, init_redis, new_redis_task
from mush_finder.schemas import HashTask, PHash, TaskBody, TaskResponse, TaskStatus
from mush_finder.utils import adaptor_hash_task_to_response


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await init_redis()
    yield
    await close_redis()


app = FastAPI(lifespan=lifespan)


@app.get("/", response_model=dict)
async def root() -> dict:
    return await health_check()


@app.post("/new-task", response_model=TaskResponse)
async def new_task(task: TaskBody) -> TaskResponse:
    hash_task = HashTask(
        p_hash=task.p_hash, img_url=task.img_url, status=TaskStatus.queued, result=[], processed_at=0, retry_count=0
    )

    res = await new_redis_task(hash_task)
    return adaptor_hash_task_to_response(res)


@app.get("/task/{p_hash}", response_model=TaskResponse)
async def get_task(
    p_hash: Annotated[str, Path(title="PHash", description="Unique identifier for the task")],
) -> TaskResponse:
    validated = PHash(p_hash=p_hash)
    res = await get_or_retry_task(validated.p_hash)
    return adaptor_hash_task_to_response(res)
