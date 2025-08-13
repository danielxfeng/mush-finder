from contextlib import asynccontextmanager
from typing import Annotated, AsyncGenerator

from fastapi import FastAPI, Path

from mush_finder.redis_service import close_redis, health_check, init_redis
from schemas import PHash, TaskBody, TaskResponse, TaskStatus

app = FastAPI()


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
    return TaskResponse(p_hash=task.p_hash, status=TaskStatus.queued)


@app.get("/task/{p_hash}", response_model=TaskResponse)
async def get_task(
    p_hash: Annotated[str, Path(title="PHash", description="Unique identifier for the task")],
) -> TaskResponse:
    validated = PHash(p_hash=p_hash)
    return TaskResponse(p_hash=validated.p_hash, status=TaskStatus.queued)
