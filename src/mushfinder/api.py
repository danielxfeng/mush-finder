from typing import Annotated

from fastapi import FastAPI, Path

from schemas import PHash, TaskBody, TaskResponse, TaskStatus

app = FastAPI()


@app.get("/", response_model=dict)
async def root():
    return {"status": "ok"}


@app.post("/new-task", response_model=TaskResponse)
async def new_task(task: TaskBody) -> TaskResponse:
    return TaskResponse(p_hash=task.p_hash, status=TaskStatus.queued)


@app.get("/task/{p_hash}", response_model=TaskResponse)
async def get_task(
    p_hash: Annotated[
        str, Path(title="PHash", description="Unique identifier for the task")
    ],
) -> TaskResponse:
    validated = PHash(p_hash=p_hash)
    return TaskResponse(p_hash=validated.p_hash, status=TaskStatus.queued)
