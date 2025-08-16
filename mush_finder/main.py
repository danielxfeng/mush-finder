import secrets
from contextlib import asynccontextmanager
from typing import Annotated, AsyncGenerator, Callable
from urllib.request import Request

from fastapi import FastAPI, HTTPException, Path, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from mush_finder.redis_service import (
    close_redis,
    get_or_retry_task,
    health_check,
    init_redis,
    new_redis_task,
)
from mush_finder.schemas import HashTask, PHash, TaskBody, TaskResponse, TaskStatus
from mush_finder.settings import settings
from mush_finder.utils import adaptor_hash_task_to_response


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await init_redis()
    yield
    await close_redis()


app = FastAPI(lifespan=lifespan, root_path="/api")


# API Key Verification Middleware
@app.middleware("http")
async def verify_api_key(request: Request, call_next: Callable) -> object:
    if not bool(getattr(settings, "is_prod", False)) or not getattr(settings, "api_key", None):
        return await call_next(request)

    api_key = request.headers.get("x-api-key") or ""
    if not secrets.compare_digest(api_key, str(getattr(settings, "api_key", ""))):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return await call_next(request)


cors_origins = (
    ["*"]
    if (settings.cors_origins == "*" or not settings.is_prod)
    else [str(origin) for origin in settings.cors_origins]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)


@app.middleware("http")
async def add_security_headers(request: Request, call_next: Callable) -> Response:
    response: Response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
    response.headers["Content-Security-Policy"] = "default-src 'self'"
    return response


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


@app.exception_handler(Exception)
async def unified_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, RequestValidationError):
        return JSONResponse(
            status_code=422,
            content={"detail": "Validation error", "errors": exc.errors()},
        )

    if isinstance(exc, StarletteHTTPException):
        if exc.status_code == 404:
            return JSONResponse(status_code=404, content={"detail": "Not Found"})
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": "Error"} if settings.is_prod else {"detail": exc.detail},
        )

    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
