from typing import cast

import pytest
from pydantic import AnyHttpUrl, ValidationError

from mush_finder.schemas import TaskBody, TaskResponse, TaskResult, TaskStatus

img = cast(AnyHttpUrl, "https://example.com/test.jpg")


def test_task_body_valid():
    body = TaskBody(p_hash="a" * 64, img_url=img)
    assert body.p_hash == "a" * 64


def test_task_body_invalid_hash_too_short():
    with pytest.raises(ValidationError):
        TaskBody(p_hash="invalid_hash", img_url=img)


def test_task_body_invalid_hash_invalid_char():
    with pytest.raises(ValidationError):
        TaskBody(p_hash="a" * 63 + "#", img_url=img)


def test_task_body_invalid_prefix():
    with pytest.raises(ValidationError):
        TaskBody(
            p_hash="a" * 64, img_url=cast(AnyHttpUrl, "http://other.com/image.jpg")
        )


def test_task_response_default_result():
    resp = TaskResponse(p_hash="a" * 64, status=TaskStatus.queued)
    assert resp.result == []


def test_task_response_with_result():
    resp = TaskResponse(
        p_hash="a" * 64,
        status=TaskStatus.done,
        result=[TaskResult(category="mushroom", confidence=0.9)],
    )
    assert len(resp.result) == 1
    assert resp.result[0].category == "mushroom"


def test_task_status_invalid_result():
    with pytest.raises(ValidationError):
        TaskResponse(
            p_hash="a" * 64,
            status=TaskStatus.done,
            result=[TaskResult(category="mushroom", confidence=1.1)],
        )
