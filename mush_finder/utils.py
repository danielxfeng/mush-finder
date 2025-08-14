from mush_finder.schemas import HashTask, TaskResponse


def adaptor_hash_task_to_response(task: HashTask) -> TaskResponse:
    return TaskResponse(
        p_hash=task.p_hash,
        status=task.status,
        result=task.result,
    )
