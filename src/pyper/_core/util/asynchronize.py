from __future__ import annotations

import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import functools
from typing import overload

from ..task import Task, PipelineTask


@overload
def asynchronize(task: PipelineTask, tp: ThreadPoolExecutor, pp: ProcessPoolExecutor) -> PipelineTask: ...


@overload
def asynchronize(task: Task, tp: ThreadPoolExecutor, pp: ProcessPoolExecutor) -> Task: ...


def asynchronize(task: Task, tp: ThreadPoolExecutor, pp: ProcessPoolExecutor) -> Task:
    """Unify async and sync tasks as awaitable futures.
    1. If the task is async already, return it.
    2. Multiprocessed synchronous functions are wrapped in a call to `run_in_executor` using `ProcessPoolExecutor`.
    3. Threaded synchronous functions are wrapped in a call to `run_in_executor` using `ThreadPoolExecutor`.
    """
    if task.is_async:
        return task
    
    if task.is_gen:
        # Small optimization to convert sync generators to async functions
        # This saves from having to use a thread/process just to get the generator object
        @functools.wraps(task.func)
        async def wrapper(*args, **kwargs):
            return task.func(*args, **kwargs)
    else:
        executor = pp if task.multiprocess else tp
        @functools.wraps(task.func)
        async def wrapper(*args, **kwargs):
            loop = asyncio.get_running_loop()
            f = functools.partial(task.func, *args, **kwargs)
            return await loop.run_in_executor(executor=executor, func=f)
    
    if isinstance(task, PipelineTask):
        return PipelineTask(
            wrapper,
            branch=task.branch,
            join=task.join,
            workers=task.workers,
            throttle=task.throttle
        )
    return Task(wrapper)
