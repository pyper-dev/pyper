---
title: Advanced Concepts
parent: User Guide
layout: default
nav_order: 4
permalink: /docs/UserGuide/AdvancedConcepts
---

# Advanced Concepts
{: .no_toc }

* TOC
{:toc}

## Threads vs Processes vs Async

Whereas threading and asynchronous code are Python's way of achieving concurrency, multiprocessing is the answer for parallelism. 

Pyper supports all three modes of execution by coordinating different types of workers:

* Synchronous tasks by default are handled by [threads](https://docs.python.org/3/library/threading.html)
* Synchronous tasks set with `multiprocess=True` are handled by [processes](https://docs.python.org/3/library/multiprocessing.html)
* Asynchronous tasks are handled by [asyncio Tasks](https://docs.python.org/3/library/asyncio-task.html)


Concurrency and parallelism are powerful constructs that allow us to squeeze the best possible performance out of our code.
To leverage these mechanisms optimally, however, we need to consider the type of work being done by each task; primarily, whether this work is [io-bound or cpu-bound](https://stackoverflow.com/questions/868568).


### IO-bound work

An IO-bound task is one that can make progress off the CPU after releasing the [GIL](https://wiki.python.org/moin/GlobalInterpreterLock), by doing something that doesn't require computation. For example by:

* Performing a sleep
* Sending a network request
* Reading from a database

IO-bound tasks benefit from both concurrent and parallel execution.
However, to avoid the overhead costs of creating processes, it is generally preferable to use either threading or async code.

{: .info}
Threads incur a higher overhead cost compared to async coroutines, but are suitable if your application prefers or requires a synchronous implementation

Note that asynchronous functions need to `await` or `yield` something in order to benefit from concurrency.
Any long-running call in an async task which does not yield execution will prevent other tasks from making progress:

```python
# Okay
def slow_func():
    time.sleep(5)

# Okay
async def slow_func():
    await asyncio.sleep(5)

# Bad -- cannot benefit from concurrency
async def slow_func():
    time.sleep(5)
```

### CPU-bound work

A CPU-bound function is one that hogs the CPU intensely, without releasing the GIL. This includes all 'heavy-computation' type operations like:

* Crunching numbers
* Parsing text data
* Sorting and searching

{: .warning}
Executing CPU-bound tasks concurrently does not improve performance, as CPU-bound tasks do not make progress while not holding the GIL

The correct way to optimize the performance of CPU-bound tasks is through parallel execution, using multiprocessing.

```python
# Okay
@task(workers=10, multiprocess=True)
def long_computation(data: int):
    for i in range(1, 1_000_000):
        data *= i
    return data

# Bad -- cannot benefit from concurrency
@task(workers=10)
def long_computation(data: int):
    for i in range(1, 1_000_000):
        data *= i
    return data
```

Note, however, that processes incur a very high overhead cost (performance in creation and memory in maintaining inter-process communication). Specific cases should be benchmarked to fine-tune the task parameters for your program / your machine.

### Summary

|                       | Threading | Multiprocessing | Async   |
|:----------------------|:----------|:----------------|:--------|
| Overhead costs        | Moderate  | High            | Low     |
| Synchronous execution | ✅        | ✅             | ❌      | 
| IO-bound work         | ⬆️        | ⬆️             | ⬆️      |
| CPU-bound work        | ❌        | ⬆️             | ❌      |

{: .text-green-200}
**Key Considerations:**

* If a task is doing extremely expensive CPU-bound work, define it synchronously and set `multiprocess=True`
* If a task is doing expensive IO-bound work, consider implementing it asynchronously, or use threads
* Do _not_ put expensive, blocking work in an async task, as this clogs up the async event loop

## Functional Design

### Logical Separation

Writing clean code is partly about defining functions with single, clear responsibilities.

In Pyper specifically, it is especially important to separate out different types of work into different tasks if we want to optimize their performance. For example, consider a task which performs an IO-bound network request along with a CPU-bound function to parse the data.

```python
# Bad -- functions not separated
@task(workers=20)
def get_data(endpoint: str):
    # IO-bound work
    r = requests.get(endpoint)
    data = r.json()
    
    # CPU-bound work
    return process_data(data)
```

Whilst it makes sense to handle the network request concurrently, the call to `process_data` within the same task is blocking and will harm concurrency.
Instead, `process_data` can be implemented as a separate task:

```python
@task(workers=20)
def get_data(endpoint: str):
    # IO-bound work
    r = requests.get(endpoint)
    return r.json()
    
@task(workers=10, multiprocess=True)
def process_data(data):
    # CPU-bound work
    ...
```

### Resource Management

It is often useful to share resources between different tasks, like http sessions or database connections.
The correct pattern is generally to define functions which take these resources as arguments.

```python
from aiohttp import ClientSession
from pyper import task

async def list_user_ids(session: ClientSession) -> list[int]:
    async with session.get("/users") as r:
        return await r.json()

async def fetch_user_data(user_id: int, session: ClientSession) -> dict:
    async with session.get(f"/users/{user_id}") as r:
        return await r.json()
```

When defining a pipeline, these additional arguments are plugged into tasks using `task.bind`. For example:

```python
async def main():
    async with ClientSession("http://localhost:8000/api") as session:
        user_data_pipeline = (
            task(list_user_ids, branch=True, bind=task.bind(session=session))
            | task(fetch_user_data, workers=10, bind=task.bind(session=session))
        )
        async for output in user_data_pipeline():
            print(output)
```

This is preferable to defining custom set-up and tear-down mechanisms, because it relies on Python's intrinsic mechanism for set-up and tear-down: using `with` syntax.
However, this requires us to define and run the pipeline within the resource's context, which means it can't be used modularly in other data flows.

If we want `user_data_pipeline` to be reusable, a simple solution is to create a factory function or factory class which uses the session resource internally. For example:

```python
from aiohttp import ClientSession
from pyper import task, AsyncPipeline

def user_data_pipeline(session: ClientSession) -> AsyncPipeline:

    async def list_user_ids() -> list[int]:
        async with session.get("/users") as r:
            return await r.json()

    async def fetch_user_data(user_id: int) -> dict:
        async with session.get(f"/users/{user_id}") as r:
            return await r.json()
    
    return (
        task(list_user_ids, branch=True)
        | task(fetch_user_data, workers=10)
    )
```

Now `user_data_pipeline` constructs a self-contained data-flow, which can be reused without having to define its internal pipeline everytime.

```python
async def main():
    async with ClientSession("http://localhost:8000/api") as session:
        run = (
            user_data_pipeline(session)
            | task(write_to_file, join=True)
            > copy_to_db
        )
        await run()
```