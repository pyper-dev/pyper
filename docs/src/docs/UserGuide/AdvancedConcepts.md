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
Threads incur a higher overhead cost compared to async coroutines, but are suitable if the task prefers or requires a synchronous implementation

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
def long_computation(data: int):
    for i in range(1, 1_000_000):
        data *= i
    return data

# Okay
pipeline = task(long_computation, workers=10, multiprocess=True)

# Bad -- cannot benefit from concurrency
pipeline = task(long_computation, workers=10)
```

Note, however, that processes incur a very high overhead cost (performance cost in creation and memory cost in inter-process communication). Specific cases should be benchmarked to fine-tune the task parameters for your program / your machine.

### Summary

|                       | Threading | Multiprocessing | Async   |
|:----------------------|:----------|:----------------|:--------|
| Overhead costs        | Moderate  | High            | Low     |
| Synchronous execution | ✅        | ✅             | ❌      | 
| IO-bound work         | ⬆️        | ⬆️             | ⬆️      |
| CPU-bound work        | ❌        | ⬆️             | ❌      |

{: .text-green-200}
**Key Considerations:**

* If a task is doing expensive CPU-bound work, define it synchronously and set `multiprocess=True`
* If a task is doing expensive IO-bound work, consider implementing it asynchronously, or use threads
* Do _not_ put expensive, blocking work in an async task, as this clogs up the async event loop

## Functional Design

### Logical Separation

Writing clean code is partly about defining functions with single, clear responsibilities.

In Pyper, it is especially important to separate out different types of work into different tasks if we want to optimize their performance. For example, consider a task which performs an IO-bound network request along with a CPU-bound function to parse the data.

```python
# Bad -- functions not separated
def get_data(endpoint: str):
    # IO-bound work
    r = requests.get(endpoint)
    data = r.json()
    
    # CPU-bound work
    for item in data["results"]:
        yield process_data(item)

pipeline = task(get_data, branch=True, workers=20)
```

Whilst it makes sense to handle the network request concurrently, the call to `process_data` within the same task requires holding onto the GIL and will harm concurrency.
Instead, `process_data` should be implemented as a separate function:

```python
def get_data(endpoint: str):
    # IO-bound work
    r = requests.get(endpoint)
    data = r.json()
    return data["results"]
    
def process_data(data):
    # CPU-bound work
    return ...

pipeline = (
    task(get_data, branch=True, workers=20)
    | task(process_data, workers=10, multiprocess=True)
)
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
            task(list_user_ids, branch=True)
            | task(fetch_user_data, workers=10, bind=task.bind(session=session))
        )
        async for output in user_data_pipeline(session):
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

## Generators

### Usage

Generators in Python are a mechanism for _lazy execution_, whereby results in an iterable are returned one by one (via underlying calls to `__next__`) instead of within a data structure, like a `list`, which requires all of its elements to be allocated in memory.

Using generators is an indispensible approach for processing large volumes of data in a memory-friendly way. We can define generator functions by using the `yield` keyword within a normal `def` block:

```python
import typing
from pyper import task

# Okay
def generate_values_lazily() -> typing.Iterable[dict]:
    for i in range(10_000_000):
        yield {"data": i}

# Bad -- this creates 10 million values in memory
# Within a pipeline, subsequent tasks also cannot start executing until the entire list is created
def create_values_in_list() -> typing.List[dict]:
    return [{"data": i} for i in range(10_000_000)]
```

{: .info}
Generator `functions` return immediately. They return `generator` objects, which are iterable

Using the `branch` task parameter in Pyper allows generators to generate multiple outputs, which get picked up by subsequent tasks as soon as the data is available.

Using a generator function without `branch=True` is also possible; this just means the task submits `generator` objects as output, instead of each generated value.

```python
from pyper import task

def get_data():
    yield 1
    yield 2
    yield 3

if __name__ == "__main__":
    branched_pipeline = task(get_data, branch=True)
    for output in branched_pipeline():
        print(output)
        #> 1
        #> 2
        #> 3

    non_branched_pipeline = task(get_data)
    for output in non_branched_pipeline():
        print(output)
        #> <generator object get_data at ...>
```

### Limitations

Implementing generator objects in a pipeline can also come with some caveats that are important to keep in mind.

{: .text-green-200}
**Synchronous Generators with Asynchronous Code**

Synchronous generators in an `AsyncPipeline` do not benefit from threading or multiprocessing.

This is because, in order to be scheduled in an async event loop, each synchronous task is run by a thread/process, and then wrapped in an `asyncio.Task`.

Generator functions, which return _immediately_, do most of their work outside of the thread/process and this synchronous work will therefore not benefit from multiple workers in an async context.

The alternatives are to:

1. Refactor your functions. If you find that one function is repeating a computation multiple times, it may be possible to [separate out responsibilities](#logical-separation) into separate functions

2. Use a synchronous generator anyway (if its performance is unlikely to be a bottleneck)

3. Use a normal synchronous function, and return an iterable data structure (if memory is unlikely to be a bottleneck)

4. Use an async generator (if an async implementation of the function is appropriate)

{: .text-green-200}
**Multiprocessing and Pickling**

In Python, anything that goes into and comes out of a process must be picklable.

On Windows, generator objects cannot be pickled, so cannot be passed as inputs and outputs when multiprocessing.

Note that, for example, using `branch=True` to pass individual outputs from a generator into a multiprocessed task is still fine, because the task input would not be a `generator` object.
