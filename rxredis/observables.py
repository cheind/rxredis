from typing import Any, Optional, Tuple, Union

import redis
from reactivex import Observable, abc
from reactivex.disposable import CompositeDisposable, Disposable
from reactivex.scheduler import CurrentThreadScheduler
from redis import Redis
from redis.client import PubSub

from . import utils


StreamData = dict
StreamDataWithId = Tuple[str, dict]


def from_stream(
    redis_api: Redis,
    stream: str,
    stream_id: str = "$",
    batch: int = 1,
    timeout: float = 0.5,
    complete_on_timeout: bool = False,
    latest: bool = False,
    scheduler: Optional[abc.SchedulerBase] = None,
) -> Observable[StreamDataWithId]:
    """Turns a Redis stream into an observable sequence.

    Params:
        redis_api: Redis client
        stream: Redis stream name
        stream_id: Stream id to be considered last read. Special tokens are '$' and '>',
            with '>' setting stream id to last available at point of subscription.
        batch: batch size per call. When greater 1, batch elements are emitted as fast
            as possible.
        timeout: Timeout in seconds
        complete_on_timeout: When true, this observable completes once no elements within
            timeout period can be read.
        latest: When true and batch-size greater than one will emit only the latest
            item of batch and ignore the rest.
        scheduler: Scheduler instance to schedule the values on

    Returns:
        The observable sequence whose elements are pulled from the given Redis stream.
        Each element is a tuple of stream-id and value dict: StreamDataWithId.
    """

    def subscribe(
        observer: abc.ObserverBase[StreamDataWithId],
        scheduler_: Optional[abc.SchedulerBase] = None,
    ) -> abc.DisposableBase:
        nonlocal stream_id
        _scheduler = scheduler or scheduler_ or CurrentThreadScheduler.singleton()
        disposed = False

        sid = stream_id
        if sid == ">":
            # Handle start with next entry after join (best-effort)
            try:
                resp = redis_api.xinfo_stream(stream)
                sid = resp["last-entry"][0]
            except redis.ResponseError:
                # Stream not available
                sid = "0"

        def from_stream_impl(_: abc.SchedulerBase, __: Any = None) -> None:
            nonlocal disposed, sid

            try:
                while not disposed:
                    resp = redis_api.xread(
                        {stream: sid}, count=batch, block=int(timeout * 1e3)
                    )
                    if len(resp) == 0:
                        # Handle timeout behavior
                        if complete_on_timeout:
                            observer.on_completed()
                    else:
                        # Handle data
                        resp = resp[0][1]  # one stream only
                        if latest:
                            resp = resp[-1:]  # latest item only
                        for rid, value in resp:
                            observer.on_next((rid, value))

                        # Update last seen
                        if len(resp) > 0:
                            sid = resp[-1][0]
            except Exception as error:  # pylint: disable=broad-except
                # Handle error
                observer.on_error(error)

        def dispose() -> None:
            nonlocal disposed
            disposed = True

        disp = Disposable(dispose)
        return CompositeDisposable(_scheduler.schedule(from_stream_impl), disp)

    return Observable(subscribe)


def from_pubsub(
    redis_api: Redis,
    pattern: Union[str, list[str]],
    timeout: float = 0.5,
    complete_on_timeout: bool = False,
    scheduler: Optional[abc.SchedulerBase] = None,
) -> Observable[StreamDataWithId]:
    """Turns a Redis stream into an observable sequence.

    Params:
        redis_api: Redis client
        pattern: Pubsub pattern to subscribe to
        batch: batch size per call. When greater 1, batch elements are emitted as fast
            as possible.
        timeout: Timeout in seconds
        complete_on_timeout: When true, this observable completes once no elements within
            timeout period can be read.
        latest: When true and batch-size greater than one will emit only the latest
            item of batch and ignore the rest.
        scheduler: Scheduler instance to schedule the values on

    Returns:
        The observable sequence whose elements are pulled from the given Redis stream.
        Each element is a tuple of stream-id and value dict: StreamDataWithId.
    """

    if isinstance(pattern, str):
        pattern = [pattern]

    def subscribe(
        observer: abc.ObserverBase[StreamDataWithId],
        scheduler_: Optional[abc.SchedulerBase] = None,
    ) -> abc.DisposableBase:
        _scheduler = scheduler or scheduler_ or CurrentThreadScheduler.singleton()
        disposed = False

        def from_stream_impl(_: abc.SchedulerBase, __: Any = None) -> None:
            nonlocal disposed
            pubsub = redis_api.pubsub(ignore_subscribe_messages=True)
            pubsub.psubscribe(*pattern)

            try:
                while not disposed:
                    resp = pubsub.get_message(timeout=timeout)
                    if resp is None:
                        # Handle timeout behavior
                        if complete_on_timeout:
                            observer.on_completed()
                    else:
                        # Handle data
                        t = redis_api.time()
                        tc = str(int(round(t[0] * 1e3 + t[1] * 1e-3)))

                        observer.on_next(
                            (tc, {"channel": resp["channel"], "message": resp["data"]})
                        )
            except Exception as error:  # pylint: disable=broad-except
                # Handle error
                observer.on_error(error)
            finally:
                pubsub.punsubscribe(*pattern)
                pubsub.close()

        def dispose() -> None:
            nonlocal disposed
            disposed = True

        disp = Disposable(dispose)
        return CompositeDisposable(_scheduler.schedule(from_stream_impl), disp)

    return Observable(subscribe)


__all__ = ["from_stream", "from_pubsub"]
