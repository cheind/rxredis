from typing import Any, Optional, Tuple

import redis
from reactivex import Observable, abc
from reactivex.disposable import CompositeDisposable, Disposable
from reactivex.scheduler import CurrentThreadScheduler
from redis import Redis

StreamDataWithId = Tuple[str, dict]


def from_stream(
    redis_api: Redis,
    stream: str,
    stream_id: str = "$",
    batch: int = 1,
    timeout: int = 500,
    complete_on_timeout: bool = False,
    scheduler: Optional[abc.SchedulerBase] = None,
) -> Observable[StreamDataWithId]:
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

        def from_xstream_impl(_: abc.SchedulerBase, __: Any = None) -> None:
            nonlocal disposed, sid

            try:
                while not disposed:
                    resp = redis_api.xread({stream: sid}, count=batch, block=timeout)
                    if len(resp) == 0:
                        # Handle timeout behavior
                        if complete_on_timeout:
                            observer.on_completed()
                    else:
                        # Handle data
                        resp = resp[0][1]  # one stream only
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
        return CompositeDisposable(_scheduler.schedule(from_xstream_impl), disp)

    return Observable(subscribe)

__all__ = ['from_stream']