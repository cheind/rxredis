from typing import Optional, Callable, Union

from reactivex import Observable
from reactivex import abc

from redis import Redis

from .observables import StreamDataWithId

MapStr = Callable[[StreamDataWithId], str]
StrOrMapStr = Union[str, MapStr]


def to_stream(
    redis_api: Redis,
    stream: StrOrMapStr,
    relay_streamid: bool = False,
    max_len: int = 500,
) -> Callable[[Observable[StreamDataWithId]], Observable[StreamDataWithId]]:

    def to_xstream_impl(source: Observable[StreamDataWithId]) -> Observable[StreamDataWithId]:
        def subscribe(
            observer: abc.ObserverBase[StreamDataWithId],
            scheduler: Optional[abc.SchedulerBase] = None,
        ) -> abc.DisposableBase:
            def on_next(x: StreamDataWithId) -> None:
                xstream = stream if isinstance(stream, str) else stream(x)
                xid = x[0] if relay_streamid else '*'
                try:
                    redis_api.xadd(
                        name=xstream, fields=x[1], id=xid, maxlen=max_len
                    )
                except Exception as e:
                    observer.on_error(e)
                observer.on_next(x)

            return source.subscribe(
                on_next, observer.on_error, observer.on_completed, scheduler=scheduler
            )

        return Observable(subscribe)

    return to_xstream_impl

__all__ = ['to_stream']