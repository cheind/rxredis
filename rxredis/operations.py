from typing import Optional, TypeVar, Callable, Union

from reactivex import Observable
from reactivex import abc

from redis import Redis

_T = TypeVar("_T")
MapStr = Callable[[_T], str]
StrOrMapStr = Union[str, MapStr]
MapDict = Callable[[_T], dict]


def to_stream(
    redis_api: Redis,
    stream: StrOrMapStr,
    stream_id: StrOrMapStr = "*",
    unstructure: MapDict = None,
    max_len: int = 500,
) -> Callable[[Observable[_T]], Observable[_T]]:

    if unstructure is None:

        def noop(x: _T):
            return x

        unstructure = noop

    def to_xstream_impl(source: Observable[_T]) -> Observable[_T]:
        def subscribe(
            observer: abc.ObserverBase[_T],
            scheduler: Optional[abc.SchedulerBase] = None,
        ) -> abc.DisposableBase:
            def on_next(x: _T) -> None:
                xstream = stream if isinstance(stream, str) else stream(x)
                xid = stream_id if isinstance(stream_id, str) else stream_id(x)
                try:
                    redis_api.xadd(
                        name=xstream, fields=unstructure(x), id=xid, maxlen=max_len
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