from threading import Event

import reactivex as rx
import redis
import reactivex.operators as ops
from redis import Redis

import rxredis as rxr
import attr
import cattr


def produce(redis_api: Redis):
    return (
        rx.interval(0.5)
        .pipe(
            ops.map(lambda i: {"i": i}),
            rxr.operations.to_stream(redis_api, "prod"),
        )
        .subscribe(
            on_next=lambda x: print(f"produced {x}"),
            on_completed=lambda: print("completed"),
            on_error=lambda e: print(e),
        )
    )


def main():


    redis_api: Redis = redis.from_url("redis://localhost:6379/0?decode_responses=True")
    done = Event()

    @attr.define
    class ProducerData:
        i: int

    StreamData = tuple[str, ProducerData]

    try:
        prod = produce(redis_api)

        req = rxr.from_stream(redis_api, stream="prod", stream_id=">").pipe(
            ops.take(10), ops.publish()
        )

        even, odd = req.pipe(
            ops.map(lambda x: cattr.structure(x, StreamData)),
            ops.partition(lambda x: x[1].i % 2 == 0),
        )

        evensub = even.pipe(
            ops.map(lambda x: cattr.unstructure(x[1])),
            rxr.operations.to_stream(redis_api, "even"),
        ).subscribe()

        oddsub = odd.pipe(
            ops.map(lambda x: cattr.unstructure(x[1])),
            rxr.operations.to_stream(redis_api, "odd"),
        ).subscribe()

        req.subscribe(
            on_next=lambda x: print(f"Consumed {x}"),
            on_completed=lambda: done.set(),
        )
        req.connect()
        done.wait()
    except KeyboardInterrupt:
        evensub.dispose()
        oddsub.dispose()
        prod.dispose()


if __name__ == "__main__":
    main()
