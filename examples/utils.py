from redis import Redis

import reactivex as rx
import reactivex.operators as ops

import rxredis as rxr


def stream_producer(
    redis_api: Redis, stream: str = "prod", marbles: str = "-1-2-3-4-5-6-|"
):
    return rx.from_marbles(marbles, timespan=0.2).pipe(
        ops.map(lambda x: ("*", {"marble": x})),
        rxr.operators.to_stream(redis_api, stream),
    )
