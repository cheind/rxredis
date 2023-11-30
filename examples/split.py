import logging

import reactivex as rx
import reactivex.operators as ops
import redis
from redis import Redis

import rxredis as rxr

from . import utils

_logger = logging.getLogger("rxredis")


def main():
    logging.basicConfig(level=logging.INFO)

    redis_api: Redis = redis.from_url("redis://localhost:6379/0?decode_responses=True")
    redis_api.flushall()

    try:
        prod = utils.stream_producer(redis_api)

        req = rxr.from_stream(
            redis_api,
            stream="prod",
            stream_id=">",
            timeout=2000,
            complete_on_timeout=True,
        ).pipe(ops.publish())

        even, odd = req.pipe(
            # ops.map(lambda x: cattr.structure(x, StreamData)),
            ops.partition(lambda x: int(x[1]["marble"]) % 2 == 0),
        )

        evensub = even.pipe(
            # ops.map(lambda x: cattr.unstructure(x[1])),
            rxr.operators.to_stream(redis_api, "even"),
        ).subscribe()

        oddsub = odd.pipe(
            # ops.map(lambda x: cattr.unstructure(x[1])),
            rxr.operators.to_stream(redis_api, "odd", relay_streamid=True),
        ).subscribe()

        req.subscribe(
            on_next=lambda x: _logger.info(f"Consumed {x}"),
            on_error=lambda _: _logger.exception("consumer"),
        )
        prod.subscribe(
            on_next=lambda x: _logger.info(f"produced {x}"),
            on_completed=lambda: _logger.info("producer completed"),
            on_error=lambda _: _logger.exception("producer"),
        )
        req.connect()
    except KeyboardInterrupt:
        evensub.dispose()
        oddsub.dispose()
        prod.dispose()


if __name__ == "__main__":
    main()
