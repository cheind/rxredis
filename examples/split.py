"""Split example.

Demonstrates splitting input stream into two output streams based on condition.
"""
import logging

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
        # Async write some stream data
        prod = utils.stream_producer(redis_api)

        # Async read stream -> partition -> write each partition to separate stream
        req = rxr.from_stream(
            redis_api,
            stream="prod",
            stream_id="0",
            timeout=2000,
            complete_on_timeout=True,
        ).pipe(
            ops.publish()
        )  # hot

        even, odd = req.pipe(
            ops.partition(lambda x: int(x[1]["marble"]) % 2 == 0),
        )

        evensub = even.pipe(
            rxr.operators.to_stream(redis_api, "even"),
        ).subscribe()  # Assigned new stream-ids

        oddsub = odd.pipe(
            rxr.operators.to_stream(redis_api, "odd", relay_streamid=True),
        ).subscribe()  # Copies stream-id to new stream

        req.subscribe(
            on_next=lambda x: _logger.info(f"Consumed {x}"),
            on_error=lambda _: _logger.exception("consumer"),
        )
        prod.subscribe(
            on_next=lambda x: _logger.info(f"Produced {x}"),
            on_completed=lambda: _logger.info("Producer completed"),
            on_error=lambda _: _logger.exception("producer"),
        )
        req.connect()
    except KeyboardInterrupt:
        evensub.dispose()
        oddsub.dispose()
        prod.dispose()


if __name__ == "__main__":
    main()
