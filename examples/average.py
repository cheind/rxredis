import logging
import random
from threading import Event

import attr
import cattr
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
        prod = utils.stream_producer(
            redis_api, stream="prod", marbles="1-2-2-3-4-5-6-|"
        ).subscribe()

        req = (
            rxr.from_stream(
                redis_api,
                stream="prod",
                stream_id="0",
                timeout=2000,
                complete_on_timeout=True,
            )
            .pipe(
                ops.map(lambda x: int(x[1]["marble"])),
                ops.distinct_until_changed(),
                ops.buffer_with_count(3, 1),
                ops.map(lambda x: sum(x) / len(x)),
                ops.map(lambda x: ("*", {"avg": x})),
                rxr.operators.to_stream(redis_api, "average", relay_streamid=True),
            )
            .subscribe(
                on_next=lambda x: _logger.info(f"Mean: {x}"),
                on_error=lambda _: _logger.exception("consumer"),
            )
        )
    except KeyboardInterrupt:
        req.dispose()


if __name__ == "__main__":
    main()
