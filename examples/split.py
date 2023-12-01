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
        prod = utils.marble_stream_producer(
            redis_api, marbles="1-2-3-4-5-6-|"
        ).subscribe(
            on_next=lambda x: _logger.info(f"Produced {x}"),
            on_completed=lambda: _logger.info("Producer completed"),
            on_error=lambda _: _logger.exception("producer"),
        )

        # Read stream -> partition -> write each partition to separate stream

        streams = ["even", "odd"]

        rxr.from_stream(
            redis_api,
            stream="prod",
            stream_id="0",
            timeout=2000,
            complete_on_timeout=True,
        ).pipe(
            ops.group_by(lambda x: int(x[1]["marble"]) % 2),
            ops.flat_map(
                lambda grp: grp.pipe(
                    rxr.operators.to_stream(redis_api, streams[grp.key]),
                    ops.to_list(),
                )
            ),
        ).subscribe(
            on_next=lambda x: _logger.info(f"Consumed {x}"),
            on_error=lambda _: _logger.exception("consumer"),
        )

    except KeyboardInterrupt:
        prod.dispose()


# > "FLUSHALL"
# > "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "1"
# > "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "0"
# > "XADD" "odd" "MAXLEN" "~" "500" "*" "marble" "1"
# > "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405426087-0"
# > "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "2"
# > "XADD" "even" "MAXLEN" "~" "500" "*" "marble" "2"
# > "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405426490-0"
# > "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "3"
# > "XADD" "odd" "MAXLEN" "~" "500" "*" "marble" "3"
# > "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405426887-0"
# > "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "4"
# > "XADD" "even" "MAXLEN" "~" "500" "*" "marble" "4"
# > "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405427288-0"
# > "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "5"
# > "XADD" "odd" "MAXLEN" "~" "500" "*" "marble" "5"
# > "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405427688-0"
# > "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "6"
# > "XADD" "even" "MAXLEN" "~" "500" "*" "marble" "6"
# > "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405428088-0"

if __name__ == "__main__":
    main()
