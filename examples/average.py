"""Average example.

Demonstrates computing a rolling average over distinct stream input values
and emitting those averages to an output stream.
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
        # Async production to xstream: prod
        prod = utils.marble_stream_producer(
            redis_api, stream="prod", marbles="1-2-2-3-4-5-6-|"
        ).subscribe()

        # Async read stream -> aggregate distinct -> compute mean -> write stream
        rxr.from_stream(
            redis_api,
            stream="prod",
            stream_id="0",  # Start id, or '$' or '>'
            timeout=2.0,
            complete_on_timeout=True,  # Emit completed event when no more data
        ).pipe(
            ops.map(lambda x: int(x[1]["marble"])),  # Extract marble data
            ops.distinct_until_changed(),  # Wait until different from last
            ops.buffer_with_count(3, 1),  # Make batches with step 1
            ops.map(lambda x: sum(x) / len(x)),  # Compute average of batch
            ops.map(lambda x: ("*", {"avg": x})),  # Map to output stream format
            rxr.operators.to_stream(
                redis_api, "average", relay_streamid=True
            ),  # Output
        ).subscribe(
            on_next=lambda x: _logger.info(f"Mean: {x}"),
            on_error=lambda _: _logger.exception("consumer"),
        )

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
