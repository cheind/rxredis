"""Structured example.

Demonstrates the use of attr/cattr library to parse/serialize Redis data into
structured datatypes.

This demo requires both libraries to be installed
    pip install attrs cattrs

"""

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


@attr.define
class ProducerData:
    marble: str


@attr.define
class TransformedData(ProducerData):
    random: float


InData = tuple[str, ProducerData]
OutData = tuple[str, TransformedData]


def transform_data(x: InData) -> OutData:
    t = TransformedData(marble=x[1].marble, random=random.random())
    return (x[0], t)


def main():
    logging.basicConfig(level=logging.INFO)

    redis_api: Redis = redis.from_url("redis://localhost:6379/0?decode_responses=True")
    redis_api.flushall()

    try:
        # Async produce
        utils.marble_stream_producer(redis_api, stream="structured").subscribe()

        # Async read stream -> structure -> transform -> unstructure -> write stream
        rxr.from_stream(
            redis_api,
            stream="structured",
            stream_id=">",
            timeout=2000,
            complete_on_timeout=True,
        ).pipe(
            ops.map(lambda x: cattr.structure(x, InData)),
            ops.map(transform_data),
            ops.map(lambda x: (x[0], cattr.unstructure(x[1]))),
            rxr.operators.to_stream(redis_api, "transformed", relay_streamid=True),
        ).subscribe(
            on_next=lambda x: _logger.info(f"Transformed into {x}"),
            on_error=lambda _: _logger.exception("consumer"),
        )

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
