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

    rxr.on_keyspace(redis_api, ["x", "y"]).pipe(
        ops.filter(lambda ev: ev[1]["event"] in ["set", "expired", "del"]),
        ops.do_action(lambda ev: print(redis_api.get(ev[1]["key"]))),
    ).subscribe(on_next=lambda e: print(e), on_error=lambda e: print(e))
    input("test")
    # In parallel on a redis-cli hit: set x 3


if __name__ == "__main__":
    main()
