import logging

import reactivex.operators as ops
import redis
from redis import Redis

import rxredis as rxr

from . import utils

_logger = logging.getLogger("rxredis")


def on_keys(redis_api: Redis, keys: list[str]):
    def extract_key(keyspace_event: str):
        return keyspace_event.split(":")[-1]

    patterns = [f"__keyspace@*__:{k}" for k in keys]

    return rxr.from_pubsub(redis_api, patterns).pipe(
        ops.map(
            lambda t: (
                t[0],
                {
                    "key": extract_key(t[1]["channel"]),
                    "event": extract_key(t[1]["message"]),
                },
            )
        )
    )


def main():
    logging.basicConfig(level=logging.INFO)

    redis_api: Redis = redis.from_url("redis://localhost:6379/0?decode_responses=True")
    redis_api.flushall()

    try:
        sub = (
            on_keys(redis_api, ["x", "y"])
            .pipe(
                ops.filter(lambda ev: ev[1]["event"] in ["set", "expired", "del"]),
                ops.do_action(lambda ev: print(redis_api.get(ev[1]["key"]))),
            )
            .subscribe(on_next=lambda e: print(e), on_error=lambda e: print(e))
        )
        input("test")

    except KeyboardInterrupt:
        sub.dispose()


if __name__ == "__main__":
    main()
