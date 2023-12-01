"""Combine example.

Demonstrates merging multiple input streams and writing a combined
result in a lower frequency.
"""

import logging
import math
import multiprocessing as mp
import time
import traceback

import reactivex as rx
import reactivex.operators as ops
import redis
from reactivex.scheduler import ThreadPoolScheduler
from redis import Redis

import rxredis as rxr

URL = "redis://localhost:6379/0?decode_responses=True"


# Producer task
def sensor(stream: str, stop: mp.Event, hz: float):
    print(f"sensor:{stream} start")

    with redis.from_url(URL) as api:
        rx.interval(1 / hz).pipe(
            ops.take_while(lambda _: not stop.is_set()),
            ops.map(lambda t: ("*", {"v": math.sin(t)})),
            rxr.operators.to_stream(api, stream),
        ).subscribe(
            on_completed=lambda: print(f"sensor:{stream} done"),
            on_error=lambda _: traceback.print_exc(),
        )
        stop.wait()


# Combiner task
def combine(streams: list[str], stop: mp.Event, hz: float):
    print(f"combine: start")
    with redis.from_url(URL) as api:
        sched = ThreadPoolScheduler(max_workers=mp.cpu_count() // 2)

        def combine_from_tuple(t):
            # Map value from each stream to dict
            data = {s: e[1]["v"] for s, e in zip(streams, t)}
            # Add abs time diff between redis times of all stream elements
            data["td"] = abs(
                (
                    rxr.utils.parse_time(t[0][0]) - rxr.utils.parse_time(t[-1][0])
                ).total_seconds()
            )
            return ("*", data)

        obs = [
            rxr.from_stream(api, stream=s, stream_id=">", batch=10, latest=True)
            for s in streams
        ]
        sub = (
            rx.combine_latest(*obs)  # combine streams whenever one stream fires
            .pipe(
                ops.take_while(lambda _: not stop.is_set()),  # check stop condition
                ops.buffer_with_time(1.0 / hz),  # buffer elems into time windows
                ops.filter(lambda b: len(b) > 0),  # skip empty buffers
                ops.map(lambda b: b[-1]),  # use latest from buffer
                ops.map(combine_from_tuple),  # transform to redis stream
                rxr.operators.to_stream(api, "combined"),  # write
            )
            .subscribe(
                on_next=lambda v: print(v),
                on_completed=lambda: print(f"combine: done"),
                on_error=lambda _: traceback.print_exc(),
                scheduler=sched,
            )
        )
        stop.wait()
        sub.dispose()


def main():
    logging.basicConfig(level=logging.INFO)

    redis_api: Redis = redis.from_url("redis://localhost:6379/0?decode_responses=True")
    redis_api.flushall()
    stop = mp.Event()

    # Generate with @ 10Hz
    s1 = mp.Process(target=sensor, args=("s1", stop, 10.0))

    # Generates @ 20Hz
    s2 = mp.Process(target=sensor, args=("s2", stop, 20.0))

    # Combine and emit at 0.5Hz (once every 2 secs)
    c = mp.Process(target=combine, args=(["s1", "s2"], stop, 0.5))

    s1.start()
    s2.start()
    c.start()

    time.sleep(20)

    # Shutdown
    stop.set()
    s1.join()
    s1.close()
    s2.join()
    s2.close()
    c.join()
    c.close()


if __name__ == "__main__":
    main()
