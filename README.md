**rxredis**: *ReactiveX extensions for Redis in Python*

## Example

The following example shows how to split elements of an input stream by parity and write the elements of the resulting groups asynchronously to different output streams.

```python
import rxredis as rxr

import redis
import reactivex.operators as ops

# Open Redis
redis_api: redis.Redis = redis.from_url("redis://localhost:6379/0?decode_responses=True")

streams = ["even", "odd"]

# Read marble stream (1,2,...) until timeout
rxr.from_stream(
    redis_api,
    stream="prod",
    stream_id="0",  # Start at beginning of stream
    timeout=2000,
    complete_on_timeout=True,  # Complete observable upon timeout
).pipe(
    # Group by parity
    ops.group_by(lambda x: int(x[1]["marble"]) % 2),
    # Map each group
    ops.flat_map(
        lambda grp: grp.pipe(
            # Push to destination stream
            rxr.operators.to_stream(redis_api, streams[grp.key]),
            # Return last element on completion of group
            ops.last(),
        )
    ),
).subscribe(
    on_next=lambda x: print(f"Last consumed {x}"),
    on_error=lambda e: print(e),
    on_completed=lambda: print('Done')
)
```

Here is the output of the `monitor` command at Redis. We use a live producer, prefixed with `P>`, and prefix consumer part with `C>`.
```
P> "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "1"
C> "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "0"
C> "XADD" "odd" "MAXLEN" "~" "500" "*" "marble" "1"
C> "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405426087-0"
P> "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "2"
C> "XADD" "even" "MAXLEN" "~" "500" "*" "marble" "2"
C> "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405426490-0"
P> "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "3"
C> "XADD" "odd" "MAXLEN" "~" "500" "*" "marble" "3"
C> "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405426887-0"
P> "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "4"
C> "XADD" "even" "MAXLEN" "~" "500" "*" "marble" "4"
C> "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405427288-0"
P> "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "5"
C> "XADD" "odd" "MAXLEN" "~" "500" "*" "marble" "5"
C> "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405427688-0"
P> "XADD" "prod" "MAXLEN" "~" "500" "*" "marble" "6"
C> "XADD" "even" "MAXLEN" "~" "500" "*" "marble" "6"
C> "XREAD" "BLOCK" "2000" "COUNT" "1" "STREAMS" "prod" "1701405428088-0"
```
