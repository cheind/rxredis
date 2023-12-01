from typing import Union
from datetime import datetime
from datetime import timezone


def parse_time(redis_time: Union[str, tuple[int, int]], tz=timezone.utc) -> datetime:
    """Returns datetime object corresponding to Redis time string.

    Works for stream IDs and TIME response.

        >>> parse_time("1701423882967-0").isoformat()
        2023-12-01T09:44:42.967000+00:00
        >>> parse_time((1701442022, 631883)).isoformat()
        2023-12-01T09:44:42.967000+00:00
    """
    if isinstance(redis_time, str):
        sec = float(redis_time.split("-")[0]) * 1e-3
    else:
        sec = redis_time[0] + redis_time[1] * 1e-6
        print(sec)
    return datetime.fromtimestamp(sec, tz)


if __name__ == "__main__":
    print(parse_time("1701442779390"))
