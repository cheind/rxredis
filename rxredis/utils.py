from datetime import datetime
from datetime import timezone


def parse_time(redis_time: str, tz=timezone.utc) -> datetime:
    """Returns datetime object corresponding to Redis time string.

    Works for stream ids and TIME command.

        >>> parse_time("1701423882967-0").isoformat()
        2023-12-01T09:44:42.967000+00:00
    """
    ms = float(redis_time.split("-")[0])
    return datetime.fromtimestamp(ms * 1e-3, tz)
