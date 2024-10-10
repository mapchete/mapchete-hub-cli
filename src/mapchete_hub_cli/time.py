from datetime import datetime, timedelta, timezone
from dateutil import parser
from typing import Union


def str_to_date(date_str: str) -> datetime:
    """Convert string to datetime object."""
    return parser.parse(date_str)


def date_to_str(date_obj: Union[str, datetime], microseconds=True) -> str:
    """Return string from datetime object in the format."""
    if isinstance(date_obj, str):
        date_obj = str_to_date(date_obj)
    return date_obj.strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ" if microseconds else "%Y-%m-%dT%H:%M:%SZ"
    )


def passed_time_to_timestamp(passed_time: str) -> datetime:
    # for a time range like '1d', '12h', '30m'
    time_types = {
        "w": "weeks",
        "d": "days",
        "h": "hours",
        "m": "minutes",
        "s": "seconds",
    }
    for k, v in time_types.items():
        if passed_time.endswith(k):
            return datetime.now(timezone.utc) - timedelta(**{v: int(passed_time[:-1])})
    else:
        raise ValueError(f"cannot not convert {passed_time} to timestamp")


def pretty_time(elapsed_seconds: float) -> str:
    minutes, seconds = divmod(elapsed_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    if days:  # pragma: no cover
        return f"{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s"
    elif hours:  # pragma: no cover
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    elif minutes:  # pragma: no cover
        return f"{int(minutes)}m {int(seconds)}s"
    else:
        return f"{round(seconds, 3)}s"


def pretty_time_passed(timestamp: datetime) -> str:
    return pretty_time((datetime.now(timezone.utc) - timestamp).total_seconds())
