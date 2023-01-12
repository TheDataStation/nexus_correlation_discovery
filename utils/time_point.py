from pandas import Timestamp
from enum import Enum


class T_GRANU(Enum):
    HOUR = 1
    DAY = 2
    MONTH = 3
    QUARTER = 4
    YEAR = 5


class Datetime:
    def __init__(self, dt: Timestamp) -> None:
        self.dt = dt
        self.hour = dt.hour
        self.day = dt.day
        self.month = dt.month
        self.quarter = dt.quarter
        self.year = dt.year
        self.full_resolution = [
            self.hour,
            self.day,
            self.month,
            self.quarter,
            self.year,
        ]

    def transform(self, granu: T_GRANU):
        return list(reversed(self.full_resolution[granu - 1 :]))

    def to_str(self, repr):
        return "-".join([str(x) for x in repr])

    def transform_to_key(self, granu: T_GRANU):
        repr = self.full_resolution[granu - 1 :]
        return str(repr)


def parse_datetime(dt: Timestamp):
    return Datetime(dt)


def transform(dt: Datetime, granu: T_GRANU):
    return dt.full_resolution[granu - 1 :]


def set_temporal_granu(dt: Datetime, granu: T_GRANU):
    return dt.to_str(dt.transform(granu))


def dt_to_str(dt):
    resolution = dt[::-1]
    res = ""
    for i, token in enumerate(resolution):
        res += str(token)
        if i + 1 != len(resolution):
            res += str("-")
    return res
