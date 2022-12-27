from pandas import Timestamp
from enum import IntEnum

class T_GRANU(IntEnum):
    MINUTE = 1
    HOUR = 2
    DAY = 3
    MONTH = 4
    QUARTER = 5
    YEAR = 6

class Datetime:
    def __init__(self, dt: Timestamp) -> None:
        self.dt = dt
        self.minute = dt.min
        self.hour = dt.hour
        self.day = dt.day
        self.month = dt.month
        self.quarter = dt.quarter
        self.year = dt.year
        self.full_resolution = [self.minute, self.hour, self.day, self.month, self.quarter, self.year]
    
    def transform(self, granu: T_GRANU):
        return self.full_resolution[granu-1:]
    


def parse_datetime(dt: Timestamp):
    return Datetime(dt)

def transform(dt: Datetime, granu: T_GRANU):
    return dt.full_resolution[granu-1:]

def dt_to_str(dt):
    resolution = dt[::-1] 
    res = ""
    for i, token in enumerate(resolution):
        res += str(token) 
        if i+1 != len(resolution):
            res += str('-')
    return res