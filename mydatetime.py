from pandas import Timestamp
from enum import IntEnum

class T_GRANU(IntEnum):
    MINUTE = 0
    HOUR = 1
    DAY = 2
    MONTH = 3
    QUARTER = 4
    YEAR = 5

class Datetime:
    def __init__(self, dt: Timestamp) -> None:
        self.minute = dt.min
        self.hour = dt.hour
        self.day = dt.day
        self.month = dt.month
        self.quarter = dt.quarter
        self.year = dt.year
        self.full_resolution = [self.minute, self.hour, self.day, self.month, self.quarter, self.year]
    
    def transform(self, granu: T_GRANU):
        return self.full_resolution[granu:]