from enum import Enum


class AggFunc(Enum):
    MIN = "min"
    MAX = "max"
    AVG = "avg"
    MEDIAN = "median"
    SUM = "sum"
    COUNT = "count"


class Variable:
    def __init__(self, attr_name: str, agg_func: AggFunc, var_name: str) -> None:
        self.attr_name = attr_name
        self.agg_func = agg_func
        self.var_name = var_name


class Unit:
    def __init__(self, attr_name: str, granu) -> None:
        self.attr_name = attr_name
        self.granu = granu
