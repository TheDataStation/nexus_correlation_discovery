from dataclasses import dataclass
from enum import Enum
from utils.time_point import TEMPORAL_GRANU
from utils.coordinate import SPATIAL_GRANU
from typing import List
from typing import Union


class AggFunc(Enum):
    MIN = "min"
    MAX = "max"
    AVG = "avg"
    MEDIAN = "median"
    SUM = "sum"
    COUNT = "count"


class AttrType(Enum):
    TIME = "time"
    SPACE = "space"


class KeyType(Enum):
    TIME = "temporal"
    SPACE = "spatial"
    TIME_SPACE = "st"


class Variable:
    def __init__(self, tbl_id: str, attr_name: str, agg_func: AggFunc = None, var_name: str = None,
                 suffix=None) -> None:
        self.tbl_id = tbl_id
        self.attr_name = attr_name
        self.agg_func = agg_func
        self.var_name = var_name

        self.suffix = suffix
        if self.suffix:
            self.proj_name = "{}_{}".format(self.var_name, self.suffix)
        else:
            self.proj_name = self.var_name

    def to_str(self):
        return "{}-{}".format(self.tbl_id, self.attr_name)


# @dataclass
# class Attr:
#     name: str
#     granu: str

class Attr:
    def __init__(self, name: str, granularity: Union[TEMPORAL_GRANU, SPATIAL_GRANU]) -> None:
        self.name = name
        self.granu = granularity

    def to_int_name(self):
        return "{}_{}".format(self.name, self.granu.value)

    def to_readable_name(self):
        return "{}_{}".format(self.name, self.granu.name)

    def get_type(self):
        if self.granu in TEMPORAL_GRANU:
            return AttrType.TIME
        elif self.granu in SPATIAL_GRANU:
            return AttrType.SPACE

    def get_granu_value(self):
        return self.granu.value

    def get_val(self):
        if self.granu in TEMPORAL_GRANU:
            return "t_val"
        elif self.granu in SPATIAL_GRANU:
            return "s_val"


@dataclass
class Table:
    domain: str
    tbl_id: str
    tbl_name: str
    t_attrs: List[Attr]
    s_attrs: List[Attr]
    num_columns: List[str]
    link: str


class SpatioTemporalKey:
    def __init__(self, temporal_attr: Attr = None, spatial_attr: Attr = None):
        self.temporal_attr = temporal_attr
        self.spatial_attr = spatial_attr
        self.type = self.get_type()

    def get_type(self):
        if self.temporal_attr and self.spatial_attr:
            return KeyType.TIME_SPACE
        elif self.temporal_attr:
            return KeyType.TIME
        else:
            return KeyType.SPACE

    def get_granularity(self):
        if self.type == KeyType.TIME_SPACE:
            return (self.temporal_attr.granu,
                    self.spatial_attr.granu)
        elif self.type == KeyType.TIME:
            return self.temporal_attr.granu
        else:
            return self.spatial_attr.granu

    def get_id(self, tbl_id):
        if self.type == KeyType.TIME_SPACE:
            return ",".join([tbl_id, self.temporal_attr.name, self.spatial_attr.name])
        elif self.type == KeyType.TIME:
            return ",".join([tbl_id, self.temporal_attr.name])
        else:
            return ",".join([tbl_id, self.spatial_attr.name])

    def get_attrs(self):
        if self.type == KeyType.TIME_SPACE:
            return [self.temporal_attr.name, self.spatial_attr.name]
        elif self.type == KeyType.TIME:
            return [self.temporal_attr.name]
        else:
            return [self.spatial_attr.name]

    def get_idx_attr_names(self):
        if self.type == KeyType.TIME_SPACE:
            return ["t_attr", "s_attr"]
        elif self.type == KeyType.TIME:
            return ["t_attr"]
        else:
            return ["s_attr"]

    def get_col_names_with_granu(self):
        if self.type == KeyType.TIME_SPACE:
            return [self.temporal_attr.to_int_name(), self.spatial_attr.to_int_name()]
        elif self.type == KeyType.TIME:
            return [self.temporal_attr.to_int_name()]
        else:
            return [self.spatial_attr.to_int_name()]

    def get_idx_tbl_name(self):
        # determine which index table to ingest the agg_tbl values
        if self.type == KeyType.TIME_SPACE:
            return "time_{}_space_{}".format(
                self.temporal_attr.get_granu_value(), self.spatial_attr.get_granu_value()
            )

        elif self.type == KeyType.TIME:
            return "time_{}".format(self.temporal_attr.get_granu_value())
        else:
            return "space_{}".format(self.spatial_attr.get_granu_value())

    def get_idx_col_names(self):
        if self.type == KeyType.TIME_SPACE:
            return ["t_val", "s_val"]

        elif self.type == KeyType.TIME:
            return ["t_val"]
        else:
            return ["s_val"]

    def get_agg_tbl_name(self, tbl):
        return "{}_{}".format(
            tbl, "_".join([col for col in self.get_col_names_with_granu()])
        )


def new_st_schema_from_units(units: List[Attr]):
    if len(units) == 2:
        t_unit, s_unit = units[0], units[1]
        st_schema = SpatioTemporalKey(
            temporal_attr=Attr(t_unit, t_unit.granu),
            spatial_attr=Attr(s_unit, s_unit.granu),
        )
    elif len(units) == 1:
        unit = units[0]
        if unit.get_type() == AttrType.TIME:
            st_schema = SpatioTemporalKey(
                temporal_attr=Attr(unit, unit.granu),
            )
        else:
            st_schema = SpatioTemporalKey(
                spatial_attr=Attr(unit, unit.granu),
            )
    return st_schema





