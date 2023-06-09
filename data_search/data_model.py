from enum import Enum
from utils.time_point import T_GRANU
from utils.coordinate import S_GRANU
from typing import List


class AggFunc(Enum):
    MIN = "min"
    MAX = "max"
    AVG = "avg"
    MEDIAN = "median"
    SUM = "sum"
    COUNT = "count"


class UnitType(Enum):
    TIME = "time"
    SPACE = "space"


class SchemaType(Enum):
    TIME = "temporal"
    SPACE = "spatial"
    TS = "st"


class Variable:
    def __init__(self, attr_name: str, agg_func: AggFunc, var_name: str) -> None:
        self.attr_name = attr_name
        self.agg_func = agg_func
        self.var_name = var_name


class Unit:
    def __init__(self, attr_name: str, granu) -> None:
        self.attr_name = attr_name
        self.granu = granu

    def to_int_name(self):
        return "{}_{}".format(self.attr_name, self.granu.value)

    def to_readable_name(self):
        return "{}_{}".format(self.attr_name, self.granu.name)

    def get_type(self):
        if self.granu in T_GRANU:
            return UnitType.TIME
        elif self.granu in S_GRANU:
            return UnitType.SPACE

    def get_granu_value(self):
        return self.granu.value

    def get_val(self):
        if self.granu in T_GRANU:
            return "t_val"
        elif self.granu in S_GRANU:
            return "s_val"


class ST_Schema:
    def __init__(self, t_unit: Unit = None, s_unit: Unit = None):
        self.t_unit = t_unit
        self.s_unit = s_unit
        self.type = self.get_type()

    def get_type(self):
        if self.t_unit and self.s_unit:
            return SchemaType.TS
        elif self.t_unit:
            return SchemaType.TIME
        else:
            return SchemaType.SPACE

    def get_scales(self):
        if self.type == SchemaType.TS:
            return (self.t_unit.granu, self.s_unit.granu)
        elif self.type == SchemaType.TIME:
            return self.t_unit.granu
        else:
            return self.s_unit.granu

    def get_id(self, tbl_id):
        if self.type == SchemaType.TS:
            return ",".join([tbl_id, self.t_unit.attr_name, self.s_unit.attr_name])
        elif self.type == SchemaType.TIME:
            return ",".join([tbl_id, self.t_unit.attr_name])
        else:
            return ",".join([tbl_id, self.s_unit.attr_name])

    def get_attrs(self):
        if self.type == SchemaType.TS:
            return [self.t_unit.attr_name, self.s_unit.attr_name]
        elif self.type == SchemaType.TIME:
            return [self.t_unit.attr_name]
        else:
            return [self.s_unit.attr_name]

    def get_idx_attr_names(self):
        if self.type == SchemaType.TS:
            return ["t_attr", "s_attr"]
        elif self.type == SchemaType.TIME:
            return ["t_attr"]
        else:
            return ["s_attr"]

    def get_col_names_with_granu(self):
        if self.type == SchemaType.TS:
            return [self.t_unit.to_int_name(), self.s_unit.to_int_name()]
        elif self.type == SchemaType.TIME:
            return [self.t_unit.to_int_name()]
        else:
            return [self.s_unit.to_int_name()]

    def get_idx_tbl_name(self):
        # determine which index table to ingest the agg_tbl values
        if self.type == SchemaType.TS:
            return "time_{}_space_{}".format(
                self.t_unit.get_granu_value(), self.s_unit.get_granu_value()
            )

        elif self.type == SchemaType.TIME:
            return "time_{}".format(self.t_unit.get_granu_value())
        else:
            return "space_{}".format(self.s_unit.get_granu_value())

    def get_idx_col_names(self):
        if self.type == SchemaType.TS:
            return ["t_val", "s_val"]

        elif self.type == SchemaType.TIME:
            return ["t_val"]
        else:
            return ["s_val"]

    def get_agg_tbl_name(self, tbl):
        return "{}_{}".format(
            tbl, "_".join([col for col in self.get_col_names_with_granu()])
        )


def get_st_schema_list_for_tbl(
    t_attrs: List[str],
    s_attrs: List[str],
    t_unit: Unit,
    s_unit: Unit,
    st_types: List[SchemaType],
):
    st_schema_list = []
    if SchemaType.TIME in st_types:
        t_scale = t_unit.granu
        for t in t_attrs:
            st_schema_list.append(ST_Schema(t_unit=Unit(t, t_scale)))

    if SchemaType.SPACE in st_types:
        s_scale = s_unit.granu
        for s in s_attrs:
            st_schema_list.append(ST_Schema(s_unit=Unit(s, s_scale)))

    if SchemaType.TS in st_types:
        t_scale = t_unit.granu
        s_scale = s_unit.granu
        for t in t_attrs:
            for s in s_attrs:
                st_schema_list.append(ST_Schema(Unit(t, t_scale), Unit(s, s_scale)))
    return st_schema_list


# def get_st_schema_list_for_tbl(t_attrs, s_attrs, st_schema: ST_Schema):
#     schema_list = []
#     type = st_schema.get_type()
#     if type == SchemaType.TIME:
#         for t_attr in t_attrs:
#             schema_list.append(ST_Schema(t_unit=Unit(t_attr, st_schema.t_unit.granu)))
#     elif type == SchemaType.SPACE:
#         for s_attr in s_attrs:
#             schema_list.append(ST_Schema(s_unit=Unit(s_attr, st_schema.s_unit.granu)))
#     else:
#         for t_attr in t_attrs:
#             for s_attr in s_attrs:
#                 schema_list.append(
#                     ST_Schema(
#                         t_unit=Unit(t_attr, st_schema.t_unit.granu),
#                         s_unit=Unit(s_attr, st_schema.s_unit.granu),
#                     )
#                 )
#     return schema_list


def new_st_schema_from_units(units: List[Unit]):
    if len(units) == 2:
        t_unit, s_unit = units[0], units[1]
        st_schema = ST_Schema(
            t_unit=Unit(t_unit, t_unit.granu),
            s_unit=Unit(s_unit, s_unit.granu),
        )
    elif len(units) == 1:
        unit = units[0]
        if unit.get_type() == UnitType.TIME:
            st_schema = ST_Schema(
                t_unit=Unit(unit, unit.granu),
            )
        else:
            st_schema = ST_Schema(
                s_unit=Unit(unit, unit.granu),
            )
    return st_schema
