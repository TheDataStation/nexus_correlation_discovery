from enum import Enum
from nexus.utils.time_point import TEMPORAL_GRANU
from nexus.utils.spatial_hierarchy import SPATIAL_GRANU
from nexus.utils.profile_utils import is_num_column_valid
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
    def __init__(self, tbl_id: str = None, attr_name: str = None, agg_func: AggFunc = None, var_name: str = None,
                 suffix=None) -> None:
        self.tbl_id = tbl_id
        self.attr_name = attr_name
        self.agg_func = agg_func
        self.var_name = var_name
        self.max_len_limit = 63

        self.suffix = suffix
        if self.suffix and self.var_name:
            self.proj_name = "{}_{}".format(self.var_name, self.suffix)[:self.max_len_limit]
        elif self.suffix is None and self.var_name:
            self.proj_name = self.var_name[:self.max_len_limit]

    def to_str(self):
        return "{}-{}".format(self.tbl_id, self.attr_name)


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


class SpatioTemporalKey:
    def __init__(self, temporal_attr: Attr = None, spatial_attr: Attr = None):
        self.temporal_attr = temporal_attr
        self.spatial_attr = spatial_attr
        self.type = self.get_type()

    def from_attr_names(self, attr_names: List[str]):
        if self.type == KeyType.TIME_SPACE:
            return SpatioTemporalKey(
                temporal_attr=Attr(attr_names[0], self.temporal_attr.granu),
                spatial_attr=Attr(attr_names[1], self.spatial_attr.granu),
            )
        elif self.type == KeyType.TIME:
            return SpatioTemporalKey(
                temporal_attr=Attr(attr_names[0], self.temporal_attr.granu),
            )
        elif self.type == KeyType.SPACE:
            return SpatioTemporalKey(
                spatial_attr=Attr(attr_names[0], self.spatial_attr.granu),
            )

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
    
    def get_agg_tbl_description(self, tbl):
        return "Aggregated table for {} using attribute {} with granularity {}".format(
            tbl, self.get_attrs(), self.get_granularity().name
        )


class Table:
    def __init__(self, domain: str = '', tbl_id: str = '', tbl_name: str = '',
                 temporal_attrs: List[Attr] = [], spatial_attrs: List[Attr] = [],
                 num_columns: List[str] = [], link: str = ''):
        self.domain = domain
        self.tbl_id = tbl_id
        self.tbl_name = tbl_name
        self.temporal_attrs = temporal_attrs
        self.spatial_attrs = spatial_attrs
        self.num_columns = num_columns
        self.link = link
    
    def to_json(self):
        return {
            "domain": self.domain,
            "tbl_id": self.tbl_id,
            "tbl_name": self.tbl_name,
            "t_attrs": [t_attr.__dict__ for t_attr in self.temporal_attrs],
            "s_attrs": [s_attr.__dict__ for s_attr in self.spatial_attrs],
            "num_columns": self.num_columns,
            "link": self.link
        }

    @staticmethod
    def table_from_tbl_id(tbl_id: str, data_catalog):
        temporal_attrs = [Attr(attr["name"], attr["granu"]) for attr in data_catalog[tbl_id]['t_attrs']]
        spatial_attrs = [Attr(attr["name"], attr["granu"]) for attr in data_catalog[tbl_id]['s_attrs']]
        num_attrs = data_catalog[tbl_id]["num_columns"]
        return Table(tbl_id=tbl_id,
                     temporal_attrs=temporal_attrs,
                     spatial_attrs=spatial_attrs,
                     num_columns=num_attrs)

    def get_spatio_temporal_keys(self, temporal_granu_l: List[TEMPORAL_GRANU], spatial_granu_l: List[SPATIAL_GRANU],
                                 mode="no_cross") -> List[SpatioTemporalKey]:
        spatio_temporal_keys = []
        for temporal_attr in self.temporal_attrs:
            for cur_granularity in temporal_granu_l:
                spatio_temporal_keys.append(SpatioTemporalKey(temporal_attr=Attr(temporal_attr.name, cur_granularity)))

        for spatial_attr in self.spatial_attrs:
            for cur_granularity in spatial_granu_l:
                if spatial_attr.granu != 'POINT' and spatial_attr.granu != cur_granularity.name:
                    continue
                spatio_temporal_keys.append(SpatioTemporalKey(spatial_attr=Attr(spatial_attr.name, cur_granularity)))

        for temporal_attr in self.temporal_attrs:
            for spatial_attr in self.spatial_attrs:
                if mode == 'no_cross':
                    for i in range(len(temporal_granu_l)):
                        if spatial_attr.granu != 'POINT' and spatial_attr.granu != spatial_granu_l[i].name:
                            continue
                        spatio_temporal_keys.append(
                            SpatioTemporalKey(Attr(temporal_attr.name, temporal_granu_l[i]),
                                              Attr(spatial_attr.name, spatial_granu_l[i]))
                        )
                elif mode == 'cross':
                    for cur_temporal_granu in temporal_granu_l:
                        for cur_spatial_granu in spatial_granu_l:
                            if spatial_attr.granu != 'POINT' and spatial_attr.granu != cur_spatial_granu.name:
                                continue
                            spatio_temporal_keys.append(
                                SpatioTemporalKey(Attr(temporal_attr.name, cur_temporal_granu),
                                                  Attr(spatial_attr.name, cur_spatial_granu))
                            )
        return spatio_temporal_keys

    def get_variables(self, suffix: str = None) -> List[Variable]:
        variables = []
        for agg_col in self.num_columns:
            if not is_num_column_valid(agg_col) or len(agg_col) > 56:
                continue
            variables.append(Variable(self.tbl_id, agg_col, AggFunc.AVG, "avg_{}".format(agg_col), suffix=suffix))
        variables.append(Variable(self.tbl_id, "*", AggFunc.COUNT, "count", suffix=suffix))
        return variables
