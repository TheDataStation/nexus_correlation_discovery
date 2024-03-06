from enum import Enum

import pandas as pd
from utils.data_model import Table, SpatioTemporalKey, Variable
from typing import List


class IndexType(Enum):
    B_TREE = "B_TREE"
    HASH = "HASH"


class DatabaseConnectorInterface:
    def create_tbl(self, tbl_id: str, df: pd.DataFrame, mode='replace'):
        pass

    def delete_tbl(self, tbl_id: str):
        pass

    def create_aggregate_tbl(self, tbl_id: str, spatio_temporal_key: SpatioTemporalKey, variables: List[Variable]):
        pass

    def create_indices_on_tbl(self, idx_name: str, tbl_id: str, col_names: List[str], mode=IndexType.B_TREE):
        pass

    def create_inv_index_tbl(self, inv_index_tbl: str):
        pass

    def insert_spatio_temporal_key_to_inv_idx(self, inv_idx: str, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        pass

    def get_variable_stats(self, agg_tbl_name: str, var_name: str):
        pass

    def join_two_tables_on_spatio_temporal_keys(self, tbl_id1: str, agg_tbl1: str, variables1: List[Variable],
                                                tbl_id2: str, agg_tbl2: str, variables2: List[Variable],
                                                use_outer: bool = False):
        pass
