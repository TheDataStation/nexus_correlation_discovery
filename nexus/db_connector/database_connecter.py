from enum import Enum

import pandas as pd
from nexus.utils.data_model import Table, SpatioTemporalKey, Variable
from typing import List, Dict


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

    def create_cnt_tbl_for_agg_tbl(self, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        pass

    def create_indices_on_tbl(self, idx_name: str, tbl_id: str, col_names: List[str], mode=IndexType.B_TREE):
        pass

    def create_inv_index_tbl(self, inv_index_tbl: str):
        pass

    def create_cnt_tbl_for_an_inverted_index(self, idx_name: str):
        pass

    def insert_spatio_temporal_key_to_inv_idx(self, inv_idx: str, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        pass

    def get_variable_stats(self, agg_tbl_name: str, var_name: str):
        pass

    def join_two_tables_on_spatio_temporal_keys(self, agg_tbl1: str, variables1: List[Variable],
                                                agg_tbl2: str, variables2: List[Variable],
                                                use_outer: bool = False):
        pass

    def join_multi_agg_tbls(self, tbl_cols: Dict[str, List[Variable]]):
        pass

    def read_agg_tbl(self, agg_tbl: str, variables: List[Variable] = []):
        pass

    def get_total_row_to_read_and_max_joinable_tables(self, tbl_id, spatio_temporal_key: SpatioTemporalKey,
                                                      threshold: int):
        pass

    def estimate_joinable_candidates(
            self, tbl_id, spatio_temporal_key: SpatioTemporalKey, threshold: int, rows_to_sample: int = 0
    ):
        pass