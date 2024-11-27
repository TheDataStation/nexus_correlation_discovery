from nexus.utils import io_utils
from nexus.utils.data_model import SpatioTemporalKey, Attr, KeyType, TEMPORAL_GRANU, Table
from nexus.utils.spatial_hierarchy import SPATIAL_GRANU
import pandas as pd
from tqdm import tqdm
import numpy as np
from collections import namedtuple
from nexus.db_connector.database_connecter import DatabaseConnectorInterface
from typing import List
"""
Collect the following stats for each aggregated table

agg_tbl_name -> [col_name->[stat_name->val]]
"""


class Profiler:
    def __init__(self, db_engine: DatabaseConnectorInterface, data_source: str, mode='no_cross') -> None:
        self.db_engine = db_engine
        self.data_source = data_source
        self.mode = mode
        self.stats_dict = {}
        self.config = io_utils.load_config(data_source)
        self.data_catalog = io_utils.load_json(self.config["attr_path"])

    def set_mode(self, mode):
        self.mode = mode

    def collect_agg_tbl_col_stats(self, temporal_granu_l: List[TEMPORAL_GRANU], spatial_granu_l: List[SPATIAL_GRANU]):
        for tbl_id in tqdm(self.data_catalog.keys()):
            table = Table.table_from_tbl_id(tbl_id, self.data_catalog)
            self.profile_tbl(table, temporal_granu_l, spatial_granu_l, self.mode)
        io_utils.dump_json(self.config["col_stats_path"], self.stats_dict)

    @staticmethod
    def load_all_spatio_temporal_keys(tbl_attrs, temporal_granu, spatial_granu, type_aware=False):
        spatio_temporal_keys = []

        all_tbls = list(tbl_attrs.keys())
        if type_aware:
            spatio_temporal_keys_by_type = {KeyType.TIME: [], KeyType.SPACE: [], KeyType.TIME_SPACE: []}
        for tbl in all_tbls:
            t_attrs, s_attrs = (
                tbl_attrs[tbl]["t_attrs"],
                tbl_attrs[tbl]["s_attrs"],
            )
            
            if temporal_granu:
                for t in t_attrs:
                    spatio_temporal_keys.append((tbl, SpatioTemporalKey(temporal_attr=Attr(t['name'], temporal_granu))))
                    if type_aware:
                        spatio_temporal_keys_by_type[KeyType.TIME].append((tbl, SpatioTemporalKey(
                            temporal_attr=Attr(t['name'], temporal_granu))))

            if spatial_granu:
                for s in s_attrs:
                    if s['granu'] != 'POINT' and s['granu'] != spatial_granu.name:
                        continue
                    spatio_temporal_keys.append((tbl, SpatioTemporalKey(spatial_attr=Attr(s['name'], spatial_granu))))
                    if type_aware:
                        spatio_temporal_keys_by_type[KeyType.SPACE].append((tbl, SpatioTemporalKey(
                            spatial_attr=Attr(s['name'], spatial_granu))))

            if temporal_granu and spatial_granu:
                for t in t_attrs:
                    for s in s_attrs:
                        if s['granu'] != 'POINT' and s['granu'] != spatial_granu.name:
                            continue
                        spatio_temporal_keys.append(
                            (tbl, SpatioTemporalKey(Attr(t['name'], temporal_granu), Attr(s['name'], spatial_granu)))
                        )
                        if type_aware:
                            spatio_temporal_keys_by_type[KeyType.TIME_SPACE].append((tbl, SpatioTemporalKey(
                                Attr(t['name'], temporal_granu), Attr(s['name'], spatial_granu))))
        if type_aware:
            return spatio_temporal_keys_by_type
        else:
            return spatio_temporal_keys

    def count_avg_rows(self, t_scale, s_scale, threshold=0):
        all_spatio_temporal_keys = self.load_all_spatio_temporal_keys(self.data_catalog, t_scale, s_scale)
        total_cnt = 0
        all_cnts = []
        for spatio_temporal_key in all_spatio_temporal_keys:
            row_cnt = self.db_engine.get_row_cnt(spatio_temporal_key[0], spatio_temporal_key[1])
            if row_cnt >= threshold:
                total_cnt += row_cnt
                all_cnts.append(row_cnt)
        print(f"num of all schemas: {len(all_spatio_temporal_keys)}")
        print(total_cnt / len(all_spatio_temporal_keys))
        print(f"median: {np.median(all_cnts)}")

    def _get_join_cost(self, t_scale, s_scale, threshold):
        all_schemas = Profiler.load_all_spatio_temporal_keys(self.data_catalog, t_scale, s_scale)
        total_cnt = 0
        # group tbls by their schema types since only tbls with the same schema types can join with each other
        # schemas in the same tbl can not join
        all_cnts = {KeyType.TIME: [], KeyType.SPACE: [], KeyType.TIME_SPACE: []}
        total_cnts = {KeyType.TIME: 0, KeyType.SPACE: 0, KeyType.TIME_SPACE: 0}

        for tbl, st_schema in all_schemas:
            agg_name = st_schema.get_agg_tbl_name(tbl)
            st_type = st_schema.get_type()
            row_cnt = Profiler.get_row_cnt(self.cur, tbl, st_schema)

            if row_cnt >= threshold:
                cnts_list = all_cnts[st_type]
                cnts_list.append((tbl, agg_name, row_cnt))
                total_cnts[st_type] += row_cnt
        # print(total_cnts)
        res = {}
        for st_type in [KeyType.TIME, KeyType.SPACE, KeyType.TIME_SPACE]:
            cnts_list = all_cnts[st_type]
            cnts_list = sorted(cnts_list, key=lambda x: x[2], reverse=True)
            total_cnt = total_cnts[st_type]
            n = len(cnts_list)
            for i, tbl_cnt in enumerate(cnts_list):
                tbl, agg_tbl, cnt = tbl_cnt[0], tbl_cnt[1], tbl_cnt[2]
                avg_cnt = total_cnt / (n - i)
                total_cnt -= cnt  # substract the current cnt
                """
                for tbls having cnts larger than cur tbl, the cost is cnt
                otherwise, the cost is avg_cnt
                """
                cost = i / n * cnt + (1 - i / n) * avg_cnt
                Stats = namedtuple("Stats", ["cost", "cnt"])
                res[agg_tbl] = Stats(round(cost, 2), cnt)
        # for tbl, val in res.items():
        #     print(tbl, val)
        return res

    @staticmethod
    def get_join_cost(db_engine, tbl_attrs, temporal_granu, spatial_granu, threshold):
        all_spatio_temporal_keys = Profiler.load_all_spatio_temporal_keys(tbl_attrs, temporal_granu, spatial_granu)
        total_cnt = 0
        # group tbls by their schema types since only tbls with the same schema types can join with each other
        # schemas in the same tbl can not join
        all_cnts = {KeyType.TIME: [], KeyType.SPACE: [], KeyType.TIME_SPACE: []}
        total_cnts = {KeyType.TIME: 0, KeyType.SPACE: 0, KeyType.TIME_SPACE: 0}

        for tbl, spatio_temporal_key in all_spatio_temporal_keys:
            agg_name = spatio_temporal_key.get_agg_tbl_name(tbl)
            st_type = spatio_temporal_key.get_type()
            row_cnt = db_engine.get_row_cnt(tbl, spatio_temporal_key)
            if row_cnt >= threshold:
                cnts_list = all_cnts[st_type]
                cnts_list.append((tbl, agg_name, row_cnt))
                total_cnts[st_type] += row_cnt
        # print(total_cnts)
        res = {}
        for st_type in [KeyType.TIME, KeyType.SPACE, KeyType.TIME_SPACE]:
            cnts_list = all_cnts[st_type]
            cnts_list = sorted(cnts_list, key=lambda x: x[2], reverse=True)
            total_cnt = total_cnts[st_type]
            n = len(cnts_list)
            for i, tbl_cnt in enumerate(cnts_list):
                tbl, agg_tbl, cnt = tbl_cnt[0], tbl_cnt[1], tbl_cnt[2]
                avg_cnt = total_cnt / (n - i)
                total_cnt -= cnt  # substract the current cnt
                """
                for tbls having cnts larger than cur tbl, the cost is cnt
                otherwise, the cost is avg_cnt
                """
                cost = i / n * cnt + (1 - i / n) * avg_cnt
                Stats = namedtuple("Stats", ["cost", "cnt"])
                res[agg_tbl] = Stats(round(cost, 2), cnt)
        return res

    def profile_tbl(self, table: Table,
                    temporal_granu_l: List[TEMPORAL_GRANU], spatial_granu_l: List[SPATIAL_GRANU], mode='no_cross'):
        spatio_temporal_keys = table.get_spatio_temporal_keys(temporal_granu_l, spatial_granu_l, mode)
        for spatio_temporal_key in spatio_temporal_keys:
            self.profile_spatio_temporal_key(table, spatio_temporal_key)

    def profile_spatio_temporal_key(self, table: Table, spatio_temporal_key: SpatioTemporalKey):
        variables = table.get_variables()
        agg_tbl_name = spatio_temporal_key.get_agg_tbl_name(table.tbl_id)
        self.stats_dict[agg_tbl_name] = {}
        for variable in variables:
            stats = self.db_engine.get_variable_stats(agg_tbl_name, variable.var_name)
            self.stats_dict[agg_tbl_name][variable.var_name] = stats

    def profile_original_data(self):
        profiles = {}
        for tbl_id, info in tqdm(self.data_catalog.items()):
            profile = {}
            f_path = "{}/{}.csv".format(self.config["data_path"], tbl_id)
            df = pd.read_csv(f_path, low_memory=False)
            for num_col in info["num_columns"]:
                if len(num_col) > 59:
                    continue
                missing_ratio = round(df[num_col].isnull().sum() / len(df[num_col]), 2)
                zero_ratio = round((df[num_col] == 0).sum() / len(df[num_col]), 2)
                num_col_name1 = "avg_{}_t1".format(num_col)[:63]
                num_col_name2 = "avg_{}_t2".format(num_col)[:63]
                num_col_name3 = "avg_{}".format(num_col)[:63]
                profile[num_col_name1] = {
                    "missing_ratio": missing_ratio,
                    "zero_ratio": zero_ratio,
                }
                profile[num_col_name2] = {
                    "missing_ratio": missing_ratio,
                    "zero_ratio": zero_ratio,
                }
                profile[num_col_name3] = {
                    "missing_ratio": missing_ratio,
                    "zero_ratio": zero_ratio,
                }

            profiles[tbl_id] = profile

        io_utils.dump_json(
            self.config["profile_path"],
            profiles,
        )

    @staticmethod
    def get_total_num_rows_original(data_source: str):
        config = io_utils.load_config(data_source)
        attr_path = config["attr_path"]
        tbl_attrs = io_utils.load_json(attr_path)
        total_row_cnt = 0
        for tbl_id, info in tqdm(tbl_attrs.items()):
            f_path = "{}/{}.csv".format(config["data_path"], tbl_id)
            df = pd.read_csv(f_path, low_memory=False)
            total_row_cnt += len(df)
        return total_row_cnt


if __name__ == "__main__":
    # t_scales = [T_GRANU.DAY, T_GRANU.MONTH]
    # s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]
    # t_scales = [T_GRANU.MONTH]
    # s_scales = [S_GRANU.TRACT]
    # data_source = 'chicago_1m_time_sampling'
    data_sources = [
        'ny_open_data', 'ct_open_data', 'maryland_open_data', 'pa_open_data',
        'texas_open_data', 'wa_open_data', 'sf_open_data', 'la_open_data',
        'nyc_open_data', 'chicago_open_data'
    ]
    total_rows = 0
    for data_source in data_sources:
        total_rows += Profiler.get_total_num_rows_original(data_source)
    print(total_rows)

    # conn_str = "postgresql://yuegong@localhost/opendata_large"
    # for data_source in data_sources:
    #     profiler = Profiler(data_source, t_scales, s_scales, conn_str)
    #     profiler.set_mode('no_cross')
    #     print("begin collecting agg stats")
    #     profiler.collect_agg_tbl_col_stats()
    #     # print("begin profiling original data")
    #     if data_source not in ['chicago_open_data', 'nyc_open_data']:
    #         profiler.profile_original_data()
    # t_scales = [T_GRANU.DAY]
    # s_scales = [S_GRANU.STATE]
    # profiler = Profiler("cdc_1m", t_scales, s_scales)
    # profiler.collect_agg_tbl_col_stats()
    # profiler.profile_original_data()
