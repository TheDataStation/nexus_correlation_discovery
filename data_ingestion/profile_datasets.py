from utils import io_utils
from utils.data_model import SpatioTemporalKey, Attr, KeyType
import pandas as pd
from tqdm import tqdm
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import numpy as np
from collections import namedtuple

"""
Collect the following stats for each aggregated table

agg_tbl_name -> [col_name->[stat_name->val]]
"""


class Profiler:
    def __init__(self, source: str, t_scales, s_scales, conn_string=None) -> None:
        self.config = io_utils.load_config(source)
        attr_path = self.config["attr_path"]
        self.tbl_attrs = io_utils.load_json(attr_path)
        if conn_string:
            self.conn_string = conn_string
        else:
            conn_string = self.config["db_path"]
        db = create_engine(conn_string)
        self.conn = db.connect()
        conn_copg2 = psycopg2.connect(conn_string)
        conn_copg2.autocommit = True
        self.cur = conn_copg2.cursor()
        self.stats_dict = {}
        self.t_scales = t_scales
        self.s_scales = s_scales
    
    def set_mode(self, mode):
        self.mode = mode

    def collect_agg_tbl_col_stats(self):
        for tbl in tqdm(self.tbl_attrs.keys()):
            print(tbl)
            t_attrs, s_attrs, num_columns = (
                self.tbl_attrs[tbl]["t_attrs"],
                self.tbl_attrs[tbl]["s_attrs"],
                self.tbl_attrs[tbl]["num_columns"],
            )
            self.profile_tbl(
                tbl, t_attrs, s_attrs, num_columns, self.t_scales, self.s_scales, self.mode
            )
        io_utils.dump_json(self.config["col_stats_path"], self.stats_dict)

    @staticmethod
    def load_st_schemas_for_a_tbl(tbl_attrs, tbl, t_scale, s_scale):
        st_schema_list = []
        t_attrs, s_attrs = (
            tbl_attrs[tbl]["t_attrs"],
            tbl_attrs[tbl]["s_attrs"],
        )

        if t_scale:
            for t in t_attrs:
                st_schema_list.append((tbl, SpatioTemporalKey(temporal_attr=Attr(t['name'], t_scale))))
              
        if s_scale:
            for s in s_attrs:
                if s['granu'] != 'POINT' and s['granu'] != s_scale.name:
                    continue
                st_schema_list.append((tbl, SpatioTemporalKey(spatial_attr=Attr(s['name'], s_scale))))
           
        if t_scale and s_scale:
            for t in t_attrs:
                for s in s_attrs:
                    if s['granu'] != 'POINT' and s['granu'] != s_scale.name:
                        continue
                    st_schema_list.append(
                        (tbl, SpatioTemporalKey(Attr(t['name'], t_scale), Attr(s['name'], s_scale)))
                    )
        return st_schema_list
                
    @staticmethod
    def load_all_st_schemas(tbl_attrs, t_scale, s_scale, type_aware=False):
        st_schema_list = []
       
        all_tbls = list(tbl_attrs.keys())
        if type_aware:
            st_schema_dict = {KeyType.TIME: [], KeyType.SPACE: [], KeyType.TIME_SPACE: []}
        for tbl in all_tbls:
            t_attrs, s_attrs = (
                tbl_attrs[tbl]["t_attrs"],
                tbl_attrs[tbl]["s_attrs"],
            )

            if t_scale:
                for t in t_attrs:
                    st_schema_list.append((tbl, SpatioTemporalKey(temporal_attr=Attr(t['name'], t_scale))))
                    if type_aware:
                        st_schema_dict[KeyType.TIME].append((tbl, SpatioTemporalKey(
                            temporal_attr=Attr(t['name'], t_scale))))

            if s_scale:
                for s in s_attrs:
                    if s['granu'] != 'POINT' and s['granu'] != s_scale.name:
                        continue
                    st_schema_list.append((tbl, SpatioTemporalKey(spatial_attr=Attr(s['name'], s_scale))))
                if type_aware:
                        st_schema_dict[KeyType.SPACE].append((tbl, SpatioTemporalKey(
                            spatial_attr=Attr(s['name'], s_scale))))

            if t_scale and s_scale:
                for t in t_attrs:
                    for s in s_attrs:
                        if s['granu'] != 'POINT' and s['granu'] != s_scale.name:
                            continue
                        st_schema_list.append(
                            (tbl, SpatioTemporalKey(Attr(t['name'], t_scale), Attr(s['name'], s_scale)))
                        )
                        if type_aware:
                            st_schema_dict[KeyType.TIME_SPACE].append((tbl, SpatioTemporalKey(Attr(t['name'], t_scale), Attr(s['name'], s_scale))))
        if type_aware:
            return st_schema_dict
        else:
            return st_schema_list

    def count_avg_rows(self, t_scale, s_scale, threshold=0):
        all_schemas = self.load_all_st_schemas(self.tbl_attrs, t_scale, s_scale)
        total_cnt = 0
        all_cnts = []
        for schema in all_schemas:
            row_cnt = self.get_row_cnt(self.cur, schema[0], schema[1])
            if row_cnt >= threshold:
                total_cnt += row_cnt
                all_cnts.append(row_cnt)
        print(f"num of all schemas: {len(all_schemas)}")
        print(total_cnt / len(all_schemas))
        print(f"median: {np.median(all_cnts)}")
    
    def _get_join_cost(self, t_scale, s_scale, threshold):
        all_schemas = Profiler.load_all_st_schemas(self.tbl_attrs, t_scale, s_scale)
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
    def get_join_cost(cur, tbl_attrs, t_scale, s_scale, threshold):
        all_schemas = Profiler.load_all_st_schemas(tbl_attrs, t_scale, s_scale)
        total_cnt = 0
        # group tbls by their schema types since only tbls with the same schema types can join with each other
        # schemas in the same tbl can not join
        all_cnts = {KeyType.TIME: [], KeyType.SPACE: [], KeyType.TIME_SPACE: []}
        total_cnts = {KeyType.TIME: 0, KeyType.SPACE: 0, KeyType.TIME_SPACE: 0}

        for tbl, st_schema in all_schemas:
            agg_name = st_schema.get_agg_tbl_name(tbl)
            st_type = st_schema.get_type()
            row_cnt = Profiler.get_row_cnt(cur, tbl, st_schema)
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

    @staticmethod
    def get_row_cnt(cur, tbl: str, st_schema: SpatioTemporalKey):
        sql_str = """
         SELECT count(*) from {tbl};
        """
        query = sql.SQL(sql_str).format(
            tbl=sql.Identifier(st_schema.get_agg_tbl_name(tbl))
        )
        cur.execute(query)
        return cur.fetchall()[0][0]

    def profile_tbl(self, tbl, t_attrs, s_attrs, num_columns, t_scales, s_scales, mode='no_cross'):
        st_schema_list = []
        for t in t_attrs:
            for scale in t_scales:
                st_schema_list.append(SpatioTemporalKey(temporal_attr=Attr(t["name"], scale)))

        for s in s_attrs:
            for scale in s_scales:
                if s["granu"] != 'POINT' and s["granu"] != scale.name:
                    continue
                st_schema_list.append(SpatioTemporalKey(spatial_attr=Attr(s["name"], scale)))

        for t in t_attrs:
            for s in s_attrs:
                if mode == 'cross':
                    for t_scale in t_scales:
                        for s_scale in s_scales:
                            if s["granu"] != 'POINT' and s["granu"] != s_scale.name:
                                continue
                            st_schema_list.append(
                                SpatioTemporalKey(Attr(t["name"], t_scale), Attr(s['name'], s_scale))
                            )
                elif mode == 'no_cross':
                    for i in range(len(t_scales)):
                        if s["granu"] != 'POINT' and s["granu"] != s_scales[i].name:
                            continue    
                        st_schema_list.append(SpatioTemporalKey(Attr(t["name"], t_scales[i]), Attr(s["name"], s_scales[i])))

        for st_schema in st_schema_list:
            self.profile_st_schema(tbl, st_schema, num_columns)

    def profile_st_schema(self, tbl, st_schema: SpatioTemporalKey, num_columns):
        vars = []
        for agg_col in num_columns:
            if len(agg_col) > 59:
                continue
            vars.append("avg_{}".format(agg_col))
        vars.append("count")
        col_names = st_schema.get_col_names_with_granu()
        # if st_schema.s_unit:
        #     print(st_schema.s_unit.attr_name, st_schema.s_unit.granu)
        # print(col_names)
        agg_tbl_name = "{}_{}".format(tbl, "_".join([col for col in col_names]))
        self.stats_dict[agg_tbl_name] = {}
        for var in vars:
            stats = self.get_stats(agg_tbl_name, var)
            self.stats_dict[agg_tbl_name][var] = stats

    def get_stats(self, agg_tbl_name, var_name):
        sql_str = """
            select sum({var}), sum({var}^2), round(avg({var})::numeric, 4), round((count(*)-1)*var_samp({var})::numeric,4), count(*)from {agg_tbl};
        """
        query = sql.SQL(sql_str).format(
            var=sql.Identifier(var_name), agg_tbl=sql.Identifier(agg_tbl_name)
        )
        # print(self.cur.mogrify(query))
        self.cur.execute(query)
        query_res = self.cur.fetchall()[0]
        return {
            "sum": float(query_res[0]) if query_res[0] != None else None,
            "sum_square": float(query_res[1]) if query_res[1] != None else None,
            "avg": float(query_res[2]) if query_res[2] != None else None,
            "res_sum": float(query_res[3]) if query_res[3] != None else None,
            "cnt": int(query_res[4]) if query_res[4] != None else None,
        }

    def profile_original_data(self):
        profiles = {}
        for tbl_id, info in tqdm(self.tbl_attrs.items()):
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
                profile[num_col_name1] = {
                    "missing_ratio": missing_ratio,
                    "zero_ratio": zero_ratio,
                }
                profile[num_col_name2] = {
                    "missing_ratio": missing_ratio,
                    "zero_ratio": zero_ratio,
                }

            profiles[tbl_id] = profile

        io_utils.dump_json(
            self.config["profile_path"],
            profiles,
        )
    
    @staticmethod
    def get_total_num_rows_original(data_source):
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
    