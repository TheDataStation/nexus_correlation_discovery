from utils import io_utils
from data_search.data_model import ST_Schema, Unit, T_GRANU, S_GRANU, SchemaType
import pandas as pd
from tqdm import tqdm
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import numpy as np
from collections import defaultdict, namedtuple

"""
Collect the following stats for each aggregated table

agg_tbl_name -> [col_name->[stat_name->val]]
"""


class Profiler:
    def __init__(self, source: str, t_scales, s_scales) -> None:
        self.config = io_utils.load_config(source)
        attr_path = self.config["attr_path"]
        self.tbl_attrs = io_utils.load_json(attr_path)
        conn_string = self.config["db_path"]
        db = create_engine(conn_string)
        self.conn = db.connect()
        conn_copg2 = psycopg2.connect(conn_string)
        conn_copg2.autocommit = True
        self.cur = conn_copg2.cursor()
        self.stats_dict = {}
        self.t_scales = t_scales
        self.s_scales = s_scales

    def collect_agg_tbl_col_stats(self):
        for tbl in tqdm(self.tbl_attrs.keys()):
            print(tbl)
            t_attrs, s_attrs, num_columns = (
                self.tbl_attrs[tbl]["t_attrs"],
                self.tbl_attrs[tbl]["s_attrs"],
                self.tbl_attrs[tbl]["num_columns"],
            )
            self.profile_tbl(
                tbl, t_attrs, s_attrs, num_columns, self.t_scales, self.s_scales
            )
        io_utils.dump_json(self.config["col_stats_path"], self.stats_dict)

    def load_all_st_schemas(self, t_scale, s_scale):
        st_schema_list = []
        all_tbls = list(self.tbl_attrs.keys())
        for tbl in all_tbls:
            t_attrs, s_attrs = (
                self.tbl_attrs[tbl]["t_attrs"],
                self.tbl_attrs[tbl]["s_attrs"],
            )

            for t in t_attrs:
                st_schema_list.append((tbl, ST_Schema(t_unit=Unit(t, t_scale))))

            for s in s_attrs:
                st_schema_list.append((tbl, ST_Schema(s_unit=Unit(s, s_scale))))

            for t in t_attrs:
                for s in s_attrs:
                    st_schema_list.append(
                        (tbl, ST_Schema(Unit(t, t_scale), Unit(s, s_scale)))
                    )
        return st_schema_list

    def count_avg_rows(self, t_scale, s_scale, threshold=0):
        all_schemas = self.load_all_st_schemas(t_scale, s_scale)
        total_cnt = 0
        all_cnts = []
        for schema in all_schemas:
            row_cnt = self.get_row_cnt(schema[0], schema[1])
            if row_cnt >= threshold:
                total_cnt += row_cnt
                all_cnts.append(row_cnt)
        print(f"num of all schemas: {len(all_schemas)}")
        print(total_cnt / len(all_schemas))
        print(f"median: {np.median(all_cnts)}")

    def get_join_cost(self, t_scale, s_scale, threshold):
        all_schemas = self.load_all_st_schemas(t_scale, s_scale)
        total_cnt = 0
        # group tbls by their schema types since only tbls with the same schema types can join with each other
        # schemas in the same tbl can not join
        all_cnts = {SchemaType.TIME: [], SchemaType.SPACE: [], SchemaType.TS: []}
        total_cnts = {SchemaType.TIME: 0, SchemaType.SPACE: 0, SchemaType.TS: 0}

        for tbl, st_schema in all_schemas:
            agg_name = st_schema.get_agg_tbl_name(tbl)
            st_type = st_schema.get_type()
            row_cnt = self.get_row_cnt(tbl, st_schema)

            if row_cnt >= threshold:
                cnts_list = all_cnts[st_type]
                cnts_list.append((tbl, agg_name, row_cnt))
                total_cnts[st_type] += row_cnt
        print(total_cnts)
        res = {}
        for st_type in [SchemaType.TIME, SchemaType.SPACE, SchemaType.TS]:
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

    def get_row_cnt(self, tbl: str, st_schema: ST_Schema):
        sql_str = """
         SELECT count(*) from {tbl};
        """
        query = sql.SQL(sql_str).format(
            tbl=sql.Identifier(st_schema.get_agg_tbl_name(tbl))
        )
        self.cur.execute(query)
        return self.cur.fetchall()[0][0]

    def profile_tbl(self, tbl, t_attrs, s_attrs, num_columns, t_scales, s_scales):
        st_schema_list = []
        for t in t_attrs:
            for scale in t_scales:
                st_schema_list.append(ST_Schema(t_unit=Unit(t, scale)))

        for s in s_attrs:
            for scale in s_scales:
                st_schema_list.append(ST_Schema(s_unit=Unit(s, scale)))

        for t in t_attrs:
            for s in s_attrs:
                for t_scale in t_scales:
                    for s_scale in s_scales:
                        st_schema_list.append(
                            ST_Schema(Unit(t, t_scale), Unit(s, s_scale))
                        )
        for st_schema in st_schema_list:
            self.profile_st_schema(tbl, st_schema, num_columns)

    def profile_st_schema(self, tbl, st_schema: ST_Schema, num_columns):
        vars = []
        for agg_col in num_columns:
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
            df = pd.read_csv(f_path)
            for num_col in info["num_columns"]:
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


if __name__ == "__main__":
    t_scales = [T_GRANU.DAY, T_GRANU.MONTH]
    s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]
    profiler = Profiler("chicago_1m", t_scales, s_scales)
    profiler.collect_agg_tbl_col_stats()
    # profiler.profile_original_data()
    # t_scales = [T_GRANU.DAY]
    # s_scales = [S_GRANU.STATE]
    # profiler = Profiler("cdc_1m", t_scales, s_scales)
    # profiler.collect_agg_tbl_col_stats()
    # profiler.profile_original_data()
    