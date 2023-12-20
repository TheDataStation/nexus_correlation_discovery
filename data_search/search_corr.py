import utils.io_utils as io_utils
import numpy as np
import pandas as pd
from data_search.data_model import (
    Unit,
    Variable,
    AggFunc,
    ST_Schema,
    SchemaType,
    get_st_schema_list_for_tbl,
)
from data_ingestion.profile_datasets import Profiler
from tqdm import tqdm
import time
import pandas as pd
from data_search.search_db import DBSearch
import os
from utils import corr_utils
from collections import defaultdict
from dataclasses import dataclass
from typing import List
import data_search.db_ops as db_ops
import math
from data_search.commons import FIND_JOIN_METHOD
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from scipy.stats import pearsonr

agg_col_profiles = None


@dataclass
class AggColumnProfile:
    missing_ratio: float
    zero_ratio: float
    missing_ratio_o: float
    zero_ratio_o: float
    cv: float

    def to_list(self):
        return [
            round(self.missing_ratio, 3),
            round(self.zero_ratio, 3),
            round(self.missing_ratio_o, 3),
            round(self.zero_ratio_o, 3),
            round(self.cv, 3),
        ]


class AggColumn:
    def __init__(
        self, tbl_id, tbl_name, st_schema: ST_Schema, agg_attr, col_data=None
    ) -> None:
        self.tbl_id = tbl_id
        self.tbl_name = tbl_name
        self.st_schema = st_schema
        self.agg_attr = agg_attr
        self.col_data = col_data

    def set_profile(self, col_data, tbl_profiles):
        missing_ratio = col_data.isnull().sum() / len(col_data)
        zero_ratio = (col_data == 0).sum() / len(col_data)
        # if the agg attr is using avg, calculate original missing and zero ratio
        missing_ratio_o, zero_ratio_o = 0, 0
        if self.agg_attr[0:3] == "avg":
            missing_ratio_o = tbl_profiles[self.agg_attr]["missing_ratio"]
            zero_ratio_o = tbl_profiles[self.agg_attr]["zero_ratio"]

        cv = col_data.dropna().std() / col_data.dropna().mean()
        self.profile = AggColumnProfile(
            missing_ratio=missing_ratio,
            zero_ratio=zero_ratio,
            missing_ratio_o=missing_ratio_o,
            zero_ratio_o=zero_ratio_o,
            cv=cv,
        )

    def get_stats(self, stat_name):
        return agg_col_profiles[self.st_schema.get_agg_tbl_name(self.tbl_id)][
            self.agg_attr[:-3]
        ][stat_name]

    def get_id(self):
        return self.tbl_id, tuple(self.st_schema.get_attrs()), self.agg_attr

    def to_list(self):
        return [
            self.tbl_id,
            self.tbl_name,
            self.st_schema.get_attrs(),
            self.agg_attr,
        ] + self.profile.to_list()


class Correlation:
    def __init__(
        self,
        agg_col1: AggColumn,
        agg_col2: AggColumn,
        r_val: float,
        p_val: float,
        overlap: int,
        align_type,
    ):
        self.agg_col1 = agg_col1
        self.agg_col2 = agg_col2
        self.r_val = r_val
        self.r_val_impute_avg = 0
        self.r_val_impute_zero = 0
        self.p_val = p_val
        self.overlap = overlap
        self.align_type = align_type

    def set_impute_avg_r(self, res_sum):
        # print(res_sum)
        self.r_val_impute_avg = res_sum / (
            math.sqrt(
                self.agg_col1.get_stats("res_sum") * self.agg_col2.get_stats("res_sum")
            )
        )
        # print("rval", self.r_val_impute_avg)

    def set_impute_zero_r(self, n, inner_prod):
        n = self.agg_col1.get_stats("cnt") + self.agg_col2.get_stats("cnt") - n
        sum1, sum2 = self.agg_col1.get_stats("sum"), self.agg_col2.get_stats("sum")
        square_sum1, square_sum2 = self.agg_col1.get_stats(
            "sum_square"
        ), self.agg_col2.get_stats("sum_square")
        self.r_val_impute_zero = (n * inner_prod - sum1 * sum2) / (
            math.sqrt(n * square_sum1 - sum1**2)
            * math.sqrt(n * square_sum2 - sum2**2)
        )

    def to_list(self):

        return (
            self.agg_col1.to_list()
            + self.agg_col2.to_list()
            + [
                round(self.r_val, 3),
                round(self.r_val_impute_avg, 3),
                round(self.r_val_impute_zero, 3),
                round(self.p_val, 3),
                self.overlap,
                self.align_type,
            ]
        )


class CorrSearch:
    def __init__(
        self,
        conn_str: str,
        data_source: str,
        find_join_method,
        join_costs,
        join_method,
        corr_method,
        r_methods,
        explicit_outer_join,
        correct_method,
        q_val=None,
    ) -> None:
        config = io_utils.load_config(data_source)
        attr_path = config["attr_path"]
        self.tbl_attrs = io_utils.load_json(attr_path)
        self.all_tbls = set(self.tbl_attrs.keys())

        profile_path = config["profile_path"]
        self.column_profiles = io_utils.load_json(profile_path)

        agg_col_profile_path = config["col_stats_path"]
        global agg_col_profiles
        agg_col_profiles = io_utils.load_json(agg_col_profile_path)

        self.db_search = DBSearch(conn_str)
        self.cur = self.db_search.cur

        self.data = []
        self.count = 0
        self.visited_tbls = set()
        self.visited_schemas = set()
       
        self.find_join_method = find_join_method
        self.join_costs = join_costs
        self.join_all_cost = 0
        self.join_method = join_method
        self.corr_method = corr_method
        self.r_methods = r_methods
        self.outer_join = explicit_outer_join
        self.correct_method = correct_method
        self.q_val = q_val
        self.overhead = 0
        self.perf_profile = {
            "num_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_find_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_join": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_correlation": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_correction": {"total": 0},
            "time_dump_csv": {"total": 0},
            "time_create_tmp_tables": {"total": 0},
            "corr_counts": {"before": 0, "after": 0},
            "corr_count": {"total": 0},
            "strategy": {"find_join": 0, "join_all": 0, "skip": 0, "sample_times": 0},
        }

    
    def dump_corrs_to_csv(self, data: List[Correlation], dir_path, tbl_id):
        df = pd.DataFrame(
            [corr.to_list() for corr in data],
            columns=[
                "tbl_id1",
                "tbl_name1",
                "align_attrs1",
                "agg_attr1",
                "missing_ratio1",
                "zero_ratio1",
                "missing_ratio_o1",
                "zero_ratio_o1",
                "cv1",
                "tbl_id2",
                "tbl_name2",
                "align_attrs2",
                "agg_attr2",
                "missing_ratio2",
                "zero_ratio2",
                "missing_ratio_o2",
                "zero_ratio_o2",
                "cv2",
                "r_val",
                "r_impute_avg_val",
                "r_impute_zero_val",
                "p_val",
                "samples",
                "align_type",
            ],
        )

        if not os.path.exists(dir_path):
            # create the directory if it does not exist
            os.makedirs(dir_path)

        df.to_csv("{}/corr_{}.csv".format(dir_path, tbl_id))

    def find_all_corr_for_all_tbls(
        self, granu_list, o_t, r_t, p_t, fill_zero=False, dir_path=None
    ):
        for tbl in tqdm(self.tbl_attrs.keys()):
            print(tbl)
            self.find_all_corr_for_a_tbl(tbl, granu_list, o_t, r_t, p_t, fill_zero)

            start = time.time()
            if dir_path:
                self.dump_corrs_to_csv(self.data, dir_path, tbl)
            # after a table is done, clear the data
            self.perf_profile["corr_count"]["total"] += len(self.data)
            self.data.clear()
            time_used = time.time() - start
            self.perf_profile["time_dump_csv"]["total"] += time_used

    def find_all_corr_for_a_tbl(self, tbl, granu_list, o_t, r_t, p_t, fill_zero):
        st_schema_list = []
        t_attrs, s_attrs = (
            self.tbl_attrs[tbl]["t_attrs"],
            self.tbl_attrs[tbl]["s_attrs"],
        )
        t_scale, s_scale = granu_list[0], granu_list[1]
        for t in t_attrs:
            st_schema_list.append(ST_Schema(t_unit=Unit(t, t_scale)))

        for s in s_attrs:
            st_schema_list.append(ST_Schema(s_unit=Unit(s, s_scale)))

        for t in t_attrs:
            for s in s_attrs:
                st_schema_list.append(ST_Schema(Unit(t, t_scale), Unit(s, s_scale)))

        for st_schema in st_schema_list:
            print("r_t", r_t)
            self.find_all_corr_for_a_tbl_schema(
                tbl, st_schema, o_t, r_t, p_t, fill_zero
            )

    def find_corr_in_a_tbl(self, tbl, granu_list, r_t, p_t):
        """
        Find correlations between variables in a table.
        """
        t_attrs, s_attrs = (
            self.tbl_attrs[tbl]["t_attrs"],
            self.tbl_attrs[tbl]["s_attrs"],
        )
        st_schema_list = get_st_schema_list_for_tbl(t_attrs, s_attrs, granu_list[0], granu_list[1], [SchemaType.TIME, SchemaType.SPACE, SchemaType.TS])
        corrs = []
        for st_schema in st_schema_list:
            res = self.find_corr_in_a_tbl_schema(
                tbl, st_schema, r_t, p_t
            )
            corrs.extend(res)
        return corrs
    
  
    def align_two_st_schemas(self, tbl1, st_schema1, tbl2, st_schema2, o_t, outer):
        tbl1_agg_cols = self.tbl_attrs[tbl1]["num_columns"]
        tbl2_agg_cols = self.tbl_attrs[tbl2]["num_columns"]

        vars1 = []
        vars2 = []
        for agg_col in tbl1_agg_cols:
            vars1.append(Variable(agg_col, AggFunc.AVG, "avg_{}_t1".format(agg_col)))
        if len(tbl1_agg_cols) == 0 or tbl1 == '85ca-t3if':
            vars1.append(Variable("*", AggFunc.COUNT, "count_t1"))
        for agg_col in tbl2_agg_cols:
            vars2.append(Variable(agg_col, AggFunc.AVG, "avg_{}_t2".format(agg_col)))
        if len(tbl2_agg_cols) == 0 or tbl2 == '85ca-t3if':
            vars2.append(Variable("*", AggFunc.COUNT, "count_t2"))

        # column names in postgres are at most 63-character long
        names1 = [var.var_name[:63] for var in vars1]
        names2 = [var.var_name[:63] for var in vars2]

        merged = None

        if self.join_method == "AGG":
            if self.outer_join:
                # merged = db_ops.join_two_agg_tables(self.cur,
                #     tbl1,
                #     st_schema1,
                #     vars1,
                #     tbl2,
                #     st_schema2,
                #     vars2,
                #     outer=False)
                merged_outer = db_ops.join_two_agg_tables(
                    self.cur,
                    tbl1,
                    st_schema1,
                    vars1,
                    tbl2,
                    st_schema2,
                    vars2,
                    outer=True,
                )
  
                merged = merged_outer.dropna(subset=["key1", "key2"])
            else:
                merged = db_ops.join_two_agg_tables(
                    self.cur, tbl1, st_schema1, vars1, tbl2, st_schema2, vars2, outer=False
                )
            
        elif self.join_method == "ALIGN":
            # begin align tables
            merged = self.db_search.align_two_two_tables(
                tbl1, st_schema1, vars1, tbl2, st_schema2, vars2
            )

        elif self.join_method == "JOIN":
            # begin joining tables
            merged = self.db_search.aggregate_join_two_tables(
                tbl1, st_schema1, vars1, tbl2, st_schema2, vars2
            )

        if merged is None or len(merged) < o_t:
            if not self.outer_join:
                return None, None
            else:
                return None, None, None, None

        df1, df2 = merged[names1].astype(float), merged[names2].astype(float)
        if outer:
            df1_outer, df2_outer = merged_outer[names1].astype(float), merged_outer[
                names2
            ].astype(float)
            return df1, df2, df1_outer, df2_outer
        return df1, df2

    def determine_find_join_method(
        self, tbl, st_schema: ST_Schema, threshold, v_cnt: int
    ):
        agg_tbl = st_schema.get_agg_tbl_name(tbl)
        print(f"current table: {agg_tbl}")
        self.visited_tbls.add(tbl)
        self.visited_schemas.add(agg_tbl)

        # median = 6447
        # v_cnt = min(v_cnt, median)

        # estimated join_all cost
        join_cost = self.join_costs[st_schema.get_agg_tbl_name(tbl)].cost
        print(f"estimated join cost is {join_cost}")

        aligned_schemas = []
        aligned_tbls = self.all_tbls

        join_all_cost = 0
        for tbl2 in aligned_tbls:
            if tbl2 == tbl:
                continue
            t_attrs, s_attrs = (
                self.tbl_attrs[tbl2]["t_attrs"],
                self.tbl_attrs[tbl2]["s_attrs"],
            )

            st_schema_list = get_st_schema_list_for_tbl(
                t_attrs,
                s_attrs,
                st_schema.t_unit,
                st_schema.s_unit,
                [st_schema.get_type()],
            )

            for st_schema2 in st_schema_list:
                agg_name = st_schema2.get_agg_tbl_name(tbl2)
               
                if agg_name not in self.join_costs or agg_name in self.visited_schemas:
                    continue  # meaning it does not have enough keys
                cnt2 = self.join_costs[agg_name].cnt
                join_all_cost += min(cnt2, v_cnt)
                aligned_schemas.append((tbl2, st_schema2))
        # join_all_cost = min(len(aligned_schemas) * join_cost, join_all_cost)
        
        # estimate index_search cost
        row_to_read, max_joinable = db_ops.get_inv_cnt(
            self.cur, tbl, st_schema, threshold
        )

        # coef = 3
        if v_cnt >= 100000:
            coef = 4.2#7
        else:
            coef = 0.1#0.15
        index_search_overhead = coef*(v_cnt + row_to_read)
        self.index_search_over_head = row_to_read
        max_joinable = min(max_joinable, len(aligned_schemas))
        # if max_joinable >= len(aligned_schemas):
        #     find_join_cost = index_search_over_head + join_all_cost
        # else:
        find_join_cost = index_search_overhead + max_joinable * v_cnt
        print(
            f"row_to_read: {row_to_read}; join all cost: {join_all_cost}; find join cost: {find_join_cost}; max_joinable: {min(max_joinable, len(aligned_schemas))}"
        )

        start = time.time()
        sample_ratio = 0.05
        if v_cnt > 200000:
            sample_ratio = 0.05
        min_sample_rows, max_sampled_rows = 1000, 10000
        sampled_rows = v_cnt * sample_ratio
        # if v_cnt >= 400000:
        #     return "JOIN_ALL", aligned_schemas
        sample_cost = index_search_overhead # sampling needs to read the column
        if find_join_cost <= join_all_cost:
            return "FIND_JOIN", None
        elif index_search_overhead >= join_all_cost:
            return "JOIN_ALL", aligned_schemas
        else:
            # if v_cnt <= sampled_rows:
            #     return "FIND_JOIN", None
            # if v_cnt <= min_sample_rows / sample_ratio:
            #     return "FIND_JOIN", None
            # elif v_cnt >= max_sampled_rows / sample_ratio:
            #     return "JOIN_ALL", aligned_schemas
            # if len(aligned_schemas) <= 10:
            #     return "JOIN_ALL", aligned_schemas
            self.perf_profile["strategy"]["sample_times"] += 1
            candidates, total_elements_sampled = db_ops.get_intersection_inv_idx(
                self.cur, tbl, st_schema, threshold, sampled_rows
            )
            if total_elements_sampled != 0:
                scale_factor = row_to_read // total_elements_sampled
                print(
                    f"total_elements_sampled: {total_elements_sampled}, scale_factor: {scale_factor}"
                )
            else:
                scale_factor = 0

            joinable_estimate = 0
            avg_join_cost = 0
            for tbl, schema, overlap in candidates:
                cand = schema.get_agg_tbl_name(tbl)
                if cand not in self.join_costs:
                    continue
                if overlap * scale_factor >= threshold:
                    if cand not in self.visited_schemas:
                        joinable_estimate += 1
                        avg_join_cost += min(v_cnt, self.join_costs[cand].cnt)
            if len(candidates) == 0 or joinable_estimate == 0:
                avg_join_cost = join_cost
            else:
                avg_join_cost = avg_join_cost / joinable_estimate
            print(
                f"index_search cost: {index_search_overhead + joinable_estimate * avg_join_cost}; joinable_estimate: {joinable_estimate}; join cost estimate: {avg_join_cost}"
            )
            print(f"step 5 takes {time.time() - start}")
            if (
                index_search_overhead + joinable_estimate * avg_join_cost
                <= join_all_cost
            ):
                return "FIND_JOIN", None
            else:
                return "JOIN_ALL", aligned_schemas

    def find_corr_in_a_tbl_schema(self, tbl, st_schema: ST_Schema, r_t, p_t):
        corrs = []
        flag = st_schema.get_type().value
        tbl_agg_cols = self.tbl_attrs[tbl]["num_columns"]
        
        vars = []
      
        for agg_col in tbl_agg_cols:
            vars.append(Variable(agg_col, AggFunc.AVG, "avg_{}_t1".format(agg_col)))
        if len(tbl_agg_cols) == 0 or tbl == '85ca-t3if':
            vars.append(Variable("*", AggFunc.COUNT, "count_t1"))
        
        df = db_ops.read_agg_tbl(self.cur, tbl, st_schema, vars)
        for i, col1 in enumerate(df.columns):
            for j, col2 in enumerate(df.columns):
                if i < j:
                    r_v, p_v = pearsonr(df[col1], df[col2])
                    if r_v >= r_t and p_v <= p_t:
                        agg_col1 = AggColumn(tbl, self.tbl_attrs[tbl]["name"], st_schema, col1, df[col1])
                        agg_col2 = AggColumn(tbl, self.tbl_attrs[tbl]["name"], st_schema, col2, df[col2])
                        corr = Correlation(agg_col1, agg_col2, r_v, p_v, len(df), flag)
                        corr.agg_col1.set_profile(
                            corr.agg_col1.col_data,
                            self.column_profiles[corr.agg_col1.tbl_id],
                        )
                        corr.agg_col2.set_profile(
                            corr.agg_col2.col_data,
                            self.column_profiles[corr.agg_col2.tbl_id],
                        )
                        corrs.append(corr)
        return corrs

    def find_all_corr_for_a_tbl_schema(
        self, tbl1, st_schema: ST_Schema, o_t, r_t, p_t, fill_zero
    ):
        print("enter")
        self.join_all_cost = 0
        self.cur_join_time = 0
        flag = st_schema.get_type().value
        # join method to be used
        method = self.find_join_method
        """
        Find aligned schemas whose overlap with the input st_schema is greater then o_t
        """
        start = time.time()
        self.visited_tbls.add(tbl1)
        agg_name1 = st_schema.get_agg_tbl_name(tbl1)
        self.visited_schemas.add(agg_name1)
        if agg_name1 not in self.join_costs:
            print("skip because this table does not have enough keys")
            self.perf_profile["strategy"]["skip"] += 1
            return
        v_cnt = self.join_costs[agg_name1].cnt

        if self.find_join_method == FIND_JOIN_METHOD.INDEX_SEARCH:
            aligned_schemas = self.db_search.find_augmentable_st_schemas(
                tbl1, st_schema, o_t, mode="inv_idx"
            )
        elif self.find_join_method == FIND_JOIN_METHOD.JOIN_ALL:
            aligned_schemas = []
            aligned_tbls = self.all_tbls
            for tbl2 in aligned_tbls:
                if tbl2 == tbl1:
                    continue
                t_attrs, s_attrs = (
                    self.tbl_attrs[tbl2]["t_attrs"],
                    self.tbl_attrs[tbl2]["s_attrs"],
                )
                st_schema_list = get_st_schema_list_for_tbl(
                    t_attrs,
                    s_attrs,
                    st_schema.t_unit,
                    st_schema.s_unit,
                    [st_schema.get_type()],
                )
                for st_schema2 in st_schema_list:
                    agg_name2 = st_schema2.get_agg_tbl_name(tbl2)
                    
                    if (
                        agg_name2 not in self.join_costs
                        or agg_name2 in self.visited_schemas 
                    ):
                        continue  # meaning it does not have enough keys
                    
                    aligned_schemas.append((tbl2, st_schema2))

        elif self.find_join_method == FIND_JOIN_METHOD.COST_MODEL:
            s = time.time()
            method, schemas = self.determine_find_join_method(
                tbl1, st_schema, o_t, v_cnt
            )
            self.overhead += time.time() - s
            print(f"choose {method}")
            if method == "FIND_JOIN":
                method = FIND_JOIN_METHOD.INDEX_SEARCH
                self.perf_profile["strategy"]["find_join"] += 1
                aligned_schemas = self.db_search.find_augmentable_st_schemas(
                    tbl1, st_schema, o_t, mode="inv_idx"
                )
            elif method == "JOIN_ALL":
                method = FIND_JOIN_METHOD.JOIN_ALL
                self.perf_profile["strategy"]["join_all"] += 1
                aligned_schemas = schemas

        time_used = time.time() - start
        self.cur_find_join_time = time_used
        self.perf_profile["time_find_joins"]["total"] += time_used
        self.perf_profile["time_find_joins"][flag] += time_used

        """
        Begin to align and compute correlations
        """
        tbl_schema_corrs = []
        for tbl_info in aligned_schemas:
            tbl2, st_schema2 = (
                tbl_info[0],
                tbl_info[1],
            )
            agg_name2 = st_schema2.get_agg_tbl_name(tbl2)
            if tbl2 == tbl1 or agg_name2 in self.visited_schemas:
                continue
            
            self.join_all_cost += min(v_cnt, self.join_costs[agg_name2].cnt)
            # Align two schemas
            start = time.time()
            df1_outer, df2_outer = None, None
            if not self.outer_join:
                df1, df2 = self.align_two_st_schemas(
                    tbl1, st_schema, tbl2, st_schema2, o_t, outer=False
                )
            else:
                df1, df2, df1_outer, df2_outer = self.align_two_st_schemas(
                    tbl1, st_schema, tbl2, st_schema2, o_t, outer=True
                )
            time_used = time.time() - start
            self.cur_join_time += time_used
            self.perf_profile["time_join"]["total"] += time_used
            self.perf_profile["time_join"][flag] += time_used

            if df1 is None or df2 is None:
                continue
           
            self.perf_profile["num_joins"]["total"] += 1
            self.perf_profile["num_joins"][flag] += 1

            # Calculate correlation
            start = time.time()
            res = []
            attrs1 = st_schema.get_attrs()
            attrs2 = st_schema2.get_attrs()
            if self.corr_method == "MATRIX":
                res = self.get_corr_opt(
                    df1,
                    df2,
                    df1_outer,
                    df2_outer,
                    tbl1,
                    st_schema,
                    tbl2,
                    st_schema2,
                    r_t,
                    p_t,
                    fill_zero,
                    flag,
                )

            if self.corr_method == "FOR_PAIR":
                res = self.get_corr_naive(
                    df1,
                    df2,
                    df1_outer,
                    df2_outer,
                    tbl1,
                    st_schema,
                    tbl2,
                    st_schema2,
                    r_t,
                    p_t,
                    fill_zero,
                    flag,
                )
            if res is not None:
                tbl_schema_corrs.extend(res)
            time_used = time.time() - start
            self.perf_profile["time_correlation"]["total"] += time_used
            self.perf_profile["time_correlation"][flag] += time_used

        """
        Perform multiple-comparison correction
        """
        print(len(tbl_schema_corrs))
        start = time.time()
        if self.correct_method == "FDR":
            tbl_schema_corrs = self.bh_correction(tbl_schema_corrs, r_t)
            self.perf_profile["corr_counts"]["after"] += len(tbl_schema_corrs)
        self.perf_profile["time_correction"]["total"] += time.time() - start
        self.data.extend(tbl_schema_corrs)
        return method

    def bh_correction(self, corrs: List[Correlation], r_t):
        filtered_corrs = []
        # group correlations by their starting columns
        corr_groups = defaultdict(list)
        for corr in corrs:
            corr_groups[corr.agg_col1.get_id()].append(corr)

        for corr_group in corr_groups.values():
            # sort corr_group by p_value
            corr_group.sort(key=lambda a: a.p_val)
            n = len(corr_group)
            largest_i = -1
            for i, corr in enumerate(corr_group):
                bh_value = ((i + 1) / n) * self.q_val
                if corr.p_val < bh_value:
                    largest_i = i
            corrected_corr_group = []
            if largest_i >= 0:
                # print("largest i", largest_i)
                for corr in corr_group[0 : largest_i + 1]:
                    if abs(corr.r_val) >= r_t:
                        corr.agg_col1.set_profile(
                            corr.agg_col1.col_data,
                            self.column_profiles[corr.agg_col1.tbl_id],
                        )
                        corr.agg_col2.set_profile(
                            corr.agg_col2.col_data,
                            self.column_profiles[corr.agg_col2.tbl_id],
                        )
                        corrected_corr_group.append(corr)
            filtered_corrs.extend(corrected_corr_group)
        return filtered_corrs

    def get_o_mean_mat(self, tbl, st_schema, df):
        stats = agg_col_profiles[st_schema.get_agg_tbl_name(tbl)]
        vec = []
        vec_dict = {}
        rows = len(df)
        names = df.columns
        for name in names:
            _name = name[:-3]
            # remove invalid columns (columns that are all nulls or have only one non-null value)
            average = stats[_name]["avg"]
            res_sum = stats[_name]["res_sum"]
            if average is None or res_sum is None or res_sum == 0:
                df = df.drop(name, axis=1)
                continue
            vec.append(average)
            vec_dict[name] = average

        o_avg_mat = np.repeat([vec], rows, axis=0)
        return df, o_avg_mat, vec_dict

    def get_corr_opt(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        df1_outer: pd.DataFrame,
        df2_outer: pd.DataFrame,
        tbl1,
        st_schema1,
        tbl2,
        st_schema2,
        r_threshold,
        p_threshold,
        fill_zero,
        flag,
    ):
        res = []
        if fill_zero:
            df1, o_avg_mat1, avg_dict1 = self.get_o_mean_mat(tbl1, st_schema1, df1)
            df2, o_avg_mat2, avg_dict2 = self.get_o_mean_mat(tbl2, st_schema2, df2)
            if df1.shape[1] == 0 or df2.shape[1] == 0:
                # meaning there is no valid column in a table
                return None
            if self.outer_join:
                df1_outer, df2_outer = df1_outer[df1.columns], df2_outer[df2.columns]

            names1, names2 = df1.columns, df2.columns
            mat1, mat2 = df1.fillna(0).to_numpy(), df2.fillna(0).to_numpy()
            mat1_avg, mat2_avg = None, None
            # if "impute_avg" in self.r_methods and not self.outer_join:
            #     mat1_avg, mat2_avg = (
            #         df1.fillna(avg_dict1).to_numpy(),
            #         df2.fillna(avg_dict2).to_numpy(),
            #     )
            mat1_avg, mat2_avg = (
                    df1.fillna(avg_dict1).to_numpy(),
                    df2.fillna(avg_dict2).to_numpy(),
            )
            mat_dict = corr_utils.mat_corr(
                mat1,
                mat2,
                mat1_avg,
                mat2_avg,
                o_avg_mat1,
                o_avg_mat2,
                names1,
                names2,
                False,
                self.r_methods,
                self.outer_join,
            )
            if self.outer_join:
                df1_outer, df2_outer = df1_outer[df1.columns], df2_outer[df2.columns]
                if "impute_avg" in self.r_methods:
                    mat_dict_outer = corr_utils.mat_corr(
                        df1_outer.fillna(df1_outer.mean()).to_numpy(),
                        df2_outer.fillna(df2_outer.mean()).to_numpy(),
                        mat1_avg,
                        mat2_avg,
                        o_avg_mat1,
                        o_avg_mat2,
                        names1,
                        names2,
                        False,
                        self.r_methods,
                        self.outer_join,
                    )
                    corr_impute_avg = mat_dict_outer["corrs"]
                if "impute_zero" in self.r_methods:
                    mat_dict_outer = corr_utils.mat_corr(
                        df1_outer.fillna(0).to_numpy(),
                        df2_outer.fillna(0).to_numpy(),
                        mat1_avg,
                        mat2_avg,
                        o_avg_mat1,
                        o_avg_mat2,
                        names1,
                        names2,
                        False,
                        self.r_methods,
                        self.outer_join,
                    )
                    corr_impute_zero = mat_dict_outer["corrs"]

            corr_mat = mat_dict["corrs"]
            pval_mat = mat_dict["p_vals"]
            if "impute_avg" in self.r_methods and not self.outer_join:
                res_sum_mat = mat_dict["res_sum"]
            if "impute_zero" in self.r_methods and not self.outer_join:
                inner_prod_mat = mat_dict["inner_product"]
        else:
            # use numpy mask array to ignore NaN values in the calculation
            df1_arr, df2_arr = df1.to_numpy(), df2.to_numpy()
            mat1 = np.ma.array(df1_arr, mask=np.isnan(df1_arr))
            mat2 = np.ma.array(df2_arr, mask=np.isnan(df2_arr))
            corr_mat, pval_mat = corr_utils.mat_corr(
                mat1, mat2, names1, names2, masked=True
            )
        # print(corr_mat)
        if self.correct_method == "FDR":
            # for fdr, we need all correlations regardless of
            # whether the corr coefficent exceeds the threhold or not.
            rows, cols = np.where(corr_mat >= -1)
        else:
            rows, cols = np.where(np.absolute(corr_mat) >= r_threshold)
        index_pairs = [
            (corr_mat.index[row], corr_mat.columns[col]) for row, col in zip(rows, cols)
        ]
        for ix_pair in index_pairs:
            row, col = ix_pair[0], ix_pair[1]

            overlap = len(df1.index)  # number of samples that make up the correlation
            r_val = corr_mat.loc[row][col]
            if r_val < 0:
                print(r_val)
            p_val = pval_mat.loc[row][col]
            if "impute_avg" in self.r_methods and not self.outer_join:
                res_sum_val = res_sum_mat.loc[row][col]
            if "impute_zero" in self.r_methods and not self.outer_join:
                inner_prod_val = inner_prod_mat.loc[row][col]
            if self.correct_method == "FDR":
                if abs(r_val) >= r_threshold and p_val <= p_threshold:
                    self.perf_profile["corr_counts"]["before"] += 1
                # for fdr correction, we need to include all correlations regardless of the p value
                agg_col1 = AggColumn(
                    tbl1, self.tbl_attrs[tbl1]["name"], st_schema1, row, df1[row]
                )
                agg_col2 = AggColumn(
                    tbl2, self.tbl_attrs[tbl2]["name"], st_schema2, col, df2[col]
                )
                new_corr = Correlation(agg_col1, agg_col2, r_val, p_val, overlap, flag)
                if "impute_avg" in self.r_methods and not self.outer_join:
                    new_corr.set_impute_avg_r(res_sum_val)
                if "impute_zero" in self.r_methods and not self.outer_join:
                    new_corr.set_impute_zero_r(mat1.shape[0], inner_prod_val)
                if "impute_avg" in self.r_methods and self.outer_join:
                    new_corr.r_val_impute_avg = corr_impute_avg.loc[row][col]
                if "impute_zero" in self.r_methods and self.outer_join:
                    new_corr.r_val_impute_zero = corr_impute_zero.loc[row][col]
                res.append(new_corr)
            else:
                agg_col1 = AggColumn(
                    tbl1, self.tbl_attrs[tbl1]["name"], st_schema1, row, df1[row]
                )
                agg_col2 = AggColumn(
                    tbl2, self.tbl_attrs[tbl2]["name"], st_schema2, col, df2[col]
                )
                if p_val <= p_threshold:
                    agg_col1.set_profile(df1[row], self.column_profiles[tbl1])
                    agg_col2.set_profile(df2[col], self.column_profiles[tbl2])
                    res.append(
                        Correlation(agg_col1, agg_col2, r_val, p_val, overlap, flag)
                    )
        return res

    def get_corr_naive(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        df1_outer: pd.DataFrame,
        df2_outer: pd.DataFrame,
        tbl1,
        st_schema1,
        tbl2,
        st_schema2,
        r_threshold,
        p_threshold,
        fill_zero,
        flag,
    ):
        res = []
        df1, df2 = df1.fillna(0), df2.fillna(0)
        merged = pd.concat([df1, df2], axis=1)
        overlap = len(merged)
        for col_name1 in df1.columns:
            for col_name2 in df2.columns:
                col1, col2 = merged[col_name1], merged[col_name2]
                col1_array, col2_array = col1.to_numpy(), col2.to_numpy()
                if np.all(np.isclose(col1_array, col1_array[0])) or np.all(np.isclose(col2_array, col2_array[0])):
                    continue
                corr, p_val = pearsonr(col1.to_numpy(), col2.to_numpy())
                # df = merged[[agg_col1, agg_col2]].astype(float)
                # corr_matrix = df.corr(method="pearson", numeric_only=True)
                # corr = corr_matrix.iloc[1, 0]
                if abs(corr) >= r_threshold:
                    # print(tbl1, attrs1, tbl2, attrs2, corr)
                    agg_col1 = AggColumn(
                        tbl1, self.tbl_attrs[tbl1]["name"], st_schema1, col_name1, col1
                    )
                    agg_col2 = AggColumn(
                        tbl2, self.tbl_attrs[tbl2]["name"], st_schema2, col_name2, col2
                    )
                    new_corr = Correlation(agg_col1, agg_col2, corr, p_val, overlap, flag)
                    res.append(new_corr)
        return res


if __name__ == "__main__":
    granu_lists = [[T_GRANU.DAY, S_GRANU.BLOCK]]
    conn_str = "postgresql://yuegong@localhost/st_tables"
    data_source = "chicago_10k"
    config = io_utils.load_config(data_source)
    for granu_list in granu_lists:
        dir_path = "result/chicago_10k/day_block/"
        corr_search = CorrSearch(
            conn_str,
            data_source,
            FIND_JOIN_METHOD.INDEX_SEARCH,
            "AGG",
            "MATRIX",
            ["impute_avg", "impute_zero"],
            False,
            "FDR",
            0.05,
        )
        start = time.time()
        corr_search.find_all_corr_for_all_tbls(
            granu_list, o_t=10, r_t=0.6, p_t=0.05, fill_zero=True, dir_path=dir_path
        )

        total_time = time.time() - start
        print("total time:", total_time)
        corr_search.perf_profile["total_time"] = total_time
        print(corr_search.perf_profile)
