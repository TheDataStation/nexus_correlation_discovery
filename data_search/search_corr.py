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
        return (self.tbl_id, tuple(self.st_schema.get_attrs()), self.agg_attr)

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

    def load_inv_cnt_tbl(t_scale, s_scale):
        pass

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

        self.find_join_method = find_join_method
        self.join_method = join_method
        self.corr_method = corr_method
        self.r_methods = r_methods
        self.outer_join = explicit_outer_join
        self.correct_method = correct_method
        self.q_val = q_val
        self.inv_overhead = 0
        self.perf_profile = {
            "num_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_find_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_join": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_correlation": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_correction": {"total": 0},
            "time_dump_csv": {"total": 0},
            "time_create_tmp_tables": {"total": 0},
            "corr_count": {"total": 0},
            "strategy": {"find_join": 0, "join_all": 0, "skip": 0},
        }

    """
    Online aggregation
    """

    def create_tmp_agg_tbls(self, granu_list):
        for tbl in tqdm(self.tbl_attrs.keys()):
            print(tbl)
            st_schema = []
            t_attrs, s_attrs = (
                self.tbl_attrs[tbl]["t_attrs"],
                self.tbl_attrs[tbl]["s_attrs"],
            )
            for t in t_attrs:
                st_schema.append([Unit(t, granu_list[0])])

            for s in s_attrs:
                st_schema.append([Unit(s, granu_list[1])])

            for t in t_attrs:
                for s in s_attrs:
                    st_schema.append([Unit(t, granu_list[0]), Unit(s, granu_list[1])])

            for units in st_schema:
                tbl1_agg_cols = self.tbl_attrs[tbl]["num_columns"]
                vars = []
                for agg_col in tbl1_agg_cols:
                    vars.append(
                        Variable(agg_col, AggFunc.AVG, "avg_{}".format(agg_col))
                    )
                vars.append(Variable("*", AggFunc.COUNT, "count"))
                self.db_search.create_tmp_agg_tbl(tbl, units, vars)

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
        if self.join_method == "TMP":
            start = time.time()
            self.create_tmp_agg_tbls(granu_list)
            self.perf_profile["time_create_tmp_tables"]["total"] = time.time() - start

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
            self.find_all_corr_for_a_tbl_schema(
                tbl, st_schema, o_t, r_t, p_t, fill_zero
            )

    def align_two_st_schemas(self, tbl1, st_schema1, tbl2, st_schema2, o_t, outer):
        tbl1_agg_cols = self.tbl_attrs[tbl1]["num_columns"]
        tbl2_agg_cols = self.tbl_attrs[tbl2]["num_columns"]

        vars1 = []
        vars2 = []
        for agg_col in tbl1_agg_cols:
            vars1.append(Variable(agg_col, AggFunc.AVG, "avg_{}_t1".format(agg_col)))
        if len(tbl1_agg_cols) == 0:
            vars1.append(Variable("*", AggFunc.COUNT, "count_t1"))
        for agg_col in tbl2_agg_cols:
            vars2.append(Variable(agg_col, AggFunc.AVG, "avg_{}_t2".format(agg_col)))
        if len(tbl2_agg_cols) == 0:
            vars2.append(Variable("*", AggFunc.COUNT, "count_t2"))

        # column names in postgres are at most 63-character long
        names1 = [var.var_name[:63] for var in vars1]
        names2 = [var.var_name[:63] for var in vars2]

        merged = None

        if self.join_method == "AGG":
            merged = db_ops.join_two_agg_tables(
                self.cur, tbl1, st_schema1, vars1, tbl2, st_schema2, vars2, outer=False
            )
            if self.outer_join:
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
        elif self.join_method == "TMP":
            merged = self.db_search.aggregate_join_two_tables_using_tmp(
                tbl1, st_schema1, vars1, tbl2, st_schema2, vars2
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
            return None, None

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
        # get find join overhead

        row_to_read, max_joinable = db_ops.get_inv_cnt(
            self.cur, tbl, st_schema, threshold
        )
        median = 6447
        v_cnt = min(v_cnt, median)
        print("row_cnt", v_cnt)
        find_join_cost = row_to_read + max_joinable * v_cnt
        # get schema count for join all
        aligned_schemas = []
        aligned_tbls = set(self.all_tbls).difference(self.visited_tbls)
        total_schema_num = 0
        for tbl2 in aligned_tbls:
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
            aligned_schemas.extend(
                [(tbl2, st_schema2) for st_schema2 in st_schema_list]
            )
        # print("num to join", len(aligned_schemas))
        join_all_cost = len(aligned_schemas) * v_cnt
        print(f"join all cost: {join_all_cost}; find join cost: {find_join_cost}")
        if find_join_cost <= join_all_cost:
            return "FIND_JOIN", None
        elif row_to_read >= join_all_cost:
            return "JOIN_ALL", aligned_schemas
        else:
            candidates, total_elements_sampled = db_ops.get_intersection_inv_idx(
                self.cur, tbl, st_schema, threshold, 20
            )

            scale_factor = row_to_read // total_elements_sampled
            joinable_estimate = 0
            for _, cnt in candidates:
                if cnt * scale_factor >= threshold:
                    joinable_estimate += 1
            print(f"joinable_estimate: {joinable_estimate}")
            if row_to_read + joinable_estimate * v_cnt <= join_all_cost:
                return "FIND_JOIN", None
            else:
                return "JOIN_ALL", aligned_schemas
            # print(f"row_to_read: {row_to_read}")
            # magnitude_num1 = math.floor(math.log10(abs(row_to_read)))
            # magnitude_num2 = math.floor(math.log10(abs(join_all_cost)))
            # if magnitude_num1 < magnitude_num2:
            #     return "FIND_JOIN", None
            # else:
            #     return "JOIN_ALL", aligned_schemas

    def find_all_corr_for_a_tbl_schema(
        self, tbl1, st_schema: ST_Schema, o_t, r_t, p_t, fill_zero
    ):
        flag = st_schema.get_type().value
        """
        Find aligned schemas whose overlap with the input st_schema is greater then o_t
        """
        start = time.time()
        self.visited_tbls.add(tbl1)
        v_cnt = db_ops.get_val_cnt(self.cur, tbl1, st_schema)
        print("v_cnt", v_cnt)
        if v_cnt < o_t:
            print("skip because this table does not have enough keys")
            self.perf_profile["strategy"]["skip"] += 1
            return

        if self.find_join_method == FIND_JOIN_METHOD.INDEX_SEARCH:
            aligned_schemas = self.db_search.find_augmentable_st_schemas(
                tbl1, st_schema, o_t, mode="inv_idx"
            )
        elif self.find_join_method == FIND_JOIN_METHOD.JOIN_ALL:
            aligned_schemas = []
            aligned_tbls = set(self.all_tbls).difference(self.visited_tbls)
            for tbl2 in aligned_tbls:
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
                aligned_schemas.extend(
                    [(tbl2, st_schema2) for st_schema2 in st_schema_list]
                )
        elif self.find_join_method == FIND_JOIN_METHOD.COST_MODEL:
            s = time.time()
            method, schemas = self.determine_find_join_method(
                tbl1, st_schema, o_t, v_cnt
            )
            self.inv_overhead += time.time() - s
            print(f"choose {method}")
            if method == "FIND_JOIN":
                self.perf_profile["strategy"]["find_join"] += 1
                aligned_schemas = self.db_search.find_augmentable_st_schemas(
                    tbl1, st_schema, o_t, mode="inv_idx"
                )
            elif method == "JOIN_ALL":
                self.perf_profile["strategy"]["join_all"] += 1
                aligned_schemas = schemas

        time_used = time.time() - start

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
            if tbl2 in self.visited_tbls:
                continue

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
                    merged, tbl1, attrs1, vars1, tbl2, attrs2, vars2, r_t, p_t
                )
            if res is not None:
                tbl_schema_corrs.extend(res)
            time_used = time.time() - start
            self.perf_profile["time_correlation"]["total"] += time_used
            self.perf_profile["time_correlation"][flag] += time_used

        """
        Perform multiple-comparison correction
        """
        start = time.time()
        if self.correct_method == "FDR":
            tbl_schema_corrs = self.bh_correction(tbl_schema_corrs, r_t)
        self.perf_profile["time_correction"]["total"] += time.time() - start
        self.data.extend(tbl_schema_corrs)

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
            if "impute_avg" in self.r_methods and not self.outer_join:
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
            rows, cols = np.where(corr_mat > r_threshold)
        index_pairs = [
            (corr_mat.index[row], corr_mat.columns[col]) for row, col in zip(rows, cols)
        ]
        for ix_pair in index_pairs:
            row, col = ix_pair[0], ix_pair[1]

            overlap = len(df1.index)  # number of samples that make up the correlation
            r_val = corr_mat.loc[row][col]
            p_val = pval_mat.loc[row][col]
            if "impute_avg" in self.r_methods and not self.outer_join:
                res_sum_val = res_sum_mat.loc[row][col]
            if "impute_zero" in self.r_methods and not self.outer_join:
                inner_prod_val = inner_prod_mat.loc[row][col]
            if self.correct_method == "FDR":
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
                agg_col1 = AggColumn(tbl1, st_schema1, row)
                agg_col2 = AggColumn(tbl2, st_schema2, col)
                if p_val <= p_threshold:
                    agg_col1.set_profile(df1[row], self.column_profiles[tbl1])
                    agg_col2.set_profile(df2[col], self.column_profiles[tbl2])
                    res.append(
                        Correlation(agg_col1, agg_col2, r_val, p_val, overlap, flag)
                    )

        return res

    def get_corr_naive(
        self, merged: pd.DataFrame, tbl1, attrs1, vars1, tbl2, attrs2, vars2, threshold
    ):
        res = []
        for var1 in vars1:
            for var2 in vars2:
                agg_col1, agg_col2 = var1.var_name, var2.var_name
                df = merged[[agg_col1, agg_col2]].astype(float)
                corr_matrix = df.corr(method="pearson", numeric_only=True)
                corr = corr_matrix.iloc[1, 0]
                if corr > threshold:
                    # print(tbl1, attrs1, tbl2, attrs2, corr)
                    res.append(
                        self.new_result(
                            tbl1,
                            tbl2,
                            attrs1,
                            attrs2,
                            agg_col1,
                            agg_col2,
                            round(corr, 2),
                        )
                    )
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
