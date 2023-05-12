from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import utils.io_utils
from config import ATTR_PATH, PROFILE_PATH
import numpy as np
import pandas as pd
from data_search.data_model import Unit, Variable, AggFunc
from tqdm import tqdm
import time
import pandas as pd
from data_search.search_db import DBSearch
import os
from utils import corr_utils
from collections import defaultdict
from dataclasses import dataclass
from typing import List

column_profiles = utils.io_utils.load_json(PROFILE_PATH)
tbl_attrs = utils.io_utils.load_json(ATTR_PATH)


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
    def __init__(self, tbl_id, st_schema, agg_attr, col_data=None) -> None:
        self.tbl_id = tbl_id
        self.tbl_name = tbl_attrs[tbl_id]["name"]
        self.st_schema = tuple(st_schema)
        self.agg_attr = agg_attr
        self.col_data = col_data

    def set_profile(self, col_data):
        missing_ratio = col_data.isnull().sum() / len(col_data)
        zero_ratio = (col_data == 0).sum() / len(col_data)
        # if the agg attr is using avg, calculate original missing and zero ratio
        missing_ratio_o, zero_ratio_o = 0, 0
        if self.agg_attr[0:3] == "avg":
            missing_ratio_o = column_profiles[self.tbl_id][self.agg_attr][
                "missing_ratio"
            ]
            zero_ratio_o = column_profiles[self.tbl_id][self.agg_attr]["zero_ratio"]

        cv = col_data.dropna().std() / col_data.dropna().mean()
        self.profile = AggColumnProfile(
            missing_ratio=missing_ratio,
            zero_ratio=zero_ratio,
            missing_ratio_o=missing_ratio_o,
            zero_ratio_o=zero_ratio_o,
            cv=cv,
        )

    def get_id(self):
        return (self.tbl_id, self.st_schema, self.agg_attr)

    def to_list(self):
        return [
            self.tbl_id,
            self.tbl_name,
            self.st_schema,
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
        self.p_val = p_val
        self.overlap = overlap
        self.align_type = align_type

    def to_list(self):
        return (
            self.agg_col1.to_list()
            + self.agg_col2.to_list()
            + [
                round(self.r_val, 3),
                round(self.p_val, 3),
                self.overlap,
                self.align_type,
            ]
        )


class CorrSearch:
    def __init__(
        self, dbSearch: DBSearch, join_method, corr_method, correct_method, q_val=None
    ) -> None:
        # database search client
        self.db_search = dbSearch
        self.data = []
        self.count = 0
        self.visited = set()
        self.join_method = join_method
        self.corr_method = corr_method
        self.correct_method = correct_method
        self.q_val = q_val
        self.perf_profile = {
            "num_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_find_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_join": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_correlation": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_dump_csv": {"total": 0},
            "time_create_tmp_tables": {"total": 0},
            "corr_count": {"total": 0},
        }

    def create_tmp_agg_tbls(self, granu_list):
        for tbl in tqdm(tbl_attrs.keys()):
            print(tbl)
            st_schema = []
            t_attrs, s_attrs = (
                tbl_attrs[tbl]["t_attrs"],
                tbl_attrs[tbl]["s_attrs"],
            )
            for t in t_attrs:
                st_schema.append([Unit(t, granu_list[0])])

            for s in s_attrs:
                st_schema.append([Unit(s, granu_list[1])])

            for t in t_attrs:
                for s in s_attrs:
                    st_schema.append([Unit(t, granu_list[0]), Unit(s, granu_list[1])])

            for units in st_schema:
                tbl1_agg_cols = tbl_attrs[tbl]["num_columns"]
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
        self, granu_list, r_t, p_t, fill_zero=False, dir_path=""
    ):
        if self.join_method == "TMP":
            start = time.time()
            self.create_tmp_agg_tbls(granu_list)
            self.perf_profile["time_create_tmp_tables"]["total"] = time.time() - start

        for tbl in tqdm(tbl_attrs.keys()):
            print(tbl)
            self.find_all_corr_for_a_tbl(tbl, granu_list, r_t, p_t, fill_zero)

            start = time.time()

            self.dump_corrs_to_csv(self.data, dir_path, tbl)
            # after a table is done, clear the data
            self.perf_profile["corr_count"]["total"] += len(self.data)
            self.data.clear()
            time_used = time.time() - start
            self.perf_profile["time_dump_csv"]["total"] += time_used

    def find_all_corr_for_a_tbl(self, tbl, granu_list, r_t, p_t, fill_zero):
        st_schema = []
        t_attrs, s_attrs = (
            tbl_attrs[tbl]["t_attrs"],
            tbl_attrs[tbl]["s_attrs"],
        )
        print(t_attrs, s_attrs)
        for t in t_attrs:
            st_schema.append([Unit(t, granu_list[0])])

        for s in s_attrs:
            st_schema.append([Unit(s, granu_list[1])])

        for t in t_attrs:
            for s in s_attrs:
                st_schema.append([Unit(t, granu_list[0]), Unit(s, granu_list[1])])

        for units in st_schema:
            self.find_all_corr_for_a_tbl_schema(tbl, units, r_t, p_t, fill_zero)

    def find_corr_between_two_schemas(self, tbl1, units1, tbl2, units2):
        tbl1_agg_cols = tbl_attrs[tbl1]["num_columns"]
        tbl2_agg_cols = tbl_attrs[tbl2]["num_columns"]

        vars1 = []
        vars2 = []
        for agg_col in tbl1_agg_cols:
            vars1.append(Variable(agg_col, AggFunc.AVG, "avg_{}_t1".format(agg_col)))
        vars1.append(Variable("*", AggFunc.COUNT, "count_t1"))
        for agg_col in tbl2_agg_cols:
            vars2.append(Variable(agg_col, AggFunc.AVG, "avg_{}_t2".format(agg_col)))
        vars2.append(Variable("*", AggFunc.COUNT, "count_t2"))

        # column names in postgres are at most 63-character long
        names1 = [var.var_name[:63] for var in vars1]
        names2 = [var.var_name[:63] for var in vars2]

        merged = self.db_search.aggregate_join_two_tables(
            tbl1, units1, vars1, tbl2, units2, vars2
        ).fillna(0)
        df1, df2 = merged[names1].astype(float), merged[names2].astype(float)
        return df1, df2

    def find_all_corr_for_a_tbl_schema(self, tbl1, units1, r_t, p_t, fill_zero):
        attrs1 = [unit.attr_name for unit in units1]
        if len(units1) == 2:
            flag = "st"
        elif units1[0].granu in T_GRANU:
            flag = "temporal"
        else:
            flag = "spatial"

        start = time.time()
        # find aligned tbls whose overlap score > 4
        aligned_tbls = self.db_search.find_augmentable_tables(
            tbl1, units1, 4, mode="agg_idx"
        )

        # find the number of test times to correct for multiple comparison problem
        # the number of tests equals the sum of numerical column numbers of aligned tables + 1
        # plus 1 is because we calculate count for each table
        test_num = 0
        for tbl_info in aligned_tbls:
            tbl2, units2 = (
                tbl_info[0],
                tbl_info[2],
            )
            attrs2 = [unit.attr_name for unit in units2]
            if (tbl1, tuple(attrs1), tbl2, tuple(attrs2)) not in self.visited:
                test_num += len(tbl_attrs[tbl_info[0]]["num_columns"]) + 1
        print("test_num", test_num)

        if self.correct_method == "Bonferroni":
            if test_num != 0:
                p_t = p_t / test_num
                print("p value", p_t)

        time_used = time.time() - start
        self.perf_profile["num_joins"]["total"] += len(aligned_tbls)
        self.perf_profile["num_joins"][flag] += len(aligned_tbls)
        self.perf_profile["time_find_joins"]["total"] += time_used
        self.perf_profile["time_find_joins"][flag] += time_used

        tbl_schema_corrs = []

        for tbl_info in aligned_tbls:
            tbl2, units2 = (
                tbl_info[0],
                tbl_info[2],
            )
            attrs2 = [unit.attr_name for unit in units2]

            if (tbl1, tuple(attrs1), tbl2, tuple(attrs2)) in self.visited:
                continue
            else:
                self.visited.add((tbl1, tuple(attrs1), tbl2, tuple(attrs2)))
                self.visited.add((tbl2, tuple(attrs2), tbl1, tuple(attrs1)))

            # calculate agg avg
            tbl1_agg_cols = tbl_attrs[tbl1]["num_columns"]
            tbl2_agg_cols = tbl_attrs[tbl2]["num_columns"]

            vars1 = []
            vars2 = []
            for agg_col in tbl1_agg_cols:
                vars1.append(
                    Variable(agg_col, AggFunc.AVG, "avg_{}_t1".format(agg_col))
                )
            vars1.append(Variable("*", AggFunc.COUNT, "count_t1"))
            for agg_col in tbl2_agg_cols:
                vars2.append(
                    Variable(agg_col, AggFunc.AVG, "avg_{}_t2".format(agg_col))
                )
            vars2.append(Variable("*", AggFunc.COUNT, "count_t2"))

            # column names in postgres are at most 63-character long
            names1 = [var.var_name[:63] for var in vars1]
            names2 = [var.var_name[:63] for var in vars2]

            start = time.time()
            merged = None

            if self.join_method == "TMP":
                merged = self.db_search.aggregate_join_two_tables_using_tmp(
                    tbl1, units1, vars1, tbl2, units2, vars2
                )

            if self.join_method == "ALIGN":
                # begin align tables
                merged = self.db_search.align_two_two_tables(
                    tbl1, units1, vars1, tbl2, units2, vars2
                )

            if self.join_method == "JOIN":
                # begin joining tables
                merged = self.db_search.aggregate_join_two_tables(
                    tbl1, units1, vars1, tbl2, units2, vars2
                )

            if merged is None:
                continue
            df1, df2 = merged[names1].astype(float), merged[names2].astype(float)

            time_used = time.time() - start
            self.perf_profile["time_join"]["total"] += time_used
            self.perf_profile["time_join"][flag] += time_used

            # begin calculating correlation
            start = time.time()
            res = []
            if self.corr_method == "MATRIX":
                res = self.get_corr_opt(
                    df1, df2, tbl1, attrs1, tbl2, attrs2, r_t, p_t, fill_zero, flag
                )

            if self.corr_method == "FOR_PAIR":
                res = self.get_corr_naive(
                    merged, tbl1, attrs1, vars1, tbl2, attrs2, vars2, r_t, p_t
                )

            tbl_schema_corrs.extend(res)

        if self.correct_method == "FDR":
            tbl_schema_corrs = self.bh_correction(tbl_schema_corrs, r_t)

        self.data.extend(tbl_schema_corrs)

        time_used = time.time() - start
        self.perf_profile["time_correlation"]["total"] += time_used
        self.perf_profile["time_correlation"][flag] += time_used

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
            print("n", n)
            largest_i = -1
            for i, corr in enumerate(corr_group):
                bh_value = ((i + 1) / n) * self.q_val
                if corr.p_val < bh_value:
                    largest_i = i
            corrected_corr_group = []
            if largest_i >= 0:
                # print("largest i", largest_i)
                for corr in corr_group[0 : largest_i + 1]:
                    if corr.r_val >= r_t:
                        corr.agg_col1.set_profile(corr.agg_col1.col_data)
                        corr.agg_col2.set_profile(corr.agg_col2.col_data)
                        corrected_corr_group.append(corr)
            filtered_corrs.extend(corrected_corr_group)
        return filtered_corrs

    def get_corr_opt(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        tbl1,
        attrs1,
        tbl2,
        attrs2,
        r_threshold,
        p_threshold,
        fill_zero,
        flag,
    ):
        res = []
        names1, names2 = df1.columns, df2.columns
        if fill_zero:
            mat1, mat2 = df1.fillna(0).to_numpy(), df2.fillna(0).to_numpy()
            corr_mat, pval_mat = corr_utils.mat_corr(
                mat1, mat2, names1, names2, masked=False
            )
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

            if self.correct_method == "FDR":
                # for fdr correction, we need to include all correlations regardless of the p value
                agg_col1 = AggColumn(tbl1, attrs1, row, df1[row])
                agg_col2 = AggColumn(tbl2, attrs2, col, df2[col])
                res.append(Correlation(agg_col1, agg_col2, r_val, p_val, overlap, flag))
            else:
                agg_col1 = AggColumn(tbl1, attrs1, row)
                agg_col2 = AggColumn(tbl2, attrs2, col)
                if p_val <= p_threshold:
                    agg_col1.set_profile(df1[row])
                    agg_col2.set_profile(df2[col])
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
