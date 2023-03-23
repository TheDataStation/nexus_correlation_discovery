from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import utils.io_utils
from config import ATTR_PATH
import numpy as np
import pandas as pd
from data_search.data_model import Unit, Variable, AggFunc
from tqdm import tqdm
import traceback
import time
import pandas as pd
from data_search.search_db import DBSearch
import os


class CorrSearch:
    def __init__(self, dbSearch: DBSearch, join_method, corr_method) -> None:
        # database search client
        self.db_search = dbSearch
        self.data = []
        # {tbl_id -> {tbl_name, t_attrs, s_attrs}}
        self.tbl_attrs = self.load_meta_data()
        self.visited = set()
        self.join_method = join_method
        self.corr_method = corr_method
        self.perf_profile = {
            "num_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_find_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_join": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_correlation": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_dump_csv": {"total": 0},
            "time_create_tmp_tables": {"total": 0},
        }

    def load_meta_data(self):
        # info about t_attrs and s_attrs in a table
        tbl_attrs = utils.io_utils.load_json(ATTR_PATH)
        return tbl_attrs

    def create_tmp_agg_tbls(self, granu_list):
        for tbl in tqdm(self.tbl_attrs.keys()):
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

    def find_all_corr_for_all_tbls(self, granu_list, threshold):
        if self.join_method == "TMP":
            start = time.time()
            self.create_tmp_agg_tbls(granu_list)
            self.perf_profile["time_create_tmp_tables"]["total"] = time.time() - start

        for tbl in tqdm(self.tbl_attrs.keys()):
            print(tbl)
            self.find_all_corr_for_a_tbl(tbl, granu_list, threshold)

            start = time.time()
            df = pd.DataFrame(
                self.data,
                columns=[
                    "tbl_id1",
                    "tbl_name1",
                    "align_attrs1",
                    "agg_attr1",
                    "tbl_id2",
                    "tbl_name2",
                    "align_attrs2",
                    "agg_attr2",
                    "corr",
                ],
            )
            dir_path = "/Users/yuegong/Documents/spatio_temporal_alignment/result/corr_{}_{}/".format(
                granu_list[0], granu_list[1]
            )

            if not os.path.exists(dir_path):
                # create the directory if it does not exist
                os.makedirs(dir_path)

            df.to_csv("{}/corr_{}.csv".format(dir_path, tbl))
            # after a table is done, clear the data
            self.data.clear()
            time_used = time.time() - start
            self.perf_profile["time_dump_csv"]["total"] += time_used

    def find_all_corr_for_a_tbl(self, tbl, granu_list, threshold):
        st_schema = []
        t_attrs, s_attrs = (
            self.tbl_attrs[tbl]["t_attrs"],
            self.tbl_attrs[tbl]["s_attrs"],
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
            self.find_all_corr_for_a_tbl_schema(tbl, units, threshold)

    def find_all_corr_for_a_tbl_schema(self, tbl1, units1, threshold):
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
        time_used = time.time() - start
        self.perf_profile["num_joins"]["total"] += len(aligned_tbls)
        self.perf_profile["num_joins"][flag] += len(aligned_tbls)
        self.perf_profile["time_find_joins"]["total"] += time_used
        self.perf_profile["time_find_joins"][flag] += time_used

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
            tbl1_agg_cols = self.tbl_attrs[tbl1]["num_columns"]
            tbl2_agg_cols = self.tbl_attrs[tbl2]["num_columns"]

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
                ).fillna(0)

            if self.join_method == "ALIGN":
                # begin align tables
                merged = self.db_search.align_two_two_tables(
                    tbl1, units1, vars1, tbl2, units2, vars2
                ).fillna(0)

            if self.join_method == "JOIN":
                # begin joining tables
                merged = self.db_search.aggregate_join_two_tables(
                    tbl1, units1, vars1, tbl2, units2, vars2
                ).fillna(0)

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
                res = self.get_corr_opt(df1, df2, tbl1, attrs1, tbl2, attrs2, threshold)

            if self.corr_method == "FOR_PAIR":
                res = self.get_corr_naive(
                    merged, tbl1, attrs1, vars1, tbl2, attrs2, vars2, threshold
                )

            self.data.extend(res)

            time_used = time.time() - start
            self.perf_profile["time_correlation"]["total"] += time_used
            self.perf_profile["time_correlation"][flag] += time_used

    def get_corr_opt(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        tbl1,
        attrs1,
        tbl2,
        attrs2,
        threshold,
    ):
        res = []
        corr_mat = self.get_corr_matrix(df1, df2)
        rows, cols = np.where(corr_mat > threshold)
        index_pairs = [
            (corr_mat.index[row], corr_mat.columns[col]) for row, col in zip(rows, cols)
        ]
        for ix_pair in index_pairs:
            row, col = ix_pair[0], ix_pair[1]
            corr_val = corr_mat.loc[row][col]
            # print(tbl1, attrs1, tbl2, attrs2, corr_val)
            res.append(
                self.new_result(
                    tbl1, tbl2, attrs1, attrs2, row, col, round(corr_val, 2)
                )
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

    def get_corr_matrix(self, df1: pd.DataFrame, df2: pd.DataFrame):
        names1, names2 = df1.columns, df2.columns
        mat1, mat2 = df1.fillna(0).to_numpy(), df2.fillna(0).to_numpy()

        # Subtract column means
        res1, res2 = mat1 - np.mean(mat1, axis=0), mat2 - np.mean(mat2, axis=0)

        # Sum squares across columns
        sums1 = (res1**2).sum(axis=0)
        sums2 = (res2**2).sum(axis=0)

        # Compute correlations
        res_products = np.dot(res1.T, res2)
        sum_products = np.sqrt(np.dot(sums1[:, None], sums2[None]))

        # Account for cases when stardard deviation is 0
        sum_zeros = sum_products == 0
        sum_products[sum_zeros] = 1

        corrs = res_products / sum_products

        corrs[sum_zeros] = 0

        # Store correlations in DataFrames
        corrs = pd.DataFrame(corrs, index=names1, columns=names2)
        return corrs

    def new_result(self, tbl1, tbl2, st1, st2, agg_attr1, agg_attr2, corr):
        tbl_name1, tbl_name2 = (
            self.tbl_attrs[tbl1]["name"],
            self.tbl_attrs[tbl2]["name"],
        )
        # print(
        #     tbl1,
        #     tbl_name1,
        #     st1,
        #     agg_attr1,
        #     tbl2,
        #     tbl_name2,
        #     st2,
        #     agg_attr2,
        #     corr,
        # )
        return [
            tbl1,
            tbl_name1,
            tuple(st1),
            agg_attr1,
            tbl2,
            tbl_name2,
            tuple(st2),
            agg_attr2,
            round(corr, 2),
        ]
