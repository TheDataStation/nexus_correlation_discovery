import utils.io_utils as io_utils
from config import META_PATH, DATA_PATH
import pandas as pd
from utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
import pandas as pd
from data_search.search_db import DBSearch
from typing import List
import numpy as np


class API:
    def __init__(self, conn_str):
        self.meta_data = io_utils.load_json(META_PATH)
        self.load_meta_data()
        self.db_search = DBSearch(conn_str)
        self.data = []

    def load_meta_data(self):
        self.tbl_lookup = {}
        for obj in self.meta_data:
            domain, tbl_id, tbl_name, t_attrs, s_attrs = (
                obj["domain"],
                obj["tbl_id"],
                obj["tbl_name"],
                obj["t_attrs"],
                obj["s_attrs"],
            )
            self.tbl_lookup[tbl_id] = [tbl_name, t_attrs, s_attrs]
        return self.tbl_lookup

    def get_tbl_name(self, tbl_id):
        return self.tbl_lookup[tbl_id][0]

    def show_all_tables(self):
        tbls = []

        for obj in self.meta_data:
            domain, tbl_id, tbl_name, t_attrs, s_attrs = (
                obj["domain"],
                obj["tbl_id"],
                obj["tbl_name"],
                obj["t_attrs"],
                obj["s_attrs"],
            )
            if t_attrs and s_attrs:
                for t_attr in t_attrs:
                    for s_attr in s_attrs:
                        tbl = [tbl_id, tbl_name, t_attr, s_attr]
                        tbls.append(tbl)
            elif t_attrs:
                for t_attr in t_attrs:
                    tbl = [tbl_id, tbl_name, t_attr, None]
                    tbls.append(tbl)
            else:
                for s_attr in s_attrs:
                    tbl = [tbl_id, tbl_name, None, s_attr]
                    tbls.append(tbl)
        df = pd.DataFrame(tbls, columns=["Table Id", "Table Name", "Time", "Location"])
        return df

    def search_tbl(self, tbl_id: str, attrs: List[str], granu_list):
        df = self.db_search.search(tbl_id, attrs, granu_list)
        return df

    def preview_tbl(self, tbl_id: str):
        df = pd.read_csv(DATA_PATH + tbl_id + ".csv", nrows=50)
        return df

    def calculate_corr(self, tbl1, attrs1, tbl2, attrs2, granu_list):
        merged = self.db_search.aggregate_join_two_tables(
            tbl1, attrs1, tbl2, attrs2, granu_list
        )
        corr_matrix = merged.corr(method="pearson", numeric_only=True)
        return corr_matrix.iloc[1, 0]

    def find_all_corr_for_a_tbl(self, tbl1, t_attrs, s_attrs, granu_list):
        st_schema = []

        # for t in t_attrs:
        #     st_schema.append(([t], [T_GRANU.MONTH]))
        # for s in s_attrs:
        #     st_schema.append(([s], [S_GRANU.TRACT]))
        for t in t_attrs:
            for s in s_attrs:
                st_schema.append(([t, s], [TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT]))

        tbl1_agg_cols = self.get_numerical_columns(tbl1)
        for st1, granu_list in st_schema:
            print(st1, granu_list)
            aligned_tbls = self.db_search.search(tbl1, st1, granu_list)

            for tbl in aligned_tbls.itertuples():
                print(tbl)
                tbl2, st2, overlap = (
                    tbl[1],
                    tbl[3],
                    tbl[4],
                )

                print(overlap)

                if overlap >= 50:
                    print(tbl1, st1, tbl1, st2)
                    # calculate agg count
                    merged = self.db_search.aggregate_join_two_tables(
                        tbl1, st1, tbl2, st2, granu_list
                    )
                    corr_matrix = merged.corr(method="pearson", numeric_only=True)
                    corr = corr_matrix.iloc[1, 0]
                    if corr > 0.6:
                        self.append_result(tbl1, tbl2, st1, st2, None, None, corr)

                    # calculate agg avg
                    tbl2_agg_cols = self.get_numerical_columns(tbl2)
                    for agg_col1 in tbl1_agg_cols:
                        for agg_col2 in tbl2_agg_cols:
                            merged = self.db_search.aggregate_join_two_tables_avg(
                                tbl1, st1, agg_col1, tbl2, st2, agg_col2, granu_list
                            )
                            corr_matrix = merged.corr(
                                method="pearson", numeric_only=True
                            )
                            corr = corr_matrix.iloc[1, 0]
                            if corr > 0.6:
                                self.append_result(
                                    tbl1, tbl2, st1, st2, agg_col1, agg_col2, corr
                                )

    def get_numerical_columns(self, tbl_id):
        df = io_utils.read_csv(DATA_PATH + tbl_id + ".csv")
        return list(df.select_dtypes(include=[np.number]).columns.values)

    def append_result(self, tbl1, tbl2, st1, st2, agg_attr1, agg_attr2, corr):
        tbl_name1, tbl_name2 = (self.tbl_lookup[tbl1][0], self.tbl_lookup[tbl2][0])
        print(
            tbl1,
            tbl_name1,
            st1,
            agg_attr1,
            tbl2,
            tbl_name2,
            st2,
            agg_attr2,
            corr,
        )
        self.data.append(
            [
                tbl1,
                tbl_name1,
                st1,
                agg_attr1,
                tbl2,
                tbl_name2,
                st2,
                agg_attr2,
                round(corr, 2),
            ]
        )

    def find_all_corr(self):
        meta_data = io_utils.load_json(META_PATH)

        data = []
        for obj in meta_data:
            tbl_id, tbl_name, t_attrs, s_attrs = (
                obj["tbl_id"],
                obj["tbl_name"],
                obj["t_attrs"],
                obj["s_attrs"],
            )
            if len(t_attrs) and len(s_attrs):
                for t_attr in t_attrs:
                    for s_attr in s_attrs:
                        aligned_tbls = self.db_search.search(
                            tbl_id, [t_attr, s_attr], [TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT]
                        )
                        for tbl in aligned_tbls:
                            tbl_id2, t_attr2, s_attr2, overlap = (
                                tbl[0],
                                tbl[1],
                                tbl[2],
                                tbl[3],
                            )
                            if overlap >= 50:
                                print(tbl_id, t_attr, s_attr, tbl_id2, t_attr2, s_attr2)
                                corr = self.calculate_corr(
                                    tbl_id,
                                    t_attr,
                                    s_attr,
                                    tbl_id2,
                                    t_attr2,
                                    s_attr2,
                                    TEMPORAL_GRANU.MONTH,
                                    SPATIAL_GRANU.TRACT,
                                )
                                if corr > 0.5:
                                    tbl_name, tbl_name2 = (
                                        self.tbl_lookup[tbl_id][0],
                                        self.tbl_lookup[tbl_id2][0],
                                    )
                                    print(
                                        tbl_id,
                                        tbl_name,
                                        t_attr,
                                        s_attr,
                                        tbl_id2,
                                        tbl_name2,
                                        t_attr2,
                                        s_attr2,
                                        corr,
                                    )
                                    data.append(
                                        [
                                            tbl_id,
                                            tbl_name,
                                            t_attr,
                                            s_attr,
                                            tbl_id2,
                                            tbl_name2,
                                            t_attr2,
                                            s_attr2,
                                            corr,
                                        ]
                                    )

        df = pd.DataFrame(
            data,
            columns=[
                "tbl_id1",
                "tbl_name1",
                "t_attr1",
                "s_attr1",
                "tbl_id2",
                "tbl_name2",
                "t_attr2",
                "s_attr2",
                "corr",
            ],
        )
        df.to_csv("corr.csv")
