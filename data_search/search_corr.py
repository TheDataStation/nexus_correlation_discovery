from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import utils.io_utils
from config import DATA_PATH, ATTR_PATH
import numpy as np
import pandas as pd
from data_search.data_model import AggFunc


class CorrSearch:
    def __init__(self, dbSearch) -> None:
        self.db_search = dbSearch
        self.data = []
        self.tbl_attr_data = utils.io_utils.load_json(ATTR_PATH)
        self.load_meta_data()
        self.visited = set()

    def load_meta_data(self):
        self.tbl_attrs = {}
        for obj in self.tbl_attr_data:
            tbl_id, tbl_name, t_attrs, s_attrs = (
                obj["tbl_id"],
                obj["tbl_name"],
                obj["t_attrs"],
                obj["s_attrs"],
            )
            self.tbl_attrs[tbl_id] = [tbl_name, t_attrs, s_attrs]
        return self.tbl_attrs

    def get_numerical_columns(self, tbl_id):
        df = utils.io_utils.read_csv(DATA_PATH + tbl_id + ".csv")
        return list(df.select_dtypes(include=[np.number]).columns.values)

    def find_all_corr_for_a_tbl(self, tbl):
        st_schema = []
        t_attrs, s_attrs = self.tbl_attrs[tbl][1], self.tbl_attrs[tbl][2]
        print(t_attrs, s_attrs)
        # for t in t_attrs:
        #     st_schema.append(([t], [T_GRANU.MONTH]))
        # for s in s_attrs:
        #     st_schema.append(([s], [S_GRANU.TRACT]))
        for t in t_attrs:
            for s in s_attrs:
                st_schema.append(([t, s], [T_GRANU.MONTH, S_GRANU.TRACT]))

        for attrs, granu_list in st_schema:
            self.find_all_corr_for_a_tbl_schema(tbl, attrs, granu_list)

    def find_all_corr_for_all_tbls(self):
        for tbl in self.tbl_attrs.keys():
            print(tbl)
            self.find_all_corr_for_a_tbl(tbl)
            self.visited.add(tbl)
            print(len(self.data))
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
            df.to_csv(
                "/Users/yuegong/Documents/spatio_temporal_alignment/result/corr/corr_{}.csv".format(
                    tbl
                )
            )
            self.data.clear()

    def find_all_corr_for_a_tbl_schema(self, tbl1, attrs1, granu_list):
        tbl1_agg_cols = self.get_numerical_columns(tbl1)

        aligned_tbls = self.db_search.search(tbl1, attrs1, granu_list)

        for row in aligned_tbls.itertuples():
            tbl2, attrs2, overlap = (
                row[1],
                row[3],
                row[4],
            )
            # if tbl2 in self.visited:
            #     continue
            print(overlap)

            if overlap >= 4:
                print(tbl1, attrs1, tbl2, attrs2)
                # calculate agg count
                merged = self.db_search.aggregate_join_two_tables(
                    tbl1, attrs1, tbl2, attrs2, granu_list
                )
                corr_matrix = merged.corr(method="pearson", numeric_only=True)
                corr = corr_matrix.iloc[1, 0]
                if corr > 0.6:
                    self.append_result(tbl1, tbl2, attrs1, attrs2, None, None, corr)

                # calculate agg avg
                tbl2_agg_cols = self.get_numerical_columns(tbl2)
                for agg_col1 in tbl1_agg_cols:
                    for agg_col2 in tbl2_agg_cols:
                        merged = self.db_search.aggregate_join_two_tables_avg(
                            tbl1, attrs1, agg_col1, tbl2, attrs2, agg_col2, granu_list
                        )
                        if merged is None:
                            continue
                        corr_matrix = merged.corr(method="pearson", numeric_only=True)
                        corr = corr_matrix.iloc[1, 0]
                        if corr > 0.6:
                            self.append_result(
                                tbl1, tbl2, attrs1, attrs2, agg_col1, agg_col2, corr
                            )

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
                corr,
            ]
        )
