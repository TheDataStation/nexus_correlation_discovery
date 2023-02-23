from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import utils.io_utils
from config import DATA_PATH, ATTR_PATH
import numpy as np
import pandas as pd
from data_search.data_model import Unit, Variable, AggFunc
from tqdm import tqdm
import traceback


class CorrSearch:
    def __init__(self, dbSearch) -> None:
        # database search client
        self.db_search = dbSearch
        self.data = []
        # {tbl_id -> {tbl_name, t_attrs, s_attrs}}
        self.tbl_attrs = self.load_meta_data()
        self.visited = set()

    def load_meta_data(self):
        # info about t_attrs and s_attrs in a table
        tbl_attr_data = utils.io_utils.load_json(ATTR_PATH)
        tbl_attrs = {}
        for obj in tbl_attr_data:
            tbl_id, tbl_name, t_attrs, s_attrs = (
                obj["tbl_id"],
                obj["tbl_name"],
                obj["t_attrs"],
                obj["s_attrs"],
            )
            tbl_attrs[tbl_id] = {
                "name": tbl_name,
                "t_attrs": t_attrs,
                "s_attrs": s_attrs,
            }
        return tbl_attrs

    def get_numerical_columns(self, tbl_id):
        df = utils.io_utils.read_csv(DATA_PATH + tbl_id + ".csv")
        numerical_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        # exclude timestamp columns
        t_attrs = self.tbl_attrs[tbl_id]["t_attrs"]
        for t_attr in t_attrs:
            if t_attr in numerical_columns:
                numerical_columns.remove(t_attr)
        return numerical_columns

    def is_agg_column_valid(self, col_name):
        stop_words = [
            "id",
            "longitude",
            "latitude",
            "ward",
            "date",
            "zipcode",
            "district",
        ]
        for stop_word in stop_words:
            if stop_word in col_name:
                return False
        return True

    def find_all_corr_for_all_tbls(self):
        for tbl in tqdm(self.tbl_attrs.keys()):
            print(tbl)
            granu_list = [T_GRANU.MONTH, S_GRANU.TRACT]
            self.find_all_corr_for_a_tbl(tbl, granu_list, threshold=0.6)

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
                "/Users/yuegong/Documents/spatio_temporal_alignment/result/corr_month_tract/corr_{}.csv".format(
                    tbl
                )
            )
            self.data.clear()

    def find_all_corr_for_a_tbl(self, tbl, granu_list, threshold):
        st_schema = []
        t_attrs, s_attrs = (
            self.tbl_attrs[tbl]["t_attrs"],
            self.tbl_attrs[tbl]["s_attrs"],
        )
        print(t_attrs, s_attrs)
        for t in t_attrs:
            st_schema.append([Unit(t, granu_list[0])])
            # st_schema.append(([t], [granu_list[0]]))
        for s in s_attrs:
            st_schema.append([Unit(s, granu_list[1])])
            # st_schema.append(([s], [granu_list[1]]))
        for t in t_attrs:
            for s in s_attrs:
                st_schema.append([Unit(t, granu_list[0]), Unit(s, granu_list[1])])
                # st_schema.append(([t, s], granu_list))

        for units in st_schema:
            self.find_all_corr_for_a_tbl_schema(tbl, units, threshold)

    def find_all_corr_for_a_tbl_schema(self, tbl1, units1, threshold):
        attrs1 = [unit.attr_name for unit in units1]
        # find aligned tbls whose overlap score > 4
        aligned_tbls = self.db_search.find_augmentable_tables(tbl1, units1, 4)

        for row in aligned_tbls:
            tbl2, units2 = (
                row[0],
                row[2],
            )
            attrs2 = [unit.attr_name for unit in units2]
            if (tbl1, tuple(attrs1), tbl2, tuple(attrs2)) in self.visited:
                continue
            else:
                self.visited.add((tbl1, tuple(attrs1), tbl2, tuple(attrs2)))
                self.visited.add((tbl2, tuple(attrs2), tbl1, tuple(attrs1)))

            # calculate agg avg
            tbl1_agg_cols = self.get_numerical_columns(tbl1)
            tbl2_agg_cols = self.get_numerical_columns(tbl2)

            vars1 = []
            vars2 = []
            for agg_col in tbl1_agg_cols:
                if not self.is_agg_column_valid(agg_col):
                    continue
                vars1.append(
                    Variable(agg_col, AggFunc.AVG, "avg_{}_t1".format(agg_col))
                )
            vars1.append(Variable("*", AggFunc.COUNT, "count1"))
            for agg_col in tbl2_agg_cols:
                if not self.is_agg_column_valid(agg_col):
                    continue
                vars2.append(
                    Variable(agg_col, AggFunc.AVG, "avg_{}_t2".format(agg_col))
                )
            vars2.append(Variable("*", AggFunc.COUNT, "count2"))

            merged = None
            try:
                merged = self.db_search.aggregate_join_two_tables2(
                    tbl1, units1, vars1, tbl2, units2, vars2
                )
            except:
                traceback.print_exc()

            if merged is None:
                continue
            # print(merged.columns)
            try:
                for var1 in vars1:
                    for var2 in vars2:
                        agg_col1, agg_col2 = var1.var_name, var2.var_name
                        df = merged[[agg_col1, agg_col2]].astype(float)
                        corr_matrix = df.corr(method="pearson", numeric_only=True)
                        corr = corr_matrix.iloc[1, 0]
                        if corr > threshold:
                            print(tbl1, attrs1, tbl2, attrs2, corr)
                            self.append_result(
                                tbl1, tbl2, attrs1, attrs2, agg_col1, agg_col2, corr
                            )
            except:
                traceback.print_exc()

    def append_result(self, tbl1, tbl2, st1, st2, agg_attr1, agg_attr2, corr):
        tbl_name1, tbl_name2 = (
            self.tbl_attrs[tbl1]["name"],
            self.tbl_attrs[tbl2]["name"],
        )
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
