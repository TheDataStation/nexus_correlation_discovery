from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import utils.io_utils
from config import DATA_PATH, ATTR_PATH
import numpy as np
import pandas as pd
from data_search.data_model import Unit, Variable, AggFunc
from tqdm import tqdm
import traceback
import time
import pandas as pd


class CorrSearch:
    def __init__(self, dbSearch) -> None:
        # database search client
        self.db_search = dbSearch
        self.data = []
        # {tbl_id -> {tbl_name, t_attrs, s_attrs}}
        self.tbl_attrs = self.load_meta_data()
        self.visited = set()
        self.perf_profile = {
            "num_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_find_joins": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_join": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_correlation": {"total": 0, "temporal": 0, "spatial": 0, "st": 0},
            "time_dump_csv": {"total": 0},
        }

    def load_meta_data(self):
        # info about t_attrs and s_attrs in a table
        tbl_attrs = utils.io_utils.load_json(ATTR_PATH)
        return tbl_attrs

    def find_all_corr_for_all_tbls(self, granu_list):
        for tbl in tqdm(self.tbl_attrs.keys()):
            print(tbl)
            self.find_all_corr_for_a_tbl(tbl, granu_list, threshold=0.6)

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
            df.to_csv(
                "/Users/yuegong/Documents/spatio_temporal_alignment/result/corr_day_block/corr_{}.csv".format(
                    tbl
                )
            )
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
        if len(units1) == 2:
            flag = "st"
        elif units1[0].granu in T_GRANU:
            flag = "temporal"
        else:
            flag = "spatial"

        start = time.time()
        # find aligned tbls whose overlap score > 4
        aligned_tbls = self.db_search.find_augmentable_tables(tbl1, units1, 4)
        time_used = time.time() - start
        self.perf_profile["num_joins"]["total"] += len(aligned_tbls)
        self.perf_profile["num_joins"][flag] += len(aligned_tbls)
        self.perf_profile["time_find_joins"]["total"] += time_used
        self.perf_profile["time_find_joins"][flag] += time_used

        # for tbl_info in aligned_tbls:
        #     tbl2, units2 = (
        #         tbl_info[0],
        #         tbl_info[2],
        #     )
        #     attrs2 = [unit.attr_name for unit in units2]

        #     if (tbl1, tuple(attrs1), tbl2, tuple(attrs2)) in self.visited:
        #         continue
        #     else:
        #         self.visited.add((tbl1, tuple(attrs1), tbl2, tuple(attrs2)))
        #         self.visited.add((tbl2, tuple(attrs2), tbl1, tuple(attrs1)))

        #     # calculate agg avg
        #     tbl1_agg_cols = self.tbl_attrs[tbl1]["num_columns"]
        #     tbl2_agg_cols = self.tbl_attrs[tbl2]["num_columns"]

        #     vars1 = []
        #     vars2 = []
        #     for agg_col in tbl1_agg_cols:
        #         vars1.append(
        #             Variable(agg_col, AggFunc.AVG, "avg_{}_t1".format(agg_col))
        #         )
        #     vars1.append(Variable("*", AggFunc.COUNT, "count1"))
        #     for agg_col in tbl2_agg_cols:
        #         vars2.append(
        #             Variable(agg_col, AggFunc.AVG, "avg_{}_t2".format(agg_col))
        #         )
        #     vars2.append(Variable("*", AggFunc.COUNT, "count2"))

        #     # begin joining tables
        #     start = time.time()
        #     merged = None
        #     try:
        #         merged = self.db_search.aggregate_join_two_tables2(
        #             tbl1, units1, vars1, tbl2, units2, vars2
        #         )
        #     except:
        #         traceback.print_exc()
        #     time_used = time.time() - start
        #     self.perf_profile["time_join"]["total"] += time_used
        #     self.perf_profile["time_join"][flag] += time_used

        #     if merged is None:
        #         continue

        #     # begin calculating correlation
        #     start = time.time()
        #     try:
        #         for var1 in vars1:
        #             for var2 in vars2:
        #                 agg_col1, agg_col2 = var1.var_name, var2.var_name
        #                 df = merged[[agg_col1, agg_col2]].astype(float)
        #                 corr_matrix = df.corr(method="pearson", numeric_only=True)
        #                 corr = corr_matrix.iloc[1, 0]
        #                 if corr > threshold:
        #                     print(tbl1, attrs1, tbl2, attrs2, corr)
        #                     self.append_result(
        #                         tbl1, tbl2, attrs1, attrs2, agg_col1, agg_col2, corr
        #                     )
        #     except:
        #         traceback.print_exc()
        #     time_used = time.time() - start
        #     self.perf_profile["time_correlation"]["total"] += time_used
        #     self.perf_profile["time_correlation"][flag] += time_used

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
