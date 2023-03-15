from st_api import API
import time
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import pandas as pd
from data_search.search_corr import CorrSearch
from data_search.search_db import DBSearch
from utils.io_utils import dump_json

db_search = DBSearch("postgresql://yuegong@localhost/st_tables")


def test_find_corr_for_a_single_tbl():
    start = time.time()
    tbl1 = "yhhz-zm2v"
    granu_list = [T_GRANU.MONTH, S_GRANU.TRACT]
    corr_search = CorrSearch(db_search)
    corr_search.find_all_corr_for_a_tbl(tbl1, granu_list)
    df = pd.DataFrame(
        corr_search.data,
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
    df.to_csv("corr_test2.csv")
    print("total time:", time.time() - start)


def test_find_corr_for_all_tbl():
    granu_lists = [[T_GRANU.MONTH, S_GRANU.TRACT]]

    for granu_list in granu_lists:
        corr_search = CorrSearch(db_search)
        start = time.time()
        corr_search.find_all_corr_for_all_tbls(granu_list)

        total_time = time.time() - start
        print("total time:", total_time)
        corr_search.perf_profile["total_time"] = total_time
        print(corr_search.perf_profile)
        dump_json(
            "result/run_time/perf_time_{}_{}_single_idx_join.json".format(
                granu_list[0], granu_list[1]
            ),
            corr_search.perf_profile,
        )


test_find_corr_for_all_tbl()
