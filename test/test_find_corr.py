import time
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import pandas as pd
from data_search.search_corr import CorrSearch
from data_search.search_db import DBSearch
from utils.io_utils import dump_json
from data_search.data_model import Unit, Variable, AggFunc

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


def test_find_corr_for_a_tbl_schema():
    conn_str = "postgresql://yuegong@localhost/st_tables"
    db_search = DBSearch(conn_str)
    tbl1 = "i86k-y6er"
    units1 = [Unit("location", S_GRANU.BLOCK)]
    corr_search = CorrSearch(db_search, "ALIGN", "MATRIX")
    corr_search.find_all_corr_for_a_tbl_schema(tbl1, units1, threshold=0.6)
    data1 = corr_search.data

    corr_search = CorrSearch(db_search, "ALIGN", "FOR_PAIR")
    corr_search.find_all_corr_for_a_tbl_schema(tbl1, units1, threshold=0.6)
    data2 = corr_search.data

    data1_set = set(map(tuple, data1))
    data2_set = set(map(tuple, data2))

    print(data1_set == data2_set)

    # tbl2 = "mqwh-r23c"
    # units2 = [Unit("location", S_GRANU.BLOCK)]


def test_find_corr_for_all_tbl():
    granu_lists = [[T_GRANU.DAY, S_GRANU.BLOCK]]

    for granu_list in granu_lists:
        corr_search = CorrSearch(db_search, "TMP", "MATRIX")
        start = time.time()
        corr_search.find_all_corr_for_all_tbls(granu_list, threshold=0.6)

        total_time = time.time() - start
        print("total time:", total_time)
        corr_search.perf_profile["total_time"] = total_time
        print(corr_search.perf_profile)
        dump_json(
            "result/run_time/perf_time_{}_{}_single_idx_align2.json".format(
                granu_list[0], granu_list[1]
            ),
            corr_search.perf_profile,
        )


test_find_corr_for_all_tbl()
