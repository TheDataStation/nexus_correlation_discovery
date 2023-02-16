from st_api import API
import time
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import pandas as pd
from data_search.search_corr import CorrSearch
from data_search.search_db import DBSearch

db_search = DBSearch("postgresql://yuegong@localhost/st_tables")
corr_search = CorrSearch(db_search)


def test_find_corr_for_a_single_tbl():
    start = time.time()
    tbl1 = "yhhz-zm2v"
    granu_list = [T_GRANU.MONTH, S_GRANU.TRACT]
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
    start = time.time()

    corr_search.find_all_corr_for_all_tbls()
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
    df.to_csv("corr_all.csv")
    print("total time:", time.time() - start)


test_find_corr_for_all_tbl()
