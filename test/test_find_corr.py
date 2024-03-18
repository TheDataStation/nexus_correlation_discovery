import time
from nexus.utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from nexus.data_search.search_corr import CorrSearch
from nexus.data_search.search_db import DBSearch
from nexus.utils.io_utils import load_config
from utils.data_model import Attr
import pandas as pd
import os
import utils
from nexus.data_search.commons import FIND_JOIN_METHOD
from collections import defaultdict

db_search = DBSearch("postgresql://yuegong@localhost/st_tables")
# db_search = DBSearch("postgresql://yuegong@localhost/cdc_open_data")
# db_search = DBSearch("postgresql://yuegong@localhost/chicago_open_data_1m")


def test_find_corr_for_a_single_tbl():
    start = time.time()
    tbl = "yhhz-zm2v"
    granu_list = [TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK]
    conn_str = "postgresql://yuegong@localhost/st_tables"
    data_source = "chicago_10k"
    corr_search = CorrSearch(
        conn_str,
        data_source,
        "FIND_JOIN",
        "AGG",
        "MATRIX",
        ["impute_avg", "impute_zero"],
        True,
        "FDR",
        0.05,
    )
    corr_search.find_all_corr_for_a_tbl(tbl, granu_list, 10, 0.6, 0.05, True)
    print(len(corr_search.data))
    corr_search.dump_corrs_to_csv(corr_search.data, f"result/", tbl)
    print("total time:", time.time() - start)


def test_find_corr_for_a_tbl_schema():
    conn_str = "postgresql://yuegong@localhost/st_tables"
    db_search = DBSearch(conn_str)
    tbl1 = "i86k-y6er"
    units1 = [Attr("location", SPATIAL_GRANU.BLOCK)]
    data_source = "chicago_10k"
    corr_search = CorrSearch(conn_str, data_source, "TMP", "MATRIX")
    corr_search.create_tmp_agg_tbls([TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK])

    start1 = time.time()
    corr_search.find_all_corr_for_a_spatio_temporal_key(tbl1, units1, corr_threshold=0.6, p_threshold=0.01)
    data1 = corr_search.data
    print("time1:", time.time() - start1)

    # corr_search = CorrSearch(db_search, "TMP", "FOR_PAIR")
    # start2 = time.time()
    # corr_search.find_all_corr_for_a_tbl_schema(tbl1, units1, threshold=0.6)
    # data2 = corr_search.data
    # print("time2:", time.time() - start2)
    data1_set = set(map(tuple, data1))
    # data2_set = set(map(tuple, data2))

    print(len(data1_set))
    # print(len(data2_set))
    # print(data1_set == data2_set)

    # tbl2 = "mqwh-r23c"
    # units2 = [Unit("location", S_GRANU.BLOCK)]


def test_find_corr_for_all_tbl():
    # granu_lists = [[T_GRANU.DAY, S_GRANU.STATE]]
    granu_lists = [[TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK]]
    conn_str = "postgresql://yuegong@localhost/st_tables"
    data_source = "chicago_10k"
    config = load_config(data_source)
    for granu_list in granu_lists:
        # dir_path = "/Users/yuegong/Documents/spatio_temporal_alignment/result/cdc/corr_{}_{}_all_join/".format(
        #     granu_list[0], granu_list[1]
        # )
        dir_path = config["corr_storage_path"]
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
            granu_list, o_t=4, r_t=0.6, p_t=0.05, fill_zero=True, dir_path=dir_path
        )

        total_time = time.time() - start
        print("total time:", total_time)
        corr_search.perf_profile["total_time"] = total_time
        print(corr_search.perf_profile)

        # dump_json(
        #     "result/run_time/cdc/perf_time_{}_{}_all_join.json".format(
        #         granu_list[0], granu_list[1]
        #     ),
        #     corr_search.perf_profile,
        # )


def test_count_corr_between_two_tbls():
    directory = "/Users/yuegong/Documents/spatio_temporal_alignment/result/corr_T_GRANU.DAY_S_GRANU.BLOCK/"
    res = defaultdict(int)
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            tbl_id = filename[5:-4]
            df = pd.read_csv(directory + filename)
            for _, row in df.iterrows():
                res[
                    (row["tbl_id1"], row["tbl_name1"], row["tbl_id2"], row["tbl_name2"])
                ] += 1
    sorted_dict = dict(sorted(res.items(), key=lambda item: item[1], reverse=True))
    data = []
    for k, v in sorted_dict.items():
        data.append(list(k) + [v])
    res_df = pd.DataFrame(
        data, columns=["tbl_id1", "tbl_name1", "tbl_id2", "tbl_name2", "corr_cnt"]
    )
    res_df.to_csv("corr_cnt.csv", index=False)
    print(sorted_dict)


def test_count_rows():
    directory = "/Users/yuegong/Documents/spatio_temporal_alignment/result/corr_T_GRANU.DAY_S_GRANU.BLOCK/"
    tbl_attrs = nexus.utils.io_utils.load_json(ATTR_PATH)
    res = {}
    total_cnt = 0
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            tbl_id = filename[5:-4]
            tbl_name = tbl_attrs[tbl_id]["name"]
            df = pd.read_csv(directory + filename)
            row_count = len(df.index)
            res[(tbl_id, tbl_name)] = row_count
            total_cnt += row_count
            print(f"{filename}: {row_count} rows")

    sorted_dict = dict(sorted(res.items(), key=lambda item: item[1], reverse=True))
    print(total_cnt)


# test_count_rows()
# test_find_corr_for_a_single_tbl()
# test_count_corr_between_two_tbls()
# # test_find_corr_for_a_tbl_schema()
test_find_corr_for_all_tbl()
