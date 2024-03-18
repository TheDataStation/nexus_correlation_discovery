from nexus.utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from nexus.data_search.search_db import DBSearch
from nexus.data_ingestion.db_ops import select_columns
from nexus.data_search.db_ops import estimate_joinable_candidates
import psycopg2
import numpy as np
import pandas as pd
from config import DATA_PATH
from utils.data_model import Attr, Variable, AggFunc, SpatioTemporalKey
import time


def test_select_columns():
    conn = psycopg2.connect("postgresql://yuegong@localhost/chicago_open_data_1m")
    conn.autocommit = True
    cur = conn.cursor()
    res = select_columns(
        cur,
        "22u3-xenr_violation_date_2_location_1",
        ["violation_date_2"],
        "RAW",
    )
    print(res[:5])


def search_joinable():
    conn = psycopg2.connect("postgresql://yuegong@localhost/st_tables")
    cur = conn.cursor()
    tbl = "22u3-xenr"
    st_schema = SpatioTemporalKey(
        Attr("violation_date", TEMPORAL_GRANU.DAY), Attr("location", SPATIAL_GRANU.BLOCK)
    )
    # res1 = get_intersection_agg_idx(cur, tbl, st_schema, None, 4)
    # print(res1)
    # print(len(res1))
    res2 = estimate_joinable_candidates(cur, tbl, st_schema, 4)
    print(res2)
    print(len(res2))


def test_search_intersection_between_two_tables():
    conn_copg2 = psycopg2.connect("postgresql://yuegong@localhost/st_tables")
    conn_copg2.autocommit = True
    cur = conn_copg2.cursor()
    tbl1 = "gumc-mgzr"
    attrs1 = ["updated", "location"]
    tbl2 = "wqdh-9gek"
    attrs2 = ["request_date", "location"]
    granu_list = [TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT]
    get_intersection_between_two_ts_schema(tbl1, attrs1, tbl2, attrs2, granu_list, cur)


def test_find_joinable_tables():
    conn_str = "postgresql://yuegong@localhost/st_tables"
    db_search = DBSearch(conn_str)
    tbl = "ijzp-q8t2"
    units = [Attr("date", TEMPORAL_GRANU.DAY), Attr("location", SPATIAL_GRANU.BLOCK)]
    aligned_tbls = db_search.find_augmentable_st_schemas(tbl, units, 4, mode="agg_idx")


def test_search_tbl():
    conn_copg2 = psycopg2.connect("postgresql://yuegong@localhost/st_tables")
    conn_copg2.autocommit = True
    cur = conn_copg2.cursor()
    tbl1 = "gumc-mgzr"
    attrs1 = ["updated", "location"]
    granu_list = [TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT]
    res = search(tbl1, attrs1, granu_list, cur)
    print(res)


def test_agg_join_count():
    conn_str = "postgresql://yuegong@localhost/st_tables"
    db_search = DBSearch(conn_str)
    tbl1 = "ijzp-q8t2"
    units1 = [Attr("date", TEMPORAL_GRANU.DAY), Attr("location", SPATIAL_GRANU.BLOCK)]
    tbl2 = "85ca-t3if"
    units2 = [Attr("crash_date", TEMPORAL_GRANU.DAY), Attr("location", SPATIAL_GRANU.BLOCK)]
    vars1 = [Variable(tbl1, "*", AggFunc.COUNT, "count1")]
    vars2 = [Variable(tbl2, "*", AggFunc.COUNT, "count2")]
    start = time.time()
    # join method
    merged = db_search.aggregate_join_two_tables(
        tbl1, units1, vars1, tbl2, units2, vars2
    )

    names1 = [unit.to_int_name() for unit in units1] + [
        var.var_name[:63] for var in vars1
    ]
    names2 = [unit.to_int_name() for unit in units1] + [
        var.var_name[:63] for var in vars2
    ]

    df1_join, df2_join = merged[names1].reset_index(drop=True), merged[
        names2
    ].reset_index(drop=True)

    print("join time", time.time() - start)
    start = time.time()
    # align method
    # df1_align, df2_align = db_search.align_two_two_tables(
    #     tbl1, units1, vars1, tbl2, units2, vars2
    # )
    merged = db_search.align_two_two_tables(tbl1, units1, vars1, tbl2, units2, vars2)
    names1 = [unit.to_int_name() for unit in units1] + [
        var.var_name[:63] for var in vars1
    ]
    names2 = [unit.to_int_name() for unit in units1] + [
        var.var_name[:63] for var in vars2
    ]

    df1_align, df2_align = merged[names1].reset_index(drop=True), merged[
        names2
    ].reset_index(drop=True)

    print("align time", time.time() - start)

    # print(df1_align)
    # print(df1_join)
    print(df1_align.equals(df1_join))

    df2_join.columns = df2_align.columns
    print(df2_align.equals(df2_join))


def test_agg_join_avg():
    conn_str = "postgresql://yuegong@localhost/st_tables"
    db_search = DBSearch(conn_str)
    tbl1, attrs1 = "yhhz-zm2v", ["week_start", "zip_code_location"]
    tbl2, attrs2 = "8vvr-jv2g", ["week_start", "zip_code_location"]
    agg_attr1, agg_attr2 = "cases_weekly", "ili_activity_level"
    granu_list = [TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT]
    df = db_search.aggregate_join_two_tables_avg(
        tbl1, attrs1, agg_attr1, tbl2, attrs2, agg_attr2, granu_list
    )
    print(df.dtypes)


def test_select_numerical_columns():
    df = pd.read_csv(DATA_PATH + "yhhz-zm2v" + ".csv")
    print(list(df.select_dtypes(include=[np.number]).columns.values))


search_joinable()
# with shelve.open("inverted_indices/chicago_1k/{}".format("time_2")) as db:
#     for key in db:
#         if len(db[key]) > 1:
#             print(key)
#             print(db[key])
