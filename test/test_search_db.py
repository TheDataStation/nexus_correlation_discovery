from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from data_search.search_db import DBSearch
import psycopg2
import numpy as np
import pandas as pd
from config import DATA_PATH


def test_aggregate_join_two_tables2():
    pass


def test_search_intersection_between_two_tables():
    conn_copg2 = psycopg2.connect("postgresql://yuegong@localhost/st_tables")
    conn_copg2.autocommit = True
    cur = conn_copg2.cursor()
    tbl1 = "gumc-mgzr"
    attrs1 = ["updated", "location"]
    tbl2 = "wqdh-9gek"
    attrs2 = ["request_date", "location"]
    granu_list = [T_GRANU.MONTH, S_GRANU.TRACT]
    get_intersection_between_two_ts_schema(tbl1, attrs1, tbl2, attrs2, granu_list, cur)


def test_search_tbl():
    conn_copg2 = psycopg2.connect("postgresql://yuegong@localhost/st_tables")
    conn_copg2.autocommit = True
    cur = conn_copg2.cursor()
    tbl1 = "gumc-mgzr"
    attrs1 = ["updated", "location"]
    granu_list = [T_GRANU.MONTH, S_GRANU.TRACT]
    res = search(tbl1, attrs1, granu_list, cur)
    print(res)


def test_agg_join_count():
    conn_str = "postgresql://yuegong@localhost/st_tables"
    db_search = DBSearch(conn_str)
    tbl1, attrs1 = "ijzp-q8t2", ["date", "location"]
    tbl2, attrs2 = "85ca-t3if", ["crash_date", "location"]
    granu_list = [T_GRANU.MONTH, S_GRANU.TRACT]
    db_search.aggregate_join_two_tables(tbl1, attrs1, tbl2, attrs2, granu_list)


def test_agg_join_avg():
    conn_str = "postgresql://yuegong@localhost/st_tables"
    db_search = DBSearch(conn_str)
    tbl1, attrs1 = "yhhz-zm2v", ["week_start", "zip_code_location"]
    tbl2, attrs2 = "8vvr-jv2g", ["week_start", "zip_code_location"]
    agg_attr1, agg_attr2 = "cases_weekly", "ili_activity_level"
    granu_list = [T_GRANU.MONTH, S_GRANU.TRACT]
    df = db_search.aggregate_join_two_tables_avg(
        tbl1, attrs1, agg_attr1, tbl2, attrs2, agg_attr2, granu_list
    )
    print(df.dtypes)


def test_select_numerical_columns():
    df = pd.read_csv(DATA_PATH + "yhhz-zm2v" + ".csv")
    print(list(df.select_dtypes(include=[np.number]).columns.values))


def test_get_table_list():
    conn_copg2 = psycopg2.connect("postgresql://yuegong@localhost/st_tables")
    conn_copg2.autocommit = True
    cur = conn_copg2.cursor()
    l = get_table_list(cur)
    print(l)


# test_search_intersection_between_two_tables()
test_select_numerical_columns()
