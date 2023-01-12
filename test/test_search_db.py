from coordinate import S_GRANU
from time_point import T_GRANU
from search_db import DBSearch
import psycopg2


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


def test_get_table_list():
    conn_copg2 = psycopg2.connect("postgresql://yuegong@localhost/st_tables")
    conn_copg2.autocommit = True
    cur = conn_copg2.cursor()
    l = get_table_list(cur)
    print(l)


# test_search_intersection_between_two_tables()
test_agg_join_count()
