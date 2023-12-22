# dump join key columns from postgres for ingestion into lazo

from data_ingestion.profile_datasets import Profiler
from data_search.search_db import DBSearch
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from utils import io_utils
from psycopg2 import sql
import os

def is_tbl_exist(cur, tbl_name):
    cur.execute(f"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='{tbl_name}')")
    if cur.fetchone()[0]:
        return True
    else:
        return False

def get_key_values(cur, agg_tbl):
    sql_str = """
        SELECT val FROM {agg_tbl};
    """

    query = sql.SQL(sql_str).format(
        agg_tbl=sql.Identifier(agg_tbl)
    )

    cur.execute(query)
    return [r[0] for r in cur.fetchall()]

def retrive(data_source, t_granu: T_GRANU, s_granu: S_GRANU):
    storage_dir = f'join_key_data/{data_source}/time_{t_granu.value}_space_{s_granu.value}'
    io_utils.create_dir(storage_dir)

    config = io_utils.load_config(data_source)
    conn_str = config["db_path"]
    db_search = DBSearch(conn_str)
    profiler = Profiler(data_source, [t_granu], [s_granu])
    st_schemas_dict = profiler.load_all_st_schemas(t_granu, s_granu, type_aware=True)
    for category, schemas in st_schemas_dict.items():
        cur_storage_dir = f'{storage_dir}/{category.value}'
        io_utils.create_dir(cur_storage_dir)
        for tbl, schema in schemas:
            agg_name = schema.get_agg_tbl_name(tbl)
            values = get_key_values(db_search.cur, agg_name)
            res = {agg_name: values}
            io_utils.dump_json(f'{cur_storage_dir}/{agg_name}.json', res)

if __name__ == "__main__":
    retrive('chicago_1m', T_GRANU.DAY, S_GRANU.BLOCK)