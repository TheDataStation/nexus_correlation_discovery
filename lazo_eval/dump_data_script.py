# dump join key columns from postgres for ingestion into lazo

from data_ingestion.data_profiler import Profiler
from data_search.search_db import DBSearch
from utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
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

def retrive(data_sources, t_granu: TEMPORAL_GRANU, s_granu: SPATIAL_GRANU):
    storage_dir = f'join_key_data/{"_".join(data_sources)}/time_{t_granu.value}_space_{s_granu.value}'
    io_utils.create_dir(storage_dir)
    for data_source in data_sources:
        print(data_source)
        config = io_utils.load_config(data_source)
        conn_str = config["db_path"]
        db_search = DBSearch(conn_str)
        # profiler = Profiler(data_source, [t_granu], [s_granu])
        tbl_attrs = io_utils.load_json(config['attr_path'])
        st_schemas_dict = Profiler.load_all_spatio_temporal_keys(tbl_attrs, t_granu, s_granu, type_aware=True)
        for category, schemas in st_schemas_dict.items():
            cur_storage_dir = f'{storage_dir}/{category.value}'
            io_utils.create_dir(cur_storage_dir)
            for tbl, schema in schemas:
                agg_name = schema.get_agg_tbl_name(tbl)
                values = get_key_values(db_search.cur, agg_name)
                res = {agg_name: values}
                io_utils.dump_json(f'{cur_storage_dir}/{agg_name}.json', res)

if __name__ == "__main__":
    # retrive('chicago_1m', T_GRANU.MONTH, S_GRANU.TRACT)
    retrive(['nyc_open_data', 'chicago_open_data'], TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT)