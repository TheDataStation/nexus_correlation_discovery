import utils.io_utils as io_utils
import time
from utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from data_ingestion.data_ingestor import DBIngestor
import pandas as pd

def ingest_chicago_time_sampling():
    start_time = time.time()
    data_source = "chicago_1m_time_sampling"
    config = io_utils.load_config(data_source)
    conn_string = config["db_path"]
    conn_string = "postgresql://yuegong@localhost/chicago_1m_time_2021_2023"
    t_scales = [TEMPORAL_GRANU.DAY, TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.BLOCK, SPATIAL_GRANU.TRACT]
    ingestor = DBIngestor(conn_string, data_source, t_scales, s_scales)
    ingestor.set_time_range([pd.to_datetime('2021-01-01 00:00:00'), pd.to_datetime('2023-01-01 00:00:00')])
    ingestor.ingest_data_source(None, clean=True, persist=True)
    print(f"ingesting data finished in {time.time() - start_time} s")

def ingest_chicago():
    start_time = time.time()
    data_source = "chicago_1m"
    config = io_utils.load_config(data_source)
    conn_string = config["db_path"]
    conn_string = "postgresql://yuegong@localhost/chicago_1m_new"
    t_scales = [TEMPORAL_GRANU.DAY, TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.BLOCK, SPATIAL_GRANU.TRACT]

    ingestor = DBIngestor(conn_string, data_source, t_scales, s_scales)
    # attr_path = config["attr_path"]
    # tbls = io_utils.load_json(attr_path)
    # for tbl_id, info in tqdm(tbls.items()):
    #     ingestor.create_cnt_tbl(
    #         tbl_id,
    #         info["t_attrs"],
    #         info["s_attrs"],
    #         t_scales,
    #         s_scales,
    #     )
    ingestor.ingest_data_source(None, clean=True, persist=True)

    print(f"ingesting data finished in {time.time() - start_time} s")

def ingest_open_data():
    data_sources = [
                    'ny_open_data', 'ct_open_data', 'maryland_open_data', 'pa_open_data',
                    'texas_open_data', 'wa_open_data', 'sf_open_data', 'la_open_data', 'nyc_open_data', 'chicago_open_data'
                    ]
    t_scales = [TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.TRACT]
    for data_source in data_sources:
        print(data_source)
        start_time = time.time()
        conn_string = "postgresql://yuegong@localhost/opendata_large"
        ingestor = DBIngestor(conn_string, data_source, t_scales, s_scales)
        if data_source == 'chicago_open_data':
            ingestor.ingest_data_source(None, clean=False, persist=True, max_limit=2)
        else:
            ingestor.ingest_data_source(None, clean=False, persist=True, max_limit=1)
        print(f"ingesting data finished in {time.time() - start_time} s")

def create_cnt_tbls(data_sources, conn_str):
    t_scales = [TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.TRACT]
    idx_tbls = ["time_3_space_3_inv", "space_3_inv", "time_3_inv"]
    DBIngestor.create_cnt_tbls_for_inv_index_tbls(conn_str, idx_tbls)
    for data_source in data_sources:
        ingestor = DBIngestor(conn_str, data_source, t_scales, s_scales)
        config = io_utils.load_config(data_source)
        print(config["attr_path"])
        tbl_attrs = io_utils.load_json(config["attr_path"])
        for tbl_id, info in tbl_attrs.items():
            # if tbl_id == "rybz-nyjw":
            #     ingestor.ingest_tbl(Table("", tbl_id, info["name"], info["t_attrs"], info["s_attrs"], info["num_columns"]))
            print(data_source, tbl_id)
            ingestor.create_cnt_tbl(
                tbl_id,
                info["t_attrs"],
                info["s_attrs"],
                t_scales,
                s_scales,
            )

# don't forget to create the indices and count tables for inverted index tables.
def create_cnt_tbls_for_inverted_index_tbls():
    conn_string = "postgresql://yuegong@localhost/opendata"
    idx_tbls = ["time_3_space_3_inv", "space_3_inv", "time_3_inv"]
    DBIngestor.create_cnt_tbls_for_inv_index_tbls(conn_string, idx_tbls)
    
def retry_failed_tbls(data_source):
    config = io_utils.load_config(data_source)
    failed_tbls = io_utils.load_json(config["failed_tbl_path"])
    print(failed_tbls)
    conn_string = "postgresql://yuegong@localhost/opendata_large"
    t_scales = [TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.TRACT]
    ingestor = DBIngestor(conn_string, data_source, t_scales, s_scales)
    if data_source == 'chicago_open_data':
        ingestor.ingest_data_source(None, clean=False, persist=True, max_limit=2, retry_list=['uk68-3rjc'])
    else:
        ingestor.ingest_data_source(None, clean=False, persist=True, max_limit=1, retry_list=['k7nh-aufb'])


if __name__ == "__main__":
    data_sources = [
                    # 'ny_open_data', 'ct_open_data', 'maryland_open_data', 'pa_open_data',
                    # 'texas_open_data', 'wa_open_data', 'sf_open_data', 'la_open_data', 
                    'nyc_open_data', 
                    'chicago_open_data'
    ]
    create_cnt_tbls(data_sources, "postgresql://yuegong@localhost/opendata_large")
    # ingest_open_data()
    # create_cnt_tbls_for_inverted_index_tbls()
    # retry_failed_tbls('chicago_open_data')