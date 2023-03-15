import utils.io_utils as io_utils
from config import DATA_PATH, META_PATH
from data_ingestion.index_builder_db import DBIngestor
from sqlalchemy import create_engine
from tqdm import tqdm
import time


conn_string = "postgresql://yuegong@localhost/st_tables"
ingestor = DBIngestor(conn_string)


def test_ingest_tbl_e2e():
    # tbl_id = "ijzp-q8t2"
    tbl_id = "w4qj-h7bg"
    t_attrs = ["open_for_enrollment_date", "closed_for_enrollment_date"]
    s_attrs = ["location"]

    ingestor.ingest_tbl(tbl_id, t_attrs, s_attrs)


def test_ingest_all_tables():
    start_time = time.time()
    meta_data = io_utils.load_json(META_PATH)

    for obj in tqdm(meta_data):
        tbl_id, t_attrs, s_attrs = obj["tbl_id"], obj["t_attrs"], obj["s_attrs"]
        ingestor.ingest_tbl(tbl_id, t_attrs, s_attrs)
    io_utils.dump_json(
        "/Users/yuegong/Documents/spatio_temporal_alignment/data/" + "tbl_attrs.json",
        ingestor.tbls,
    )
    return time.time() - start_time


def test_expand_table():
    tbl, t_attrs, s_attrs = "ijzp-q8t2", ["date"], ["location"]
    df = io_utils.read_csv(DATA_PATH + tbl + ".csv")
    expanded = ingestor.expand_df(df, t_attrs, s_attrs)
    print(expanded.head())
    print(expanded.dtypes)


# ingesting all tables takes about 6.5 minutes
duration = test_ingest_all_tables()
print("ingestion finished in {} s".format(duration))
# test_ingest_tbl_e2e()

# test_expand_table()
