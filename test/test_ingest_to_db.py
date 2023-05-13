import utils.io_utils as io_utils
from config import DATA_PATH, META_PATH, ATTR_PATH
from data_ingestion.index_builder_raw import DBIngestor, Table
from data_ingestion.index_builder_agg import DBIngestorAgg
from sqlalchemy import create_engine
from tqdm import tqdm
import time
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU

conn_string = "postgresql://yuegong@localhost/cdc_open_data"
# conn_string = "postgresql://yuegong@localhost/st_tables"
# conn_string = "postgresql://yuegong@localhost/chicago_1m"


def test_ingest_tbl_e2e():
    t_scales = [T_GRANU.DAY, T_GRANU.MONTH, T_GRANU.QUARTER, T_GRANU.YEAR]
    s_scales = [S_GRANU.COUNTY, S_GRANU.STATE]
    # ingestor = DBIngestor(conn_string, t_scales, s_scales)
    ingestor = DBIngestorAgg(conn_string, t_scales, s_scales)

    tbl_info_all = io_utils.load_json(ATTR_PATH)
    tbl_id = "4bft-6yws"
    tbl_info = tbl_info_all[tbl_id]
    tbl = Table(
        domain="",
        tbl_id=tbl_id,
        tbl_name=tbl_info["name"],
        t_attrs=tbl_info["t_attrs"],
        s_attrs=tbl_info["s_attrs"],
        num_columns=tbl_info["num_columns"],
    )

    ingestor.ingest_tbl(tbl)


def test_ingest_all_tables():
    start_time = time.time()
    meta_data = io_utils.load_json(META_PATH)

    t_scales = [T_GRANU.DAY, T_GRANU.MONTH, T_GRANU.QUARTER, T_GRANU.YEAR]
    s_scales = [S_GRANU.COUNTY, S_GRANU.STATE]
    # ingestor = DBIngestor(conn_string, t_scales, s_scales)
    ingestor = DBIngestorAgg(conn_string, t_scales, s_scales)

    ingestor.clean_aggregated_idx_tbls()

    for obj in tqdm(meta_data):
        tbl = Table(
            domain=obj["domain"],
            tbl_id=obj["tbl_id"],
            tbl_name=obj["tbl_name"],
            t_attrs=obj["t_attrs"],
            s_attrs=obj["s_attrs"],
            num_columns=obj["num_columns"],
        )
        print(tbl.tbl_id)
        ingestor.ingest_tbl(tbl)

    ingestor.create_index_on_agg_idx_table()

    # print("begin creating indices")
    # begin_creating_index = time.time()
    # ingestor.create_index_on_agg_idx_table()
    # print("used {}", time.time() - begin_creating_index)

    io_utils.dump_json(
        "/Users/yuegong/Documents/spatio_temporal_alignment/data/"
        + "tbl_attrs_cdc_10k.json",
        ingestor.tbls,
    )
    return time.time() - start_time


def test_expand_table():
    meta_data = io_utils.load_json(META_PATH)
    for obj in tqdm(meta_data):
        tbl_id, t_attrs, s_attrs = obj["tbl_id"], obj["t_attrs"], obj["s_attrs"]
        print(tbl_id)
        df = io_utils.read_csv(DATA_PATH + tbl_id + ".csv")
        df, success_t_attrs, success_s_attrs = ingestor.expand_df(df, t_attrs, s_attrs)
        t_attrs_granu, s_attrs_granu = [], []
        for t_attr in success_t_attrs:
            for t_granu in T_GRANU:
                new_attr = "{}_{}".format(t_attr, t_granu.value)
                t_attrs_granu.append(new_attr)
        for s_attr in success_s_attrs:
            for s_granu in S_GRANU:
                new_attr = "{}_{}".format(s_attr, s_granu.value)
                s_attrs_granu.append(new_attr)
        print(success_t_attrs + t_attrs_granu)
        print("t_attrs before: {}".format(len(success_t_attrs + t_attrs_granu)))
        print(df[success_t_attrs + t_attrs_granu])
        after_cnt = len(
            df[success_t_attrs + t_attrs_granu].dropna().T.drop_duplicates().T.columns
        )
        print("t_attrs after: {}".format(after_cnt))


# ingesting all tables in chicago open data 10k takes about 6.5 minutes
duration = test_ingest_all_tables()
print("ingestion finished in {} s".format(duration))
# start = time.time()
# test_ingest_tbl_e2e()
# print("time took:", time.time() - start)
# test_correct_num_columns()
# test_expand_table()
