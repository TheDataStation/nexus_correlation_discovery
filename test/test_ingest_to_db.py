import utils.io_utils as io_utils
from config import DATA_PATH, META_PATH, ATTR_PATH
from data_ingestion.index_builder_raw import DBIngestor, Table
from data_ingestion.index_builder_agg import DBIngestorAgg
from sqlalchemy import create_engine
from tqdm import tqdm
import time
from utils.coordinate import S_GRANU
import utils.coordinate as coordinate
from utils.time_point import T_GRANU

# conn_string = "postgresql://yuegong@localhost/cdc_open_data"
# conn_string = "postgresql://yuegong@localhost/st_tables"
# conn_string = "postgresql://yuegong@localhost/chicago_open_data_1m"


def test_ingest_tbl_e2e():
    t_scales = [T_GRANU.DAY, T_GRANU.MONTH]
    s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]
    conn_string = "postgresql://yuegong@localhost/test"
    ingestor = DBIngestorAgg(conn_string, t_scales, s_scales)
    data_config = io_utils.load_config("chicago_10k")
    geo_chain = data_config["geo_chain"]
    geo_keys = data_config["geo_keys"]
    coordinate.resolve_geo_chain(geo_chain, geo_keys)

    attr_path = data_config["attr_path"]
    ingestor.data_path = io_utils.load_config("chicago_1m")["data_path"]
    tbl_info_all = io_utils.load_json(attr_path)
    # print(tbl_info_all)
    # tbl_id = "zgvr-7yfd"
    tbl_id = "ydr8-5enu"
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

    t_scales = [T_GRANU.DAY, T_GRANU.MONTH]
    # s_scales = [S_GRANU.COUNTY, S_GRANU.STATE]
    s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]
    # ingestor = DBIngestor(conn_string, t_scales, s_scales)

    data_source = "chicago_1m"
    config = io_utils.load_config(data_source)
    conn_string = config["db_path"]
    idx_tbl_path = config["idx_tbl_path"]
    idx_tbls = io_utils.load_json(idx_tbl_path)
    ingestor = DBIngestorAgg(conn_string, t_scales, s_scales)
    ingestor.create_cnt_tbls(data_source, [T_GRANU.DAY], [S_GRANU.BLOCK])
    # ingestor.create_inv_cnt_tbls(idx_tbls)
    # ingestor.ingest_data_source("chicago_10k", clean=True, persist=True)

    # idx_tables = []
    # for t_scale in t_scales:
    #     idx_tables.append("time_{}".format(t_scale.value))
    # for s_scale in s_scales:
    #     idx_tables.append("space_{}".format(s_scale.value))
    # for t_scale in t_scales:
    #     for s_scale in s_scales:
    #         idx_tables.append("time_{}_space_{}".format(t_scale.value, s_scale.value))
    # ingestor.create_inv_indices(idx_tables)

    return time.time() - start_time


def test_create_index_on_agg_idx_table():
    t_scales = [T_GRANU.DAY, T_GRANU.MONTH, T_GRANU.QUARTER, T_GRANU.YEAR]
    # s_scales = [S_GRANU.COUNTY, S_GRANU.STATE]
    s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]
    ingestor = DBIngestorAgg(conn_string, t_scales, s_scales)
    print("begin creating indices on the aggregated index tables")
    ingestor.create_index_on_agg_idx_table()


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
test_ingest_all_tables()
# print("ingestion finished in {} s".format(duration))
# start = time.time()
# test_ingest_tbl_e2e()
# print("time took:", time.time() - start)

# start = time.time()
# test_create_index_on_agg_idx_table()
# print("time took:", time.time() - start)
# test_correct_num_columns()
# test_expand_table()
