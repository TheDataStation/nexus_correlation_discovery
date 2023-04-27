import utils.io_utils as io_utils
from config import DATA_PATH, META_PATH
from data_ingestion.index_builder_db import DBIngestor, Table
from sqlalchemy import create_engine
from tqdm import tqdm
import time
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU

conn_string = "postgresql://yuegong@localhost/st_tables"
# # conn_string = "postgresql://yuegong@localhost/chicago_1m"
ingestor = DBIngestor(conn_string)


def test_ingest_tbl_e2e():
    # tbl_id = "ijzp-q8t2"
    tbl_id = "zuxi-7xem"
    t_attrs = ["creation_date"]
    s_attrs = ["location"]

    ingestor.ingest_tbl(tbl_id, t_attrs, s_attrs)


def test_ingest_all_tables():
    start_time = time.time()
    meta_data = io_utils.load_json(META_PATH)

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

    io_utils.dump_json(
        "/Users/yuegong/Documents/spatio_temporal_alignment/data/"
        + "tbl_attrs_10k.json",
        ingestor.tbls,
    )
    return time.time() - start_time


def is_num_column_valid(col_name):
    stop_words_contain = [
        "id",
        "longitude",
        "latitude",
        "ward",
        "date",
        "zipcode",
        "zip_code",
        "_zip",
        "street_number",
        "street_address",
        "district",
        "coordinate",
        "community_area",
        "_no",
        "_year",
        "_day",
        "_month",
        "_hour",
        "_number",
        "_code",
        "census_tract",
        "address",
        "x_coord",
        "y_coord",
    ]
    stop_words_equal = [
        "permit_",
        "beat",
        "zip",
        "year",
        "week_number",
        "ssa",
        "license_",
        "day_of_week",
        "police_sector",
        "police_beat",
        "license",
        "month",
        "hour",
        "day",
        "lat",
        "long",
        "mmwr_week",
        "zip4",
        "phone",
    ]
    for stop_word in stop_words_contain:
        if stop_word in col_name:
            return False
    for stop_word in stop_words_equal:
        if stop_word == col_name:
            return False
    return True


def test_correct_num_columns():
    tbls_info = io_utils.load_json(
        "/Users/yuegong/Documents/spatio_temporal_alignment/data/"
        + "tbl_attrs_10k.json",
    )
    new_tbls_info = {}
    num_columns_cnt_old = 0
    num_columns_cnt_new = 0
    for tbl_id, tbl_info in tbls_info.items():
        num_columns_corrected = []
        num_columns_cnt_old += len(tbl_info["num_columns"])
        for col in tbl_info["num_columns"]:
            if is_num_column_valid(col):
                num_columns_corrected.append(col)

        tbl_info["num_columns"] = num_columns_corrected
        num_columns_cnt_new += len(num_columns_corrected)
        new_tbls_info[tbl_id] = tbl_info
    print("column cnt old", num_columns_cnt_old)
    print("column cnt new", num_columns_cnt_new)
    print("tbl count", len(new_tbls_info))
    io_utils.dump_json(
        "/Users/yuegong/Documents/spatio_temporal_alignment/data/"
        + "tbl_attrs_10k_corrected.json",
        new_tbls_info,
    )


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


# ingesting all tables takes about 6.5 minutes
duration = test_ingest_all_tables()
print("ingestion finished in {} s".format(duration))
# test_ingest_tbl_e2e()
# test_correct_num_columns()
# test_expand_table()
