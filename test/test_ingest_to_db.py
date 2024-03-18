import nexus.utils.io_utils as io_utils
# from data_ingestion.index_builder_raw import DBIngestor, Table
from nexus.data_ingestion.data_ingestor import DBIngestor
from tqdm import tqdm
import time
from nexus.utils.coordinate import SPATIAL_GRANU
import nexus.utils.coordinate as coordinate
from utils.time_point import TEMPORAL_GRANU
from utils.data_model import Attr, Table
from nexus.data_ingestion.connection import ConnectionFactory
from nexus.data_ingestion.data_profiler import Profiler


# conn_string = "postgresql://yuegong@localhost/cdc_open_data"
# conn_string = "postgresql://yuegong@localhost/st_tables"
# conn_string = "postgresql://yuegong@localhost/chicago_open_data_1m"


def test_ingest_tbl_e2e():
    t_scales = [TEMPORAL_GRANU.DAY, TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.BLOCK, SPATIAL_GRANU.TRACT]
    conn_string = "postgresql://yuegong@localhost/test"
    ingestor = DBIngestor(conn_string, t_scales, s_scales)
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
        temporal_attrs=tbl_info["t_attrs"],
        spatial_attrs=tbl_info["s_attrs"],
        num_columns=tbl_info["num_columns"],
    )
    ingestor.ingest_tbl(tbl)


def test_ingest_all_tables():
    start_time = time.time()

    t_scales = [TEMPORAL_GRANU.DAY, TEMPORAL_GRANU.MONTH]
    # s_scales = [S_GRANU.COUNTY, S_GRANU.STATE]
    s_scales = [SPATIAL_GRANU.BLOCK, SPATIAL_GRANU.TRACT]
    # ingestor = DBIngestor(conn_string, t_scales, s_scales)

    data_source = "chicago_1m"
    config = io_utils.load_config(data_source)
    conn_string = config["db_path"]
    idx_tbl_path = config["idx_tbl_path"]
    idx_tbls = io_utils.load_json(idx_tbl_path)
    ingestor = DBIngestor(conn_string, t_scales, s_scales)
    ingestor.create_cnt_tbls(data_source, [TEMPORAL_GRANU.DAY], [SPATIAL_GRANU.BLOCK])
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
    t_scales = [TEMPORAL_GRANU.DAY, TEMPORAL_GRANU.MONTH, TEMPORAL_GRANU.QUARTER, TEMPORAL_GRANU.YEAR]
    # s_scales = [S_GRANU.COUNTY, S_GRANU.STATE]
    s_scales = [SPATIAL_GRANU.BLOCK, SPATIAL_GRANU.TRACT]
    ingestor = DBIngestor(conn_string, t_scales, s_scales)
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
            for t_granu in TEMPORAL_GRANU:
                new_attr = "{}_{}".format(t_attr, t_granu.value)
                t_attrs_granu.append(new_attr)
        for s_attr in success_s_attrs:
            for s_granu in SPATIAL_GRANU:
                new_attr = "{}_{}".format(s_attr, s_granu.value)
                s_attrs_granu.append(new_attr)
        print(success_t_attrs + t_attrs_granu)
        print("t_attrs before: {}".format(len(success_t_attrs + t_attrs_granu)))
        print(df[success_t_attrs + t_attrs_granu])
        after_cnt = len(
            df[success_t_attrs + t_attrs_granu].dropna().T.drop_duplicates().T.columns
        )
        print("t_attrs after: {}".format(after_cnt))


def test_create_sketch_tbl():
    t_scales = [TEMPORAL_GRANU.DAY, TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.BLOCK, SPATIAL_GRANU.TRACT]
    data_source = "chicago_1m"
    config = io_utils.load_config(data_source)
    conn_string = config["db_path"]
    ingestor = DBIngestor(conn_string, data_source, t_scales, s_scales)
    agg_tbl = 'ijzp-q8t2_date_2'
    k = 256
    ingestor.create_correlation_sketch(agg_tbl, k)


def test_ingest_all_tbls():
    # data_sources = ['chicago_factors', 'asthma', 'chicago_1m_zipcode']
    data_sources = ['chicago_factors']
    ingestor = DBIngestor('data/quickstart.db', engine='duckdb')
    temporal_granu_l = []
    spatial_granu_l = [SPATIAL_GRANU.ZIPCODE]
    for data_source in data_sources:
        ingestor.ingest_data_source(data_source, temporal_granu_l=temporal_granu_l, spatial_granu_l=spatial_granu_l,
                                    persist=True)

def test_create_inverted_indices():
    data_sources = ['chicago_factors', 'asthma', 'chicago_zipcode']
    ingestor = DBIngestor('data/quickstart.db', engine='duckdb')
    temporal_granu = None
    spatial_granu = SPATIAL_GRANU.ZIPCODE
    # for data_source in data_sources:
    #     ingestor.create_inverted_indices_for_a_data_source(data_source, temporal_granu, spatial_granu)
    ingestor.create_cnt_tbls_for_inv_index_tbls(['space_6_inv'])

def test_create_cnt_tables_for_all_tables():
    data_sources = ['chicago_factors', 'asthma', 'chicago_zipcode']
    ingestor = DBIngestor('data/quickstart.db', engine='duckdb')
    temporal_granu = None
    spatial_granu = SPATIAL_GRANU.ZIPCODE
    for data_source in data_sources:
        ingestor.create_count_tables_for_aggregated_tables_in_a_data_source(data_source,
                                                                            temporal_granu,
                                                                            spatial_granu)

def test_profile_data_sources():
    data_sources = ['chicago_factors', 'asthma', 'chicago_1m_zipcode']
    db_engine = ConnectionFactory.create_connection('data/quickstart.db', engine='duckdb')
    temporal_granu_l = []
    spatial_granu_l = [SPATIAL_GRANU.ZIPCODE]
    for data_source in data_sources:
        profiler = Profiler(db_engine, data_source)
        profiler.collect_agg_tbl_col_stats(temporal_granu_l, spatial_granu_l)


def test_ingest_a_tbl(tbl_id, engine):
    # ingest asthma dataset
    data_sources = ['chicago_1m_zipcode']
    conn_str = "postgresql://yuegong@localhost/test"
    temporal_granu_l = []
    spatial_granu_l = [SPATIAL_GRANU.ZIPCODE]

    # ingest tables
    for data_source in data_sources:
        print(data_source)
        start_time = time.time()
        if engine == 'postgres':
            ingestor = DBIngestor(conn_str)
        else:
            ingestor = DBIngestor('data/test.db', engine='duckdb')
        data_source_config = io_utils.load_config(data_source)
        meta_data = io_utils.load_json(data_source_config['meta_path'])
        obj = meta_data[tbl_id]
        t_attrs = [Attr(attr["name"], attr["granu"]) for attr in obj["t_attrs"]]
        s_attrs = [Attr(attr["name"], attr["granu"]) for attr in obj["s_attrs"]]
        t_attrs, s_attrs = ingestor.select_valid_attrs(t_attrs, 1), ingestor.select_valid_attrs(s_attrs, 1)

        tbl = Table(
            domain=obj["domain"],
            tbl_id=obj["tbl_id"],
            tbl_name=obj["tbl_name"],
            temporal_attrs=t_attrs,
            spatial_attrs=s_attrs,
            num_columns=obj["num_columns"],
            link=obj["link"] if "link" in obj else "",
        )

        ingestor.ingest_tbl(tbl, temporal_granu_l, spatial_granu_l, data_source_config=data_source_config)
        print(f"ingesting data finished in {time.time() - start_time} s")


if __name__ == '__main__':
    # engine='duckdb'
    # for tbl in ['22u3-xenr']:
    #     test_ingest_a_tbl(tbl, engine)
    test_create_cnt_tables_for_all_tables()
