import io_utils
from config import META_PATH
import index_builder
import time
from config import DATA_PATH, INDEX_PATH

def test_read_data():
    start = time.time()
    tbl_id = 'ijzp-q8t2.csv'
    t_attr, s_attr = "date", "location"
    index_builder.read_data(tbl_id, t_attr, s_attr)
    print(time.time() - start)

def test_index_builder_for_single_tbl():
    start = time.time()
    tbl_id = 'zuxi-7xem'
    t_attr, s_attr = 'creation_date', "location"
    index_builder.build_index_for_tbl(tbl_id + '.csv', t_attr, s_attr, 'test/')
    print(time.time() - start)

def test_index_builder_time():
    index_builder.build_indices_for_all_tables()

def test_storage():
    tbl_cnt = 0
    row_cnt = 0
    meta_data = io_utils.load_json(META_PATH)
    for obj in meta_data:
        domain, tbl_id, tbl_name, t_attrs, s_attrs = obj['domain'], obj['tbl_id'], obj['tbl_name'], obj['t_attrs'], obj['s_attrs']
        if len(t_attrs) and len(s_attrs):
            df = io_utils.read_columns(DATA_PATH + tbl_id + '.csv', [t_attrs[0], s_attrs[0]])
        elif len(t_attrs):
            df = io_utils.read_columns(DATA_PATH + tbl_id + '.csv', [t_attrs[0]])
        elif len(s_attrs):
            df = io_utils.read_columns(DATA_PATH + tbl_id + '.csv', [s_attrs[0]])
        tbl_cnt += 1
        row_cnt += len(df)
        df.to_csv('data/chicago_1k_attributes/{}.csv'.format(tbl_id))
    print(tbl_cnt, row_cnt)


start = time.time()
test_index_builder_time()
print("duration:", time.time() - start)

