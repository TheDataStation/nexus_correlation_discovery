## find ground truth for all pairs of joinable tables
from tqdm import tqdm

from data_search.data_model import ST_Schema, Unit
from utils import io_utils
from data_search.search_db import DBSearch
from collections import defaultdict
from utils.coordinate import S_GRANU

from utils.time_point import T_GRANU
import time

def generate_ground_truth(data_source, t_granu, s_granu, o_t):
    # use inverted index to construct the ground truth, no comparison between performance yet.
    config = io_utils.load_config(data_source)
    tbl_attrs = io_utils.load_json(config["attr_path"])
    db_search = DBSearch(config['db_path'])
    gt = defaultdict(list) # ground truth joinable pairs tbl -> list of (joinable_tbl, overlap)
    for tbl in tqdm(tbl_attrs.keys()):
        st_schema_list = []
        t_attrs, s_attrs = (
            tbl_attrs[tbl]["t_attrs"],
            tbl_attrs[tbl]["s_attrs"],
        )
        for t in t_attrs:
            st_schema_list.append(ST_Schema(t_unit=Unit(t, t_granu)))

        for s in s_attrs:
            st_schema_list.append(ST_Schema(s_unit=Unit(s, s_granu)))

        for t in t_attrs:
            for s in s_attrs:
                st_schema_list.append(ST_Schema(Unit(t, t_granu), Unit(s, s_granu)))

        for st_schema in st_schema_list:
            aligned_schemas = db_search.find_augmentable_st_schemas(tbl, st_schema, o_t, mode="inv_idx")
            for tbl_info in aligned_schemas:
                tbl2, st_schema2, overlap = (
                    tbl_info[0],
                    tbl_info[1],
                    tbl_info[2]
                )
                gt[st_schema.get_agg_tbl_name(tbl)].append((st_schema2.get_agg_tbl_name(tbl2), overlap))
    io_utils.dump_json(f"lazo_eval/join_ground_truth_{t_granu}_{s_granu}_overlap_{o_t}.json", gt)

if __name__ == "__main__":
    data_source = "chicago_1m" 
    t_granu, s_granu = T_GRANU.MONTH, S_GRANU.TRACT
    o_t = 10
    generate_ground_truth(data_source, t_granu, s_granu, 10)