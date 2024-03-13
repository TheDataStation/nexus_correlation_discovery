## find ground truth for all pairs of joinable tables
from tqdm import tqdm

from utils.data_model import SpatioTemporalKey, Attr
from utils import io_utils
from data_search.search_db import DBSearch
from collections import defaultdict
from utils.spatial_hierarchy import SPATIAL_GRANU

from utils.time_point import TEMPORAL_GRANU
import time

def generate_ground_truth(data_sources, t_granu, s_granu, o_t, persist):
    # use inverted index to construct the ground truth, no comparison between performance yet.
    gt = defaultdict(list) # ground truth joinable pairs tbl -> list of (joinable_tbl, overlap)
    for data_source in data_sources:
        config = io_utils.load_config(data_source)
        tbl_attrs = io_utils.load_json(config["attr_path"])
        db_search = DBSearch(config['db_path'])
       
        for tbl in tqdm(tbl_attrs.keys()):
            st_schema_list = []
            t_attrs, s_attrs = (
                tbl_attrs[tbl]["t_attrs"],
                tbl_attrs[tbl]["s_attrs"],
            )
            for t in t_attrs:
                st_schema_list.append(SpatioTemporalKey(temporal_attr=Attr(t, t_granu)))

            for s in s_attrs:
                st_schema_list.append(SpatioTemporalKey(spatial_attr=Attr(s, s_granu)))

            for t in t_attrs:
                for s in s_attrs:
                    st_schema_list.append(SpatioTemporalKey(Attr(t, t_granu), Attr(s, s_granu)))

            for st_schema in st_schema_list:
                aligned_schemas = db_search.find_augmentable_st_schemas(tbl, st_schema, o_t, mode="inv_idx")
                for tbl_info in aligned_schemas:
                    tbl2, st_schema2, overlap = (
                        tbl_info[0],
                        tbl_info[1],
                        tbl_info[2]
                    )
                    gt[st_schema.get_agg_tbl_name(tbl)].append((st_schema2.get_agg_tbl_name(tbl2), overlap))
    if persist:
        io_utils.dump_json(f"lazo_eval/ground_truth_join/{'_'.join(data_sources)}/join_ground_truth_{t_granu}_{s_granu}_overlap_{o_t}.json", gt)

if __name__ == "__main__":
    # data_source = "chicago_1m" 
    data_sources = ['nyc_open_data', 'chicago_open_data']
    granu_lists = [(TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT)]
    # t_granu, s_granu = T_GRANU.MONTH, S_GRANU.TRACT
    o_t = 10
    persist = True
    for t_granu, s_granu in granu_lists:
        start = time.time()
        generate_ground_truth(data_sources, t_granu, s_granu, o_t, persist)
        total_time = time.time() - start
        print(f"total time: {total_time}")
        # io_utils.dump_json(f"lazo_eval/ground_truth_join/find_joinable_time_{t_granu}_{s_granu}_overlap_{o_t}.json", {"total_time": total_time})