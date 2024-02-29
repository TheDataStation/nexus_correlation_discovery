import utils.io_utils as io_utils
from data_search.search_corr import CorrSearch
import time
from tqdm import tqdm
from data_search.commons import FIND_JOIN_METHOD
from utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from data_ingestion.data_profiler import Profiler

data_source = "chicago_1m"
granu_lists = [[TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK]]

# data_source = "cdc_1m"
# granu_lists = [[T_GRANU.DAY, S_GRANU.STATE]]

config = io_utils.load_config(data_source)
conn_str = config["db_path"]
overlaps = [10, 1000]
input = "full_st_schemas"
st_schemas = io_utils.load_pickle(f"evaluation/input/{data_source}/{input}.json")
joins_list = []
for granu_list in granu_lists:
    profiler = Profiler(data_source, granu_list[0], granu_list[1])
    # sort st_schemas
    sorted_st_schemas = []
    for st_schema in tqdm(st_schemas):
        tbl, schema = st_schema[0], st_schema[1]
        cnt = profiler.get_row_cnt(tbl, schema)
        sorted_st_schemas.append((st_schema, cnt))
    sorted_st_schemas = sorted(sorted_st_schemas, key=lambda x: x[1], reverse=False)
    # print(sorted_st_schemas)
    for join_method in [
        FIND_JOIN_METHOD.COST_MODEL,
    ]:
        for o_t in overlaps:
            join_costs = profiler.get_join_cost(granu_list[0], granu_list[1], o_t)
            print(f"current join method: {join_method}; overlap threshold: {o_t}")
            corr_search = CorrSearch(
                conn_str,
                data_source,
                join_method,
                join_costs,
                "AGG",
                "FOR_PAIR",
                [],
                False,
                "FDR",
                0.05,
            )
            tbl_perf_profiles = {}
            start = time.time()
            for st_schema in tqdm(sorted_st_schemas):
                tbl, schema = st_schema[0]
                agg_name = schema.get_agg_tbl_name(tbl)
                if agg_name not in join_costs:
                    print("skip")
                    continue
                # print(agg_name, join_costs[agg_name].cnt)
              
                method = corr_search.find_all_corr_for_a_spatio_temporal_key(
                    tbl,
                    schema,
                    o_t=o_t,
                    r_t=0.6,
                    p_t=0.05,
                    fill_zero=True,
                )
              
            total_time = time.time() - start
            print("total time:", total_time)
            corr_search.perf_profile["total_time"] = total_time
            print(corr_search.perf_profile)
            print(corr_search.overhead)
            corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
            
            io_utils.dump_json(
                f"evaluation/run_time/{data_source}/{input}/perf_time_{granu_list[0]}_{granu_list[1]}_{join_method.value}_{o_t}_no_vec.json",
                corr_search.perf_profile,
            )