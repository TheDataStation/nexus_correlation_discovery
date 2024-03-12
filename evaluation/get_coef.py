"""
In this experiment, we aim to run different baselines and compare their runtimes.
"""
from utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from data_search.search_corr import CorrSearch
import time
from utils.io_utils import dump_json
import utils.io_utils as io_utils
from tqdm import tqdm
from data_search.commons import FIND_JOIN_METHOD
from data_ingestion.data_profiler import Profiler
from collections import namedtuple

data_source = "chicago_1m"
config = io_utils.load_config(data_source)
conn_str = config["db_path"]
granu_lists = [[TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK]]
overlaps = [10]
st_schema_list_path = "100_st_schemas"
st_schemas = io_utils.load_pickle(f"evaluation/input/{data_source}/{st_schema_list_path}.json")
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
        FIND_JOIN_METHOD.INDEX_SEARCH,
        # FIND_JOIN_METHOD.JOIN_ALL,
        # FIND_JOIN_METHOD.COST_MODEL,
    ]:
        for o_t in overlaps:
            join_costs = profiler.get_join_cost(granu_list[0], granu_list[1], o_t)
            print(f"current join method: {join_method}; overlap threshold: {o_t}")
            corr_search1 = CorrSearch(
                conn_str,
                data_source,
                FIND_JOIN_METHOD.INDEX_SEARCH,
                join_costs,
                "AGG",
                "MATRIX",
                ["impute_avg"],
                False,
                "FDR",
                0.05,
            )
         
            tbl_perf_profiles = {}
            start = time.time()
            data = []
            for st_schema in tqdm(sorted_st_schemas):
                tbl, schema = st_schema[0]
                agg_name = schema.get_agg_tbl_name(tbl)
                if agg_name not in join_costs:
                    print("skip")
                    continue
               
                method, schemas = corr_search1.determine_find_join_method(
                    tbl, schema, o_t, join_costs[agg_name].cnt
                )

                method = corr_search1.find_all_corr_for_a_spatio_temporal_key(
                    tbl,
                    schema,
                    overlap_threshold=o_t,
                    corr_threshold=0.6,
                    p_threshold=0.05,
                    fill_zero=True,
                )

                find_join_time = corr_search1.cur_find_join_time
                find_join_cost = corr_search1.index_search_over_head
                join_cost = corr_search1.join_all_cost
                if join_cost == 0:
                    continue
                join_time = corr_search1.cur_join_time

                print(corr_search1.perf_profile)
                print(f"find_join_cost: {find_join_cost}, find_join_time: {find_join_time}, join_cost: {join_cost}, join_time: {join_time}")
                v_cnt = join_costs[agg_name].cnt
                ratio = (find_join_time/find_join_cost)/(join_time/join_cost)
                print(f"row cnt: {v_cnt}; ratio: {ratio}; find join: {find_join_time/find_join_cost}; join: {join_time/join_cost};")
                Stats = namedtuple('Stats', ['v_cnt', 'row_to_read', 'find_join_time', 'join_cost', 'join_time', 'ratio'])
                data.append(Stats(v_cnt=v_cnt, row_to_read=find_join_cost, find_join_time=find_join_time, join_cost=join_cost, join_time=join_time, ratio=ratio))
            
            dump_json(f"evaluation/run_time/{data_source}/{st_schema_list_path}/cnt_ratio.json", data)
            # corr_search.find_all_corr_for_all_tbls(
            #     granu_list, o_t=o_t, r_t=0.6, p_t=0.05, fill_zero=True, dir_path=None
            # )
            # total_time = time.time() - start
            # print("total time:", total_time)
            # corr_search.perf_profile["total_time"] = total_time
            # print(corr_search.perf_profile)
            # print(corr_search.overhead)
            # corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
            
           