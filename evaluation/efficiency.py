"""
In this experiment, we aim to run different baselines and compare their runtimes.
"""
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from data_search.search_corr import CorrSearch
import time
from utils.io_utils import dump_json
import utils.io_utils as io_utils
from tqdm import tqdm
from data_search.commons import FIND_JOIN_METHOD
from data_ingestion.profile_datasets import Profiler

data_source = "chicago_1m"
config = io_utils.load_config(data_source)
conn_str = config["db_path"]
granu_lists = [[T_GRANU.DAY, S_GRANU.BLOCK]]
overlaps = [10]
st_schemas = io_utils.load_pickle(f"evaluation/input/{data_source}/200_st_schemas.json")
joins_list = []
for granu_list in granu_lists:
    profiler = Profiler(data_source, granu_list[0], granu_list[1])
    # sort st_schemas
    sorted_st_schemas = []
    for st_schema in tqdm(st_schemas):
        tbl, schema = st_schema[0], st_schema[1]
        cnt = profiler.get_row_cnt(tbl, schema)
        sorted_st_schemas.append((st_schema, cnt))
    sorted_st_schemas = sorted(sorted_st_schemas, key=lambda x: x[0][0], reverse=False)[
        0:8
    ]
    # print(sorted_st_schemas)
    for join_method in [
        FIND_JOIN_METHOD.INDEX_SEARCH,
        FIND_JOIN_METHOD.JOIN_ALL,
        # FIND_JOIN_METHOD.COST_MODEL,
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
                "MATRIX",
                ["impute_avg", "impute_zero"],
                False,
                "FDR",
                0.05,
            )
            tbl_perf_profiles = {}
            start = time.time()
            for st_schema in tqdm(sorted_st_schemas):
                tbl, schema = st_schema[0]
                agg_name = schema.get_agg_tbl_name(tbl)
                print(agg_name)
                _start = time.time()
                method = corr_search.find_all_corr_for_a_tbl_schema(
                    tbl,
                    schema,
                    o_t=o_t,
                    r_t=0.6,
                    p_t=0.05,
                    fill_zero=True,
                )
                _end = time.time()
                tbl_perf_profiles[agg_name] = _end - _start

            # corr_search.find_all_corr_for_all_tbls(
            #     granu_list, o_t=o_t, r_t=0.6, p_t=0.05, fill_zero=True, dir_path=None
            # )
            total_time = time.time() - start
            print("total time:", total_time)
            corr_search.perf_profile["total_time"] = total_time
            print(corr_search.perf_profile)
            print(corr_search.overhead)
            corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
            joins_list.append(corr_search.joins)
            # dump_json(
            #     f"evaluation/run_time/{data_source}/test_perf_time_{granu_list[0]}_{granu_list[1]}_{join_method.value}_{o_t}.json",
            #     corr_search.perf_profile,
            # )
            # dump_json(
            #     f"evaluation/run_time/{data_source}/test_perf_time_per_tbl_{granu_list[0]}_{granu_list[1]}_{join_method.value}_{o_t}.json",
            #     tbl_perf_profiles,
            # )
joins1 = joins_list[0]
joins2 = joins_list[1]
for x in joins1:
    if x not in joins2:
        print(x)
