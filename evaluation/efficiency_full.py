from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from data_search.search_corr import CorrSearch
import time
from utils.io_utils import dump_json
import utils.io_utils as io_utils
from tqdm import tqdm
from data_search.commons import FIND_JOIN_METHOD
from data_ingestion.profile_datasets import Profiler
from enum import Enum
 
class Method(Enum):
    BASELINE = 1
    OURS = 2

def run(data_source, method):
    config = io_utils.load_config(data_source)
    conn_str = config["db_path"]
    overlaps = [100]
    input = "full_st_schemas"
    st_schemas = io_utils.load_pickle(f"evaluation/input/{data_source}/{input}.json")
    granu_lists = [[T_GRANU.DAY, S_GRANU.BLOCK]]
    for granu_list in granu_lists:
        profiler = Profiler(data_source, granu_list[0], granu_list[1])
        # sort st_schemas
        sorted_st_schemas = []
        for st_schema in tqdm(st_schemas):
            tbl, schema = st_schema[0], st_schema[1]
            cnt = profiler.get_row_cnt(tbl, schema)
            sorted_st_schemas.append((st_schema, cnt))
        sorted_st_schemas = sorted(sorted_st_schemas, key=lambda x: x[1], reverse=False)
      
        for o_t in overlaps:
            join_costs = profiler.get_join_cost(granu_list[0], granu_list[1], o_t)
            if method == Method.BASELINE:
                join_method = FIND_JOIN_METHOD.INDEX_SEARCH
                corr_search = CorrSearch(
                    conn_str,
                    data_source,
                    join_method,
                    join_costs,
                    "AGG",
                    "FOR_PAIR",
                    ["impute_avg", "impute_zero"],
                    True,
                    "FDR",
                    0.05,
                )
            else:
                join_method = FIND_JOIN_METHOD.COST_MODEL
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
                
            start = time.time()
            for st_schema in tqdm(sorted_st_schemas):
                tbl, schema = st_schema[0]
                agg_name = schema.get_agg_tbl_name(tbl)
                if agg_name not in join_costs:
                    print("skip")
                    continue
                # print(agg_name, join_costs[agg_name].cnt)
            
                method = corr_search.find_all_corr_for_a_tbl_schema(
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
                f"evaluation/run_time/{data_source}/{input}/perf_time_{granu_list[0]}_{granu_list[1]}_{join_method.value}_{o_t}_baseline.json",
                corr_search.perf_profile,
            )

if __name__ == '__main__':
    run('chicago_1m', Method.BASELINE)