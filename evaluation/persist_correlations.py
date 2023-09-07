from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from data_search.search_corr import CorrSearch
import time
from utils.io_utils import dump_json
import utils.io_utils as io_utils
from tqdm import tqdm
from data_search.commons import FIND_JOIN_METHOD
from data_ingestion.profile_datasets import Profiler

root_path = "/home/cc/resolution_aware_spatial_temporal_alignment/"

def chicago():
    data_source = "chicago_1m"
    config = io_utils.load_config(data_source)
    conn_str = config["db_path"]
    # granu_lists = [[T_GRANU.MONTH, S_GRANU.TRACT], [T_GRANU.DAY, S_GRANU.BLOCK]]
    granu_lists = [[T_GRANU.DAY, S_GRANU.TRACT], [T_GRANU.MONTH, S_GRANU.BLOCK]]
    o_t = 10
    for granu_list in granu_lists:
        profiler = Profiler(data_source, granu_list[0], granu_list[1])
        join_costs = profiler.get_join_cost(granu_list[0], granu_list[1], o_t)
        corr_search = CorrSearch(
            conn_str,
            data_source,
            FIND_JOIN_METHOD.COST_MODEL,
            join_costs,
            "AGG",
            "MATRIX",
            ["impute_avg", "impute_zero"],
            False,
            "FDR",
            0.05,
        )

        start = time.time()
        dir_path = f'{root_path}/evaluation/correlations2/{data_source}_{granu_list[0]}_{granu_list[1]}/'
        corr_search.find_all_corr_for_all_tbls(
            granu_list, o_t=o_t, r_t=0.6, p_t=0.05, fill_zero=True, dir_path=dir_path
        )
        total_time = time.time() - start
        print("total time:", total_time)
        corr_search.perf_profile["total_time"] = total_time
        corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
        dump_json(
                f"{root_path}/evaluation/run_time/{data_source}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{FIND_JOIN_METHOD.COST_MODEL}_{o_t}.json",
                corr_search.perf_profile,
            )

if __name__ == "__main__":
    chicago()