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


data_source = "chicago_1m"
config = io_utils.load_config(data_source)
conn_str = config["db_path"]
granu_lists = [[T_GRANU.DAY, S_GRANU.BLOCK]]
overlaps = [1000]
st_schemas = io_utils.load_pickle(f"evaluation/input/{data_source}/200_st_schemas.json")

for granu_list in granu_lists:
    for join_method in [
        FIND_JOIN_METHOD.INDEX_SEARCH,
        FIND_JOIN_METHOD.JOIN_ALL,
        FIND_JOIN_METHOD.COST_MODEL,
    ]:
        for o_t in overlaps:
            print(f"current join method: {join_method}; overlap threshold: {o_t}")
            corr_search = CorrSearch(
                conn_str,
                data_source,
                join_method,
                "AGG",
                "MATRIX",
                ["impute_avg", "impute_zero"],
                False,
                "FDR",
                0.05,
            )
            start = time.time()
            for st_schema in tqdm(st_schemas):
                corr_search.find_all_corr_for_a_tbl_schema(
                    st_schema[0],
                    st_schema[1],
                    o_t=o_t,
                    r_t=0.6,
                    p_t=0.05,
                    fill_zero=True,
                )
            # corr_search.find_all_corr_for_all_tbls(
            #     granu_list, o_t=o_t, r_t=0.6, p_t=0.05, fill_zero=True, dir_path=None
            # )
            total_time = time.time() - start
            print("total time:", total_time)
            corr_search.perf_profile["total_time"] = total_time
            print(corr_search.perf_profile)
            print(corr_search.inv_overhead)
            dump_json(
                f"evaluation/run_time/{data_source}/perf_time_{granu_list[0]}_{granu_list[1]}_{join_method.value}_{o_t}.json",
                corr_search.perf_profile,
            )
