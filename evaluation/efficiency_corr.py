import utils.io_utils as io_utils
from data_search.search_corr import CorrSearch
import time
from tqdm import tqdm

"""
Find Join method: Index Search
Compare the performances of calculating correlation.
Baseline:  Materializes both the inner join and the outer join results. Then, calculate correlations.
Our method: only need to materialize the inner join result. 
By using the result of inner join and single column stats, 
we can get outer join corr efficiently without the need of doing outer join explicitly.
"""

data_source = "chicago_1m"
config = io_utils.load_config(data_source)
conn_str = config["db_path"]
# load a sample of st schemas
st_schemas = io_utils.load_pickle(f"evaluation/input/{data_source}/100_st_schemas.json")

for is_out_join in [False, True]:
    print(f"outer join: {is_out_join}")
    corr_search = CorrSearch(
        conn_str,
        data_source,
        "FIND_JOIN",
        "AGG",
        "MATRIX",
        ["impute_avg", "impute_zero"],
        is_out_join,
        "FDR",
        0.05,
    )
    o_t, r_t, p_t = 10, 0.6, 0.05
    start = time.time()
    for st_schema in tqdm(st_schemas):
        corr_search.find_all_corr_for_a_tbl_schema(
            st_schema[0], st_schema[1], o_t, r_t, p_t, fill_zero=True
        )
    total_time = time.time() - start
    corr_search.perf_profile["total_time"] = total_time
    print(corr_search.perf_profile)

    io_utils.dump_json(
        f"run_time/{data_source}/perf_time_outer_join_{is_out_join}.json",
        corr_search.perf_profile,
    )
