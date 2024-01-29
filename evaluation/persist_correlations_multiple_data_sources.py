import psycopg2
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from data_search.search_corr import CorrSearch
import time
from utils.io_utils import dump_json
import utils.io_utils as io_utils
from tqdm import tqdm
from data_search.commons import FIND_JOIN_METHOD
from data_ingestion.profile_datasets import Profiler
from evaluation.persist_correlations import load_lazo_join_res, load_gt_join_res

root_path = "/home/cc/resolution_aware_spatial_temporal_alignment/"

def lazo_open_data(data_sources, granu_list, o_t, r_t, jc_threshold, persist, persist_dir, validate):
    conn_str = 'postgresql://yuegong@localhost/opendata'
    t_granu, s_granu = granu_list[0], granu_list[1]
    jc_joinable_tbls = load_lazo_join_res(data_sources, [0.2], t_granu, s_granu, validate)
   
    corr_search = CorrSearch(
        conn_str,
        data_sources,
        FIND_JOIN_METHOD.COST_MODEL,
        "AGG",
        "MATRIX",
        ["impute_avg", "impute_zero"],
        False,
        "FDR",
        0.05,
        jc_joinable_tbls[jc_threshold],
        mode="lazo",
    )
    corr_search.set_join_cost(t_granu, s_granu, o_t)
    start = time.time()
    if persist:
        dir_path = f'{root_path}/evaluation/{persist_dir[0]}/lazo_jc_{jc_threshold}_{r_t}_{validate}/{"_".join(data_sources)}_{granu_list[0]}_{granu_list[1]}/'
    else:
        dir_path = None
    corr_search.find_all_corr_for_all_tbls(
        granu_list, o_t=o_t, r_t=r_t, p_t=0.05, fill_zero=True, dir_path=dir_path
    )
   
    total_time = time.time() - start
    print("total time:", total_time)
    corr_search.perf_profile["total_time"] = total_time
    # corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
    dump_json(
            f"{root_path}/evaluation/{persist_dir[1]}/{'_'.join(data_sources)}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_lazo_jc_{jc_threshold}_{o_t}_{validate}.json",
            corr_search.perf_profile,
    )

def nexus_open_data(data_sources, granu_list, o_t, r_t, find_join_method, correction, persist, persist_dir, mode=None):
    conn_str = 'postgresql://yuegong@localhost/opendata_large'
    t_granu, s_granu = granu_list[0], granu_list[1]
    corr_search = CorrSearch(
        conn_str,
        data_sources,
        find_join_method,
        "AGG",
        "MATRIX",
        ["impute_avg", "impute_zero"],
        False,
        correction,
        0.05,
    )
    corr_search.set_join_cost(t_granu, s_granu, o_t)
    start = time.time()
    if persist:
        if mode == "time_sampling":
            dir_path = f'{root_path}/evaluation/{persist_dir[0]}/nexus_time_sampling/{"_".join(data_sources)}_{granu_list[0]}_{granu_list[1]}/'
        else:
            if len(data_sources) == 10:
                dir_path = f'{root_path}/evaluation/{persist_dir[0]}/nexus_{r_t}/all_sources_{granu_list[0]}_{granu_list[1]}/'
            else:
                dir_path = f'{root_path}/evaluation/{persist_dir[0]}/nexus_{r_t}/{"_".join(data_sources)}_{granu_list[0]}_{granu_list[1]}/'
    else:
        dir_path = None
    corr_search.find_all_corr_for_all_tbls(
        granu_list, o_t=o_t, r_t=r_t, p_t=0.05, fill_zero=True, dir_path=dir_path
    )
    total_time = time.time() - start
    print("total time:", total_time)
    corr_search.perf_profile["total_time"] = total_time
    corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
    if mode == 'time_sampling':
        runtime_profile_path = f"{root_path}/evaluation/{persist_dir[1]}/{'_'.join(data_sources)}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{find_join_method}_{o_t}_time_sampling.json"
    else:
        if len(data_sources) == 10:
            runtime_profile_path = f"{root_path}/evaluation/{persist_dir[1]}/all_sources/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{find_join_method}_{o_t}_{r_t}.json"
        else:
            runtime_profile_path = f"{root_path}/evaluation/{persist_dir[1]}/{'_'.join(data_sources)}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{find_join_method}_{o_t}_{r_t}.json"
    dump_json(
            runtime_profile_path,
            corr_search.perf_profile,
    )

if __name__ == "__main__":
    # data_sources = ['nyc_open_data', 'chicago_open_data']
    data_sources = [
                    'ny_open_data', 'ct_open_data', 'maryland_open_data', 'pa_open_data',
                    'texas_open_data', 'wa_open_data', 'sf_open_data', 'la_open_data', 
                    'nyc_open_data', 'chicago_open_data'
    ]
    granu_lists = [[T_GRANU.MONTH, S_GRANU.TRACT]]
    o_t, r_t = 10, 0.0
    jc_threshold_l = [0.2]
    correction = 'FDR'
    persist = True
    # validate = False
    persist_dir = ["correlations12_29", "runtime12_29"]
    for granu_list in granu_lists:
        nexus_open_data(data_sources, granu_list, o_t, r_t, FIND_JOIN_METHOD.COST_MODEL, correction, persist, persist_dir)
        # for jc_threshold in jc_threshold_l:
        #     for validate in [True, False]:
        #         lazo_open_data(data_sources, granu_list, o_t, r_t, jc_threshold, persist, persist_dir, validate)