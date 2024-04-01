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

root_path = "/home/cc/resolution_aware_spatial_temporal_alignment/"

def load_lazo_join_res(data_sources, jc_l, t_granu, s_granu, validate=True):
    jc_joinable_tbls = {}
    for jc in jc_l:
        jc_joinable_tbls[jc] = {}
        for category in ["spatial", "temporal", "st"]:
            if validate:
                path = f"evaluation/lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{t_granu.value}_space_{s_granu.value}/{category}_joinable_jc_{str(jc)}.json"
            else:
                path = f"evaluation/lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{t_granu.value}_space_{s_granu.value}/{category}_joinable_jc_{str(jc)}_validate_false.json"
            joinable_tbls = io_utils.load_json(path)
            for join_key, candidates in joinable_tbls.items():
                jc_joinable_tbls[jc][join_key] = [(x["l"], x["r"]) for x in candidates]
    return jc_joinable_tbls

def load_gt_join_res(t_granu, s_granu):
    path = f"lazo_eval/ground_truth_join/chicago_1m/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json"
    return io_utils.load_json(path)

def lazo_corr_sketch(data_sources, granu_list, o_t, r_t, jc_threshold, sketch_size, persist, persist_dir):
    conn_str = 'postgresql://yuegong@localhost/chicago_1m_new'
    t_granu, s_granu = granu_list[0], granu_list[1]
    
    jc_joinable_tbls = load_lazo_join_res(data_sources, [0.2], granu_list[0], granu_list[1], validate=False)
   
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
        mode = 'sketch',
        sketch_size=sketch_size
    )
    corr_search.set_join_cost(t_granu, s_granu, o_t)
    start = time.time()
    if persist:
        dir_path = f'{root_path}/evaluation/{persist_dir[0]}/corr_sketch_jc_{jc_threshold}_{r_t}_{sketch_size}/{"_".join(data_sources)}_{granu_list[0]}_{granu_list[1]}/'
    else:
        dir_path = None

    corr_search.find_all_corr_for_all_tbls(
        granu_list, o_t=o_t, r_t=r_t, p_t=0.05, fill_zero=True, dir_path=dir_path
    )
    total_time = time.time() - start
    print("total time:", total_time)
    corr_search.perf_profile["total_time"] = total_time
    corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
    dump_json(
            f"{root_path}/evaluation/{persist_dir[1]}/{'_'.join(data_sources)}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{o_t}_{r_t}_jc_{jc_threshold}_{sketch_size}_correlation_sketch.json",
            corr_search.perf_profile,
        )

def lazo(data_source, granu_list, o_t, r_t, jc_threshold, persist, persist_dir):
    config = io_utils.load_config(data_source)
    conn_str = config["db_path"]
    jc_joinable_tbls = load_lazo_join_res([data_source], [0.0, 0.2, 0.4, 0.6], granu_list[0], granu_list[1])
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
        jc_joinable_tbls[jc_threshold],
        mode="lazo",
    )
    start = time.time()
    if persist:
        dir_path = f'{root_path}/evaluation/{persist_dir[0]}/lazo_jc_{jc_threshold}_{r_t}/{data_source}_{granu_list[0]}_{granu_list[1]}/'
    else:
        dir_path = None
    corr_search.find_all_corr_for_all_tbls(
        granu_list, o_t=o_t, r_t=r_t, p_t=0.05, fill_zero=True, dir_path=dir_path
    )
    # print(len(corr_search.joinable_pairs))
    # dump_json('joinable_pairs.json', sorted(corr_search.joinable_pairs))
    total_time = time.time() - start
    print("total time:", total_time)
    corr_search.perf_profile["total_time"] = total_time
    # corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
    dump_json(
            f"{root_path}/evaluation/{persist_dir[1]}/{data_source}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_lazo_jc_{jc_threshold}_{o_t}.json",
            corr_search.perf_profile,
    )

def nexus(data_sources, granu_list, o_t, r_t, find_join_method, correction, persist, persist_dir, mode=None):
    conn_str = 'postgresql://yuegong@localhost/chicago_1m_new'
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
        # jc_joinable_tbls[jc_threshold],
        # mode = 'nexus'
    )

    find_join_only = False
    # corr_search.set_find_join_only(find_join_only)
    corr_search.set_join_cost(t_granu, s_granu, o_t)

    start = time.time()
    if persist:
        if mode == "time_sampling":
            dir_path = f'{root_path}/evaluation/{persist_dir[0]}/nexus_time_sampling/{data_sources}_{granu_list[0]}_{granu_list[1]}/'
        else:
            dir_path = f'{root_path}/evaluation/{persist_dir[0]}/nexus_{r_t}/{data_sources}_{granu_list[0]}_{granu_list[1]}/'
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
    elif find_join_only == True:
        runtime_profile_path = f"{root_path}/evaluation/{persist_dir[1]}/{'_'.join(data_sources)}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{find_join_method}_{o_t}_find_join_only.json"
    else:
        runtime_profile_path = f"{root_path}/evaluation/{persist_dir[1]}/{'_'.join(data_sources)}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{find_join_method}_{o_t}_{r_t}_new.json"
    dump_json(
            runtime_profile_path,
            corr_search.perf_profile,
    )

def polygamy(data_sources, granu_list, o_t, r_t, persist, persist_dir, shuffle_num):
    # config = io_utils.load_config(data_source)
    # conn_str = config["db_path"]
    conn_str = 'postgresql://yuegong@localhost/chicago_1m_new'
    find_join_method = FIND_JOIN_METHOD.COST_MODEL

    joinable_tbls = load_gt_join_res(granu_list[0], granu_list[1])
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
        joinable_tbls,
        mode = 'data_polygamy',
    )
    corr_search.set_join_cost(granu_list[0], granu_list[1], o_t)
    
    corr_search.shuffle_num = shuffle_num
    corr_search.st_shuffle_num = 2
    start = time.time()
    if persist:
        dir_path = f'{root_path}/evaluation/{persist_dir[0]}/data_polygmay_full_{shuffle_num}_new/{"_".join(data_sources)}_{granu_list[0]}_{granu_list[1]}/'
    else:
        dir_path = None

    corr_search.find_all_corr_for_all_tbls(
        granu_list, o_t=o_t, r_t=r_t, p_t=0.05, fill_zero=True, dir_path=dir_path
    )
    total_time = time.time() - start
    print("total time:", total_time)
    corr_search.perf_profile["total_time"] = total_time
    corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
    dump_json(
            f"{root_path}/evaluation/{persist_dir[1]}/{'_'.join(data_sources)}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{o_t}_data_polygamy_full_{shuffle_num}.json",
            corr_search.perf_profile,
    )

def sketch_chicago(data_sources, granu_list, o_t, r_t, sketch_size, correction, persist, persist_dir):
    conn_str = 'postgresql://yuegong@localhost/chicago_1m_new'
    t_granu, s_granu = granu_list[0], granu_list[1]
   
    find_join_method = FIND_JOIN_METHOD.COST_MODEL

    joinable_tbls = load_gt_join_res(t_granu, s_granu)
  
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
        joinable_tbls,
        mode = 'sketch',
        sketch_size=sketch_size
    )
    corr_search.set_join_cost(t_granu, s_granu, o_t)
    start = time.time()
    if persist:
        dir_path = f'{root_path}/evaluation/{persist_dir[0]}/corr_sketch_{r_t}_{sketch_size}/{"_".join(data_sources)}_{granu_list[0]}_{granu_list[1]}/'
    else:
        dir_path = None

    corr_search.find_all_corr_for_all_tbls(
        granu_list, o_t=o_t, r_t=r_t, p_t=0.05, fill_zero=True, dir_path=dir_path
    )
    total_time = time.time() - start
    print("total time:", total_time)
    corr_search.perf_profile["total_time"] = total_time
    corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
    dump_json(
            f"{root_path}/evaluation/{persist_dir[1]}/{'_'.join(data_sources)}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{o_t}_{r_t}_{sketch_size}_correlation_sketch.json",
            corr_search.perf_profile,
        )

if __name__ == "__main__":
    data_sources = ["chicago_1m"]
    # data_source = "chicago_1m_time_sampling"
    # data_sources = ['nyc_open_data', 'chicago_open_data']
    # granu_lists = [[T_GRANU.DAY, S_GRANU.BLOCK], [T_GRANU.MONTH, S_GRANU.TRACT]]
    granu_lists = [[T_GRANU.MONTH, S_GRANU.TRACT]]
    o_t, r_t = 10, 0.0
    # jc_threshold = 0.2
    jc_threshold_l = [0.0, 0.2, 0.4, 0.6]
    sketch_size = 256
    correction = "FDR"
    persist = True
    persist_dir = ["correlations12_29", "runtime12_29"]
    shuffle_num = 4
    for granu_list in granu_lists:
        # lazo_corr_sketch(data_sources, granu_list, o_t, r_t, 0.2, sketch_size, persist, persist_dir)
        polygamy(data_sources, granu_list, o_t, r_t, persist, persist_dir, shuffle_num)
        # nexus_open_data(data_sources, granu_list, o_t, r_t, FIND_JOIN_METHOD.COST_MODEL, correction, persist, persist_dir)
        # polygamy(data_source, granu_list, o_t, r_t, persist, persist_dir, shuffle_num)
        # nexus(data_sources, granu_list, o_t, r_t, FIND_JOIN_METHOD.COST_MODEL, correction, persist, persist_dir)
        # nexus(data_sources, granu_list, o_t, r_t, FIND_JOIN_METHOD.INDEX_SEARCH, correction, False, persist_dir)
        # sketch_chicago(data_sources, granu_list, o_t, r_t, sketch_size, correction, persist, persist_dir)
        # for jc_threshold in jc_threshold_l:
        #     lazo_chicago(data_source, granu_list, o_t, r_t, jc_threshold, persist, persist_dir)
    
    # chicago()
    # lazo_chicago()
    # joinable_dict = load_lazo_join_res()
    # lookup = joinable_dict[0.2]
    # pairs = set()
    # for k, v in lookup.items():
    #     for c in v:
    #         if c[1]>=30 and c[0][:9] != k[:9]:
    #             # pairs.add((k, c[0]))
    #             pairs.add((min(k, c[0]), max(k, c[0])))

    # dump_json('joinable_pairs_gt.json', sorted(set(pairs)))
    # lazo_chicago()
    # print(len(pairs))