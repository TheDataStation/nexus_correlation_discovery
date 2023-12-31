from data_ingestion.profile_datasets import Profiler
from data_search.commons import FIND_JOIN_METHOD
from evaluation.persist_correlations import load_lazo_join_res
from utils import io_utils
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from evaluation.plot_lib.plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
import matplotlib.pyplot as plt

def box_plot(ax, data_i):
    data = []
    for d in data_i:
        data.append([v for v in d if v is not None])
    ax.boxplot(data)
    ax.set_ylabel('The size of the larger table involved in a join')
    ax.set_xticks([1, 2], ['Ground Truth Joinable Pairs', 'Joinable Pairs Missed by Lazo'])

def lazo_nexus_runtime_comparison_overall(granu_list, jc_threshold_l, o_t, r_t, data_source, storage_dir):
    nexus_perf_path = f"evaluation/{storage_dir}/{data_source}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{FIND_JOIN_METHOD.COST_MODEL}_{o_t}_{r_t}.json"
    nexus_profile = io_utils.load_json(nexus_perf_path)
    nexus_runtime = nexus_profile['total_time']
    lazo_runtime_l = []
    nexus_runtime_l = []
    for jc_threshold in jc_threshold_l:
        lazo_perf_path = f"evaluation/{storage_dir}/{data_source}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_lazo_jc_{jc_threshold}_{o_t}.json"
        lazo_profile = load_data(lazo_perf_path, [Stages.TOTAL], mode='lazo', granu_list=granu_list, jc_t=jc_threshold)
        lazo_runtime_l.append(lazo_profile[0])
        nexus_runtime_l.append(nexus_runtime)
    values = np.array([lazo_runtime_l, nexus_runtime_l])
    params = {
        "ylabel": "Run time(s)",
        # "title": f"Overlap threshold {o_t}",
        "save_path": f"lazo_eval/figure/{data_source}_run_time_comparison_total_time_{r_t}_{o_t}.png",
    }
    fig, axs = plt.subplots(1, 1, sharey=True, figsize=(15, 5))
    grouped_bar_plot(
        axs, ['Lazo', 'Nexus'], [f"JC={jc}" for jc in jc_threshold_l], values, params
    )

def lazo_nexus_runtime_comparison():
    granu_list = [T_GRANU.MONTH, S_GRANU.TRACT]
    jc_threshold, o_t, r_t = 0.2, 30, 0.2
    find_join_method = FIND_JOIN_METHOD.COST_MODEL
    data_source = "chicago_1m"
    lazo_perf_path = f"evaluation/run_time2/{data_source}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_lazo_jc_{jc_threshold}_{o_t}_2.json"
    nexus_perf_path = f"evaluation/run_time2/{data_source}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{find_join_method}_{o_t}_{r_t}_equal_comparison.json"

    vars = [Stages.TOTAL, Stages.FIND_JOIN, Stages.MATERIALIZATION, Stages.CORRELATION, Stages.CORRECTION]
    # vars = [Stages.MATERIALIZATION]
    perf_lazo = load_data(lazo_perf_path, vars, lazo=True, granu_list=granu_list, jc_t=jc_threshold)
    perf_nexus = load_data(nexus_perf_path, vars)
    values = np.array([perf_lazo, perf_nexus])
    fig, axs = plt.subplots(1, 1, sharey=True, figsize=(15, 5))
    # print(values)
    params = {
        "ylabel": "Run time(s)",
        # "title": f"Overlap threshold {o_t}",
        "save_path": f"lazo_eval/figure/{data_source}_run_time_comparison_equal.png",
    }
    # make a bar plot for values using matplotlib
    # print(perf_nexus)
    # plt.bar(['Lazo', 'Ground Truth Join Time'], [perf_lazo[0], perf_nexus[0]])
    # plt.savefig(params["save_path"])
    grouped_bar_plot(
        axs, ['Lazo', 'Nexus'], [var.value for var in vars], values, params
    )


def find_dist_false_nagetive_joins():
    o_t = 30
    joinable_dict = load_lazo_join_res()
    lookup = joinable_dict[0.2]
    lazo_pairs = set()
    t_granu, s_granu = T_GRANU.DAY, S_GRANU.BLOCK
    profiler = Profiler('chicago_1m', t_granu, s_granu)
    join_costs = profiler.get_join_cost(t_granu, s_granu, o_t)
    fn_join_costs = []
    for k, v in lookup.items():
        for c in v:
            if c[1]>=o_t and c[0][:9] != k[:9]:
                # pairs.add((k, c[0]))
                lazo_pairs.add((min(k, c[0]), max(k, c[0]), max(join_costs[k].cnt, join_costs[c[0]].cnt)))
    
    nexus_pairs = set()
    gt_join_costs = []
    gt = io_utils.load_json(f"lazo_eval/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json")
    for k, v in gt.items():
        for c in v:
            if c[1]>=o_t and c[0][:9] != k[:9]:
                # pairs.add((k, c[0]))
                gt_join_costs.append(min(join_costs[k].cnt, join_costs[c[0]].cnt))
                nexus_pairs.add((min(k, c[0]), max(k, c[0]), max(join_costs[k].cnt, join_costs[c[0]].cnt)))
    fn_joins = nexus_pairs.difference(lazo_pairs)
    for fn_join in fn_joins:
        fn_join_costs.append(fn_join[2])

    # plot the distribution of gt_join_costs and fn_join_costs
    import matplotlib.pyplot as plt
    import numpy as np
    fig, axs = plt.subplots(1, 1, figsize=(15, 5))
    box_plot(axs, [gt_join_costs, fn_join_costs])
    # plt.hist(gt_join_costs, bins=50, alpha=0.5, label='gt')
    # plt.hist(fn_join_costs, bins=50, alpha=0.5, label='fn')
    plt.savefig('lazo_eval/figure/fn_joins.png')

if __name__ == "__main__":
    lazo_nexus_runtime_comparison_overall([T_GRANU.MONTH, S_GRANU.TRACT], [0.0, 0.2, 0.4, 0.6], 10, 0.0, 'chicago_1m', 'runtime12_29')
    # lazo_nexus_runtime_comparison()
    # find_dist_false_nagetive_joins()