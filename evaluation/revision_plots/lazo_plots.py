from matplotlib import pyplot as plt, ticker
from data_ingestion.data_profiler import Profiler
from utils import io_utils
from utils.spatial_hierarchy import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
import matplotlib as mpl
import pandas as pd
from evaluation.plot_lib.plot_utils import Stages, load_data, grouped_bar_plot
from evaluation.revision_plots.lazo_plot_utils import plot_clustered_stacked
from evaluation.persist_correlations import load_lazo_join_res

mpl.rcParams['font.family'] = 'Times New Roman'
mpl.rcParams['font.size'] = 25

ext = 'pdf'
"""
Figure for join discovery run time
"""
def lazo_joinable_run_time(data_source, t_granu, s_granu):
    lazo_thresholds = [0.0, 0.2, 0.4, 0.6]
    jc_values = ["JC=0", "JC=0.2", "JC=0.4", "JC=0.6"]
    filter_time = []
    validate_time = []
    total_time = []
    for t in lazo_thresholds:
        path = f"lazo_eval/lazo_join_res/{data_source}/time_{t_granu.value}_space_{s_granu.value}/jc_{t}_perf.json"
        res = io_utils.load_json(path)
        filter_time.append(res["filterTime"]/1000)
        validate_time.append(res["validateTime"]/1000)
        total_time.append(res['TotalTime']/1000)
    
    df = pd.DataFrame({'Filter Time': filter_time, 'Validate Time': validate_time}, index=jc_values)
    # # Create a bar plot for filter times
    # ax.bar(jc_values, filter_time, label='Filter Time', width=0.5)

    # # Stack validate times on top of filter times
    # ax.bar(jc_values, validate_time, bottom=filter_time, label='Validate Time', width=0.5)
    ax = df.plot.bar(stacked=True, rot=0, figsize=(10, 6), edgecolor='black')
    print(df)

    data_num = range(len(ax.patches))
    idx_num = len(jc_values)
    for i, bar in enumerate(ax.patches):
        print(i, bar.get_height(), bar.get_y())
        ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height()/2+ bar.get_y()+2,
            round(bar.get_height(), 1), ha = 'center',
            color = 'w', weight = 'bold')
        
    for i in data_num[-idx_num:]:
        print(i)
        bar = ax.patches[i]
        ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + bar.get_y()+3,
            round(total_time[i-(len(data_num)-idx_num)], 1), ha = 'center',
            color = 'black', weight = 'bold')
    # nexus_join_only_profile = io_utils.load_json(f"evaluation/runtime12_29/chicago_1m/full_tables/perf_time_{t_granu}_{s_granu}_FIND_JOIN_METHOD.COST_MODEL_10_find_join_only.json")
    # nexus_run_time = round(nexus_join_only_profile['time_find_joins']['total'], 1)
    nexus_run_time = 152.6
    ax.hlines(y=nexus_run_time, xmin=-10, xmax=100, color='red', linestyle='--', label='Nexus')
    ax.text(1.5, nexus_run_time + 10, f'{nexus_run_time}', fontsize=20, color='red')
    ax.set_ylim(0, 250)
    plt.xlabel('Jaccard Containment Threshold')
    plt.ylabel('Runtime(s)')
  
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.22), ncol=3)
    
    for text_obj in ax.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        text_obj.set_fontsize(30)
    
    # plt.tight_layout()
    plt.savefig(f'evaluation/final_plots/jc_runtime_{t_granu}_{s_granu}.{ext}', bbox_inches="tight")

"""
Plot Recall
"""
def join_discovery_recall(data_sources, o_t, jc_values, validate):
    # load data
    data = {}
    for i, jc_value in enumerate(jc_values):
        recall_l = []
        for category in ["temporal", "spatial", "st"]:
            gt = io_utils.load_json(f"lazo_eval/ground_truth_join/{'_'.join(data_sources)}/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json")
            if validate:
                lazo = io_utils.load_json(f"lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{t_granu.value}_space_{s_granu.value}/{category}_joinable_jc_{str(jc_value)}.json")
            else:
                lazo = io_utils.load_json(f"lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{t_granu.value}_space_{s_granu.value}/{category}_joinable_jc_{str(jc_value)}_validate_false.json")
            for join_key in lazo.keys():
                join_key, precision, recall, f_score = calculate_precision_recall(join_key, gt, lazo, o_t, validate=validate)
                recall_l.append(recall)
        data['JC={}'.format(jc_value)] = recall_l
    df = pd.DataFrame(data)
    ax = df.plot.box( rot=0, figsize=(8, 6), whiskerprops=dict(linewidth=2, color='blue'), boxprops=dict(linewidth=2, color='blue'), flierprops=dict(marker='o', linewidth=2),
             medianprops=dict(linestyle='-', linewidth=2, color='red'),  capprops=dict(linestyle='-', linewidth=2, color='blue'))
    plt.xlabel('Jaccard Containment Threshold')
    plt.ylabel('Recall')

    for text_obj in ax.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        text_obj.set_fontsize(30)
    # plt.tight_layout()
    plt.savefig(f'evaluation/final_plots/jc_recall_{t_granu}_{s_granu}_o_t_{o_t}_validate_{validate}.{ext}', bbox_inches="tight")

"""

"""
def e2e_runtime_stacked(data_sources, t_granu, s_granu):
    storage_dir = "runtime12_29"
    o_t = 10
    validate = "True"

    # group = []
    # labels = []
    filter_time_lazo = []
    validate_time_lazo = []
    mater_part1_lazo = []
    mater_miss_lazo = []
    correlation_lazo = []
    total_lazo = []
    other_lazo = []

    filter_time_nexus = []
    validate_time_nexus = []
    mater_part1_nexus = []
    mater_miss_nexus = []
    correlation_nexus = []
    total_nexus = []
    other_nexus = []


    jc_threshold_l = [0.0, 0.2, 0.4, 0.6]
    for jc_threshold in jc_threshold_l:
        # group.append(f"jc={jc_threshold}")
        # labels.append(f"Lazo pipeline")
        lazo_perf_path = f"evaluation/{storage_dir}/{'_'.join(data_sources)}/full_tables/perf_time_{t_granu}_{s_granu}_lazo_jc_{jc_threshold}_{o_t}.json"
        total_time, filter, vali, mater_lazo, corr = load_data(lazo_perf_path, [Stages.TOTAL, Stages.FILTER, Stages.VALIDATION, Stages.MATERIALIZATION, Stages.CORRELATION], mode='lazo', granu_list=[t_granu, s_granu], jc_t=jc_threshold, data_sources=data_sources, validate=validate)
        total_lazo.append(total_time)
        filter_time_lazo.append(filter)
        validate_time_lazo.append(vali)
        mater_part1_lazo.append(mater_lazo)
        mater_miss_lazo.append(0)
        correlation_lazo.append(corr)
        other_lazo.append(total_time - filter - vali - mater_lazo - corr)
        print(total_time, filter, vali, mater_lazo, corr, total_time - filter - vali - mater_lazo - corr)

        # group.append(f"jc={jc_threshold}")
        # labels.append(f"Nexus")
        nexus_perf_path = f"evaluation/{storage_dir}/{'_'.join(data_sources)}/full_tables/perf_time_{t_granu}_{s_granu}_FIND_JOIN_METHOD.COST_MODEL_{o_t}_0.0.json"
        total_time, filter, mater, corr = load_data(nexus_perf_path, [Stages.TOTAL, Stages.FIND_JOIN, Stages.MATERIALIZATION, Stages.CORRELATION])
        nexus_perf_path2 = f"evaluation/{storage_dir}/{'_'.join(data_sources)}/full_tables/perf_time_{t_granu}_{s_granu}_FIND_JOIN_METHOD.INDEX_SEARCH_{o_t}_0.0.json"
        mater_gt = load_data(nexus_perf_path2, [Stages.MATERIALIZATION])[0]
        total_nexus.append(total_time)
        filter_time_nexus.append(filter)
        validate_time_nexus.append(0)
        mater_part1_nexus.append(mater - (mater_gt - mater_lazo))
        mater_miss_nexus.append(mater_gt-mater_lazo) 
        correlation_nexus.append(corr)
        other_nexus.append(total_time - filter - mater - corr)
    df_lazo = pd.DataFrame({'Filter': filter_time_lazo, 'Validate': validate_time_lazo, 'Materialize': mater_part1_lazo, 'Materialize\njoins missed\nby Lazo': mater_miss_lazo, 'Correlation': correlation_lazo}, index=["JC=0.0", "JC=0.2", "JC=0.4", "JC=0.6"])
    df_nexus = pd.DataFrame({'Filter': filter_time_nexus, 'Validate': validate_time_nexus, 'Materialize': mater_part1_nexus, 'Materialize\njoins missed\nby Lazo': mater_miss_nexus, 'Correlation': correlation_nexus}, index=["JC=0.0", "JC=0.2", "JC=0.4", "JC=0.6"])
    
    ax = plot_clustered_stacked([df_lazo, df_nexus], ['CorrLazo', 'Nexus'])

    # annotate the bars
    for i, bar in enumerate(ax.patches):
        if bar.get_height() == 0:
            continue
        print(i, bar.get_height(), bar.get_height()+ bar.get_y())
        ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height()/2+ bar.get_y()+2,
            round(bar.get_height(), 1), ha = 'center',
            color = 'w', weight = 'bold')
    
    data_num = range(len(ax.patches)-2)
    idx_num = len(jc_threshold_l)*2
   
    ## annotate total time lazo
    for i in range(16, 20):
        bar = ax.patches[i]
        ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + bar.get_y()+3,
            round(total_lazo[i-16], 1), ha = 'center',
            color = 'black', weight = 'bold')
    ## annotate total time nexus
    for i in range(36, 40):
        bar = ax.patches[i]
        ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + bar.get_y()+3,
            round(total_nexus[i-36], 1), ha = 'center',
            color = 'black', weight = 'bold')


    for text_obj in ax.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        text_obj.set_fontsize(32)
    
            
    ax.set_ylabel('Runtime(s)', fontsize=36)
    ax.set_xticklabels(['JC=0.0', 'JC=0.2', 'JC=0.4', 'JC=0.6'], fontsize=36)
    plt.savefig(f'evaluation/final_plots/lazo_e2e.{ext}', bbox_inches="tight")

    # print(df)
  
def calculate_precision_recall(join_key, gt, lazo, o_t, fn_p=False, validate=False):
    if join_key not in gt:
        gt_list = [] 
    else:
        gt_list = gt[join_key]
        gt_list = [x[0] for x in gt_list if x[1]>=o_t]
    # print(gt_list)
    lazo_list = lazo[join_key]
    if validate:
        lazo_list = [x['l'] for x in lazo_list if x['r']>=o_t]
    else:
        lazo_list = [x['l'] for x in lazo_list]
    # print(lazo_list)
    TP = len(set(gt_list).intersection(set(lazo_list)))
    FP = len(set(lazo_list).difference(set(gt_list)))
    if FP != 0:
        print(join_key)
        print(set(lazo_list).difference(set(gt_list)))
    FN_elements = set(gt_list).difference(set(lazo_list))
    if fn_p:
        return TP+FP, FN_elements
    FN = len(set(gt_list).difference(set(lazo_list)))
    # print(TP, FP, FN)
    if TP + FP == 0:
        precision = None
    else:
        precision = TP / (TP + FP)
    if TP+FN == 0:
        recall = None
    else:
        recall = TP / (TP + FN)
    if precision is None and recall is None:
        f_score = None
    elif precision == 0 or recall == 0:
        f_score = 0
    else:
        f_score = 2 * precision * recall / (precision + recall)
    # print(join_key, round(precision,2) if precision else None, round(recall,2) if recall else None, round(f_score, 2) if f_score else None)
    return join_key, round(precision,2) if precision is not None else None, round(recall,2) if recall is not None else None, round(f_score, 2) if f_score is not None else None

"""
Plot the distribution of false negative joins
"""
def find_dist_false_nagetive_joins(t_granu, s_granu):
    o_t = 10
    joinable_dict = load_lazo_join_res(['chicago_1m'], [0.2], t_granu, s_granu, validate=True)
    lookup = joinable_dict[0.2]
    lazo_pairs = set()
    t_granu, s_granu = TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK
    profiler = Profiler('chicago_1m', t_granu, s_granu)
    join_costs = profiler._get_join_cost(t_granu, s_granu, o_t)
    fn_join_costs = []
    for k, v in lookup.items():
        for c in v:
            if c[1]>=o_t and c[0][:9] != k[:9]:
                # pairs.add((k, c[0]))
                lazo_pairs.add((min(k, c[0]), max(k, c[0]), max(join_costs[k].cnt, join_costs[c[0]].cnt)))
    
    nexus_pairs = set()
    gt_join_costs = []
    gt = io_utils.load_json(f"lazo_eval/ground_truth_join/chicago_1m/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json")
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
    # df = pd.DataFrame({'All Joins': gt_join_costs, 'Joins Missed by Lazo': fn_join_costs})
    # ax = df.plot.box(rot=0, figsize=(8, 6))
    fig, axs = plt.subplots(1, 1, figsize=(6, 6))
    axs.boxplot([gt_join_costs, fn_join_costs], whiskerprops=dict(linewidth=2, color='blue'), boxprops=dict(linewidth=2, color='blue'), flierprops=dict(marker='o', linewidth=2),
             medianprops=dict(linestyle='-', linewidth=2, color='red'),  capprops=dict(linestyle='-', linewidth=2, color='blue'))
    plt.ylabel('Larger table size in a join')
    axs.set_xticks([1, 2], ['All Joins', 'Joins Missed\nby Lazo'])
    # Function to format the tick labels
    def format_func(value, tick_number):
        return f'{int(value/1000)}k' if value > 0 else '0'

    # Set custom formatter for the y-axis
    axs.yaxis.set_major_formatter(ticker.FuncFormatter(format_func))

    for text_obj in axs.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        text_obj.set_fontsize(30)
    # plt.tight_layout()
    plt.savefig(f'evaluation/final_plots/fn_join_dist_{t_granu}_{s_granu}_o_t_{o_t}.{ext}', bbox_inches="tight")

    # fig, axs = plt.subplots(1, 1, figsize=(15, 5))
    # box_plot(axs, [gt_join_costs, fn_join_costs])
    # plt.hist(gt_join_costs, bins=50, alpha=0.5, label='gt')
    # plt.hist(fn_join_costs, bins=50, alpha=0.5, label='fn')
    # plt.savefig('lazo_eval/figure/fn_joins.png')

if __name__ == "__main__":
    t_granu, s_granu = TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK
    # t_granu, s_granu = T_GRANU.MONTH, S_GRANU.TRACT
    lazo_joinable_run_time('chicago_1m', t_granu, s_granu)
    join_discovery_recall(['chicago_1m'], 10, [0.0, 0.2, 0.4, 0.6], True)
    e2e_runtime_stacked(['chicago_1m'], t_granu, s_granu)
    find_dist_false_nagetive_joins(t_granu, s_granu)