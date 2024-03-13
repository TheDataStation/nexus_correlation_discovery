import os
import pandas as pd
from graph.graph_utils import remove_bad_cols
import matplotlib.pyplot as plt
import matplotlib as mpl
from utils import io_utils
from evaluation.plot_lib.plot_utils import Stages, load_data, grouped_bar_plot
from utils.spatial_hierarchy import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU

mpl.rcParams['font.family'] = 'Times New Roman'
mpl.rcParams['font.size'] = 29

stop_words = ["wind_direction", "heading", "dig_ticket_", "uniquekey", "streetnumberto", "streetnumberfrom", "census_block", 
              "stnoto", "stnofrom", "lon", "lat", "northing", "easting", "property_group", "insepctnumber", 'primarykey','beat_',
              "north", "south", "west", "east", "beat_of_occurrence", "lastinspectionnumber", "fax", "latest_dist_res", "majority_dist", "latest_dist",
             "f12", "f13"]

ext = 'png'

def load_all_corrs(dir, r_t, type=None, with_r=False, polygamy=False, st_type=None):
    all_corrs = []
    all_corrs_map = {}
    for filename in os.listdir(dir):
        if filename.endswith(".csv"):
            df = pd.read_csv(dir + filename)
            df = remove_bad_cols(stop_words, df)
            for i in df.index:
                # if type:
                #     if df['align_type'][i] != type:
                #         continue
                if type == 'inner':
                    if not polygamy:
                        v = abs(df['r_val'][i])
                    else:
                        v = abs(df['score'][i])
                        strength = abs(df['strength'][i])
                elif type == 'impute_zero':
                    v = abs(df['r_impute_zero_val'][i])
                elif type == 'impute_avg':
                    v = abs(df['r_impute_avg_val'][i])
                if v >= r_t:
                    if not with_r:
                        if polygamy:
                            if strength >= 0:
                                all_corrs.append((df['align_attrs1'][i], df['agg_attr1'][i], df['align_attrs2'][i], df['agg_attr2'][i]))
                        else:
                            all_corrs.append((df['align_attrs1'][i], df['agg_attr1'][i], df['align_attrs2'][i], df['agg_attr2'][i]))
                    else:
                        all_corrs_map[(df['align_attrs1'][i], df['agg_attr1'][i], df['align_attrs2'][i], df['agg_attr2'][i])] = df['r_val'][i]
    if with_r:
        return all_corrs_map
    else:                   
        return all_corrs
    
def plot_distribution(dir1, dir2, r_t):
    with_r = True
    all_corrs_nexus = load_all_corrs(dir1, r_t, 'inner', with_r)
    print(len(all_corrs_nexus))
    all_corrs_baseline = load_all_corrs(dir2, r_t, 'inner', with_r)
    # sample 1000 correlations from all_corrs_baseline, which is a dict
    import random
    all_corrs_baseline = random.sample(all_corrs_baseline.items(), 40000)
    est_r, true_r = [], []
    for k, v in all_corrs_baseline:
        if v == 0:
            continue
        if k in all_corrs_nexus:
            est_r.append(v)
            true_r.append(all_corrs_nexus[k])
    # persist sampled pairs
    data = {'est_r': est_r, 'true_r': true_r}
    io_utils.dump_json('coef_dist_data.json', data)
    # plot pairs
    fig, axs = plt.subplots(1, 1, figsize=(10, 10))
    axs.scatter(true_r, est_r, s=5)
    axs.set_xlabel('Actual Correlation')
    axs.set_ylabel('Estimated Correlation')

    for text_obj in axs.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        text_obj.set_fontsize(29)

    plt.savefig(f'correlation_distribution_new.{ext}',  bbox_inches="tight")

"""
Create distribution plot
"""
def plot_dist_fast():
    data = io_utils.load_json('coef_dist_data.json')

    est_r, true_r = data['est_r'], data['true_r']
    pairs = list(zip(est_r, true_r))
    # sample 
    import random
    pairs = random.sample(pairs, 10000)
    est_r_new, true_r_new = [pair[0] for pair in pairs], [pair[1] for pair in pairs]
    # plot pairs
    fig, axs = plt.subplots(1, 1, figsize=(10, 7))
    axs.scatter(true_r_new, est_r_new, s=5)
    axs.set_xlabel('Actual Correlation')
    axs.set_ylabel('Estimated Correlation')

    for text_obj in axs.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        text_obj.set_fontsize(30)

    plt.savefig(f'evaluation/final_plots/correlation_distribution_new.{ext}',  bbox_inches="tight")

"""
Run time stacked plot
"""
def plot_run_time():
    t_granu, s_granu = TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK
    find_join_l = []
    materialize_l = []
    correlation_l = []
    total_l = []
   
    # load sketch data
    sketch_path = "evaluation/runtime12_29/chicago_1m/full_tables/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_10_0.0_256_correlation_sketch.json"
    find_join_sketch, mater_sketch, corr_sketch, total_sketch = load_data(sketch_path, vars=[Stages.FIND_JOIN, Stages.MATERIALIZATION, Stages.CORRELATION, Stages.TOTAL], mode='sketch', granu_list=[t_granu, s_granu])
    find_join_l.append(find_join_sketch)
    materialize_l.append(mater_sketch)
    correlation_l.append(corr_sketch)
    total_l.append(total_sketch)

    # load nexus data
    nexus_path = "evaluation/runtime12_29/chicago_1m/full_tables/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_FIND_JOIN_METHOD.COST_MODEL_10_0.0.json"
    find_join, mater, corr, total = load_data(nexus_path, vars=[Stages.FIND_JOIN, Stages.MATERIALIZATION, Stages.CORRELATION, Stages.TOTAL])
    find_join_l.append(find_join)
    materialize_l.append(mater)
    correlation_l.append(corr)
    total_l.append(total)

    df = pd.DataFrame({'Filter': find_join_l, 'Materialize': materialize_l, 'Correlation': correlation_l}, index=["CorrSketch", "Nexus"])
    ax = df.plot.bar(stacked=True, rot=0, figsize=(10, 7), edgecolor='black')
    ax.set_ylabel('Run time(s)')
   
    for i, bar in enumerate(ax.patches):
        print(i, bar.get_height(), bar.get_y())
        ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height()/2.5+ bar.get_y(),
            round(bar.get_height(), 1), ha = 'center',
            color = 'w', weight = 'bold')
        
    for i in range(4,6):
        print(i)
        bar = ax.patches[i]
        ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + bar.get_y()+3,
            round(total_l[i-4], 1), ha = 'center',
            color = 'black', weight = 'bold')

    for text_obj in ax.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        text_obj.set_fontsize(29)
    
    # plt.tight_layout()
    plt.savefig(f'evaluation/final_plots/corr_run_time_{t_granu}_{s_granu}.{ext}', bbox_inches="tight")

if __name__ == '__main__':
    dir1 = 'evaluation/correlations01_10/nexus_0.0/chicago_1m_T_GRANU.DAY_S_GRANU.BLOCK/'
    dir2 = 'evaluation/correlations01_10/corr_sketch_0.0_256/chicago_1m_T_GRANU.DAY_S_GRANU.BLOCK/'
    # plot_distribution(dir1, dir2, 0)
    plot_dist_fast()
    # plot_run_time()