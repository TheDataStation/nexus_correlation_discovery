import os
import pandas as pd
from graph.graph_utils import remove_bad_cols
from utils.time_point import TEMPORAL_GRANU
from utils.spatial_hierarchy import SPATIAL_GRANU
from nexus.utils.io_utils import dump_json
stop_words = ["wind_direction", "heading", "dig_ticket_", "uniquekey", "streetnumberto", "streetnumberfrom", "census_block", 
              "stnoto", "stnofrom", "lon", "lat", "northing", "easting", "property_group", "insepctnumber", 'primarykey','beat_',
              "north", "south", "west", "east", "beat_of_occurrence", "lastinspectionnumber", "fax", "latest_dist_res", "majority_dist", "latest_dist",
             "f12", "f13"]

def plot_distribution(dir1, dir2, r_t):
    with_r = True
    all_corrs_nexus = load_all_corrs(dir1, r_t, 'inner', with_r)
    print(len(all_corrs_nexus))
    all_corrs_baseline = load_all_corrs(dir2, r_t, 'inner', with_r)
    # sample 1000 correlations from all_corrs_baseline, which is a dict
    import random
    all_corrs_baseline = random.sample(all_corrs_baseline.items(), 10000)
    est_r, true_r = [], []
    for k, v in all_corrs_baseline:
        if k in all_corrs_nexus:
            est_r.append(v)
            true_r.append(all_corrs_nexus[k])
    # plot pairs
    import matplotlib.pyplot as plt
    fig, axs = plt.subplots(1, 1, figsize=(10, 10))
    axs.scatter(true_r, est_r)
    axs.set_xlabel('Actual Pearson\'s Correlation')
    axs.set_ylabel('Estimated Pearson\'s Correlation')
    plt.savefig('correlation_distribution.png')

def compare_corrs(dir1, dir2, r_t, type=None, output_path=None, polygamy=False):
    # compare the divergence between two sets of correlations.
    # dir1 and dir2 are the directories containing the correlation files.
    profile = {}
    if type is None:
        all_corrs_nexus = load_all_corrs(dir1, r_t, 'inner')
    else:
        all_corrs_nexus = load_all_corrs(dir1, r_t, type)
    print(len(all_corrs_nexus))
    all_corrs_baseline = load_all_corrs(dir2, r_t, 'inner', polygamy=polygamy)
    # calculate the jaccard similarity between the two sets of correlations
    all_corrs_nexus = set(all_corrs_nexus)
    all_corrs_baseline = set(all_corrs_baseline)
    # calculate the precision and recall
    print("number of correlations in nexus:", len(all_corrs_nexus))
    print("number of correlations in baseline:", len(all_corrs_baseline))
    TP = len(all_corrs_nexus.intersection(all_corrs_baseline))
    FN = len(all_corrs_nexus.difference(all_corrs_baseline))
    # wrap false negatives into a csv file
    

    FP = len(all_corrs_baseline.difference(all_corrs_nexus))
    profile['corr_nexus'] = len(all_corrs_nexus)
    profile['corr_baseline'] = len(all_corrs_baseline)
    profile['TP'] = TP
    profile['FN'] = FN
    profile['FP'] = FP
    profile['precision'] = TP / (TP + FP)
    profile['recall'] = TP / (TP + FN)
    dump_json(output_path, profile)
  
def load_all_corrs(dir, r_t, type=None, with_r=False, polygamy=False, st_type=None):
    all_corrs = []
    all_corrs_map = {}
    for filename in os.listdir(dir):
        if st_type == 'time':
            if not ('_2' in filename and '_1' not in filename):
                continue
        if st_type == 'space':
            if not ('_2' not in filename and '_1' in filename):
                continue
        if st_type == 'time_and_space':
            if '_2' in filename and '_1' in filename:
                continue
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

if __name__ == "__main__":
    storage_dir = 'correlations12_29'
    # dump_dir = 'correlation_quality12_30'
    # baseline = 'polygamy'
    # baseline = 'lazo'
    t_granu, s_granu = TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT
    # t_granu, s_granu = T_GRANU.DAY, S_GRANU.BLOCK
    # dir1 = f'/home/cc/nexus_correlation_discovery/evaluation/correlations12_29/nexus_0.0/chicago_1m_{t_granu}_{s_granu}/'
    # # dir2 =  f'/home/cc/nexus_correlation_discovery/evaluation/{storage_dir}/corr_sketch_0.0_256/chicago_1m_{t_granu}_{s_granu}/'
    # # dir2 = f'/home/cc/nexus_correlation_discovery//evaluation/{storage_dir}/data_polygmay/chicago_1m_{t_granu}_{s_granu}/'
    # # dir2 = f'/home/cc/nexus_correlation_discovery/evaluation/correlations12_30/data_polygmay_full_4_new/chicago_1m_T_GRANU.DAY_S_GRANU.BLOCK/'
    # # plot_distribution(dir1, dir2, 0)
    # r_t_l = [0]
    # # all_corrs_baseline = load_all_corrs(dir2, 0, 'inner', polygamy=True, st_type='space')
    # # print(len(all_corrs_baseline))
    # for r_t in r_t_l:
    #     # compare_corrs(dir1, dir2, r_t, type=None, output_path=f'evaluation/{dump_dir}/correlation_comparison__{r_t}_{baseline}_{t_granu}_{s_granu}_all.json')
    #     for jc in [0]:
    #         dir2 = f'/home/cc/nexus_correlation_discovery/evaluation/{storage_dir}/lazo_jc_{jc}_{r_t}/chicago_1m_{t_granu}_{s_granu}/'
    #         compare_corrs(dir1, dir2, r_t, type=None)
        # for type in ['inner', 'impute_zero', 'impute_avg']:
        #     # print(f"r_t: {r_t}; jc: {jc}")
        #     print(f"r_t: {r_t}; type: {type}")
        #     # compare_corrs(dir1, dir2, r_t, type=None, output_path=f'evaluation/{dump_dir}/correlation_comparison_jc_{jc}_{r_t}_{baseline}_{t_granu}_{s_granu}.json')
        #     compare_corrs(dir1, dir2, r_t, type=type, output_path=f'evaluation/{dump_dir}/correlation_comparison_{type}_{r_t}_{baseline}_{t_granu}_{s_granu}.json')
    
   
    dir1 = f"evaluation/correlations12_29/nexus_0.0/chicago_1m_{t_granu}_{s_granu}/"
    # dir2 = f"evaluation/correlations12_29/corr_sketch_jc_0.2_0.0_256/chicago_1m_{t_granu}_{s_granu}/"
    dir2 = f"evaluation/correlations12_29/data_polygmay_full_4_new/chicago_1m_T_GRANU.MONTH_S_GRANU.TRACT/"
    compare_corrs(dir1, dir2, 0, type=None, output_path=f"evaluation/correlation_quality12_29/correlation_comparison_nexus_poly_{t_granu}_{s_granu}.json", polygamy=True)