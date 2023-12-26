import os
import pandas as pd
from graph.graph_utils import remove_bad_cols

stop_words = ["wind_direction", "heading", "dig_ticket_", "uniquekey", "streetnumberto", "streetnumberfrom", "census_block", 
              "stnoto", "stnofrom", "lon", "lat", "northing", "easting", "property_group", "insepctnumber", 'primarykey','beat_',
              "north", "south", "west", "east", "beat_of_occurrence", "lastinspectionnumber", "fax", "latest_dist_res", "majority_dist", "latest_dist",
             "f12", "f13"]

def compare_corrs(dir1, dir2, r_t, type=None):
    # compare the divergence between two sets of correlations.
    # dir1 and dir2 are the directories containing the correlation files.
    all_corrs_nexus = load_all_corrs(dir1, r_t, type)
    print(len(all_corrs_nexus))
    print(all_corrs_nexus[0:10])
    all_corrs_lazo = load_all_corrs(dir2, r_t, type)
    # calculate the jaccard similarity between the two sets of correlations
    all_corrs_nexus = set(all_corrs_nexus)
    all_corrs_lazo = set(all_corrs_lazo)
    print("number of correlations in nexus:", len(all_corrs_nexus))
    print("number of correlations in lazo:", len(all_corrs_lazo))
    print("number of correlations in both:", len(all_corrs_nexus.intersection(all_corrs_lazo)))
    fn = all_corrs_nexus.difference(all_corrs_lazo)
    print("numebr of correlations in Nexus but not in Lazo", len(fn))
    print("numebr of correlations in Lazo but not in Nexus", len(all_corrs_lazo.difference(all_corrs_nexus)))
    
    # for i in fn:
    #     print(i)
    print("number of correlations in either:", len(all_corrs_nexus.union(all_corrs_lazo)))
    print("jaccard similarity:", len(all_corrs_nexus.intersection(all_corrs_lazo)) / len(all_corrs_nexus.union(all_corrs_lazo)))

def load_all_corrs(dir, r_t, type=None):
    all_corrs = []
    for filename in os.listdir(dir):
        if len(filename) <= 18:
            continue
        if filename.endswith(".csv"):
            df = pd.read_csv(dir + filename)
            df = remove_bad_cols(stop_words, df)
            for i in df.index:
                if type:
                    if df['align_type'][i] != type:
                        continue
                if abs(df['r_val'][i]) >= r_t:
                    all_corrs.append((df['align_attrs1'][i], df['agg_attr1'][i], df['align_attrs2'][i], df['agg_attr2'][i]))
    return all_corrs

if __name__ == "__main__":
    dir1 = '/home/cc/resolution_aware_spatial_temporal_alignment/evaluation/correlations4/nexus/chicago_1m_T_GRANU.DAY_S_GRANU.BLOCK/'
    dir2 =  '/home/cc/resolution_aware_spatial_temporal_alignment/evaluation/correlations4/lazo_jc_0.2/chicago_1m_T_GRANU.DAY_S_GRANU.BLOCK/'
    r_t = 0.6
    compare_corrs(dir1, dir2, r_t)