import pandas as pd
from collections import defaultdict
import os
import scipy.stats
import networkx as nx
import pandas as pd
import pickle
from pyvis.network import Network
from utils.io_utils import load_json, dump_json
from graph.graph_utils import (
    remove_bad_cols,
    Signal,
    load_corr,
    filter_on_a_signal,
    build_graph,
    build_graph_on_vars,
    filter_on_signals,
    get_cov_ratio,
    get_mod_score,
    filter_on_graph_edge_weight,
    build_graph_with_labels_on_vars,
    build_graph_with_labels,
) 
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

def get_clusters(G):
    print(f"number of nodes: {G.number_of_nodes()}; number of edges: {G.number_of_edges()}")
    print(f"clustering coefficient: {nx.average_clustering(G)}")
    print(n)
    print(G.number_of_nodes()/n)
    comps = nx.community.louvain_communities(G)
    print(f"modularity score: {nx.community.modularity(G, comps)}")

    for i, comp in enumerate(comps):
            print(f"==========community {i}=============")
            for tbl in comp:
                print(G.nodes[tbl]["label"])
    return comps

def get_corr_in_a_cluster(df, tbls):
    res = df[(df['tbl_id1'].isin(tbls)) & (df['tbl_id2'].isin(tbls))]
    print(f"number of correlations: {len(res)}")
    # print(res)
    return res

def get_corr_in_cluster_i(corrs, comps, i):
    nodes = comps[i]
    print(nodes)
    get_corr_in_a_cluster(corrs, nodes)

def get_corr_in_a_cluster_vars(df, vars):
#     res = df[(df['tbl_id1'].isin(vars)) & (df['tbl_id2'].isin(vars))]
    res = df[df.apply(lambda row: f'{row.tbl_id1}-{row.agg_attr1[:-3]}' in vars, axis=1)]
    res = res[res.apply(lambda row: f'{row.tbl_id2}-{row.agg_attr2[:-3]}' in vars, axis=1)]
    print(f"number of correlations: {len(res)}")
    print(res)
    return res

def get_corr_in_cluster_i_vars(i, corrs, comps):
    nodes = comps[i]
    print(nodes)
  
    get_corr_in_a_cluster_vars(corrs, nodes)

def case_study_incentive_amounts_victims():
    tbl1 = "etqr-sz5x"
    df1 = pd.read_csv(f"/home/cc/resolution_aware_spatial_temporal_alignment/data/chicago_open_data_1m/{tbl1}.csv")
    df1 = df1[['completion_date', 'incentive_amount']]
    df1['completion_date'] = pd.to_datetime(df1['completion_date'])
    df1 = df1.set_index('completion_date')
    df1 = df1.resample('M').mean()
   
    tbl2 = "gj7a-742p"
    df2 = pd.read_csv(f"/home/cc/resolution_aware_spatial_temporal_alignment/data/chicago_open_data_1m/{tbl2}.csv")
    df2 = df2[['time_period_start', 'number_of_victims']]
    df2['time_period_start'] = pd.to_datetime(df2['time_period_start'])
    df2 = df2.set_index('time_period_start')
    df2 = df2.resample('M').mean()
    df2 = df2.loc[(df2!=0).any(axis=1)]
    print(len(df2))
    merged_df = pd.merge(df1, df2, left_on='completion_date', right_on='time_period_start', right_index=True)
    merged_df = merged_df.dropna()
    # print(merged_df)
    corr, p_value = scipy.stats.pearsonr(merged_df['incentive_amount'], merged_df['number_of_victims'])
    print(corr, p_value)
    # df1 = df1.resample('M').mean()


case_study_incentive_amounts_victims()
# corr_path = "correlations2/chicago_1m_T_GRANU.DAY_S_GRANU.BLOCK/"
# corr_path = "correlations2/chicago_1m_T_GRANU.MONTH_S_GRANU.TRACT/"
# stop_words = ["wind_direction", "heading", "dig_ticket_", "uniquekey", "streetnumberto", "streetnumberfrom", "census_block", 
#               "stnoto", "stnofrom", "lon", "lat", "northing", "easting", "property_group", "insepctnumber", 'primarykey','beat_',
#               "north", "south", "west", "east", "beat_of_occurrence"]
# corrs = load_corr(corr_path)
# corrs = remove_bad_cols(stop_words, corrs)
# n = pd.concat([corrs["tbl_id1"], corrs["tbl_id2"]]).nunique()
# print(len(corrs))
# # # filtered_corr = filter_on_signals(corrs, None, [1.0, 1.0, 0.8, 0.4, 0.6, 70.0])
# # # filtered_corr = filter_on_signals(corrs, None, [1.0, 1.0, 1.0, 0.8, 0.6, 60.0])
# filtered_corr = filter_on_signals(corrs, None, [1.0, 1.0, 1.0, 0.8, 0.6, 80])
# G = build_graph_on_vars(filtered_corr, 0, False)
# print(get_mod_score(G))
# # filtered_corr = filter_on_signals(corrs, None, [1.0, 0.8, 1.0, 0.2, 1.0, 4.0])
# print(len(filtered_corr))
# G = build_graph_with_labels(filtered_corr, 0, True)
# comps = get_clusters(G)
# # print(comps)
# i = 0
# # get_corr_in_cluster_i(filtered_corr, comps, i)