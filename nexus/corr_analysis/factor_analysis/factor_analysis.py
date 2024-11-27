from nexus.corr_analysis.graph.graph_utils import build_graph_with_labels_on_vars, filter_on_signals
import networkx as nx
from nexus.utils.io_utils import load_corrs_from_dir
from factor_analyzer import FactorAnalyzer
import pickle
import pandas as pd
from collections import defaultdict

def corr_matrix_from_corrs(corrs, corr_map):
    G = build_graph_with_labels_on_vars(corrs, weighted=True, index='name')
    all_nodes = list(G.nodes())
    df = nx.to_pandas_adjacency(G, nodelist=all_nodes, dtype=float)
    for node in all_nodes:
        df[node][node] = 1
    for i in range(len(all_nodes)):
        node_i = all_nodes[i]
        for j in range(i, len(all_nodes)):
            node_j = all_nodes[j]
            if node_i == node_j:
                df[node_i][node_j] = 1
            if df.iloc[i, j] == 0:
                key = tuple(sorted([node_i, node_j]))
                if key in corr_map:
                    df.iloc[i, j] = corr_map[key]
                    df.iloc[j, i] = corr_map[key]
    return df

def factor_analysis(corrs, corrs_map, n_factors=3, threshold=0.5, save_path=None):
    df = corr_matrix_from_corrs(corrs, corrs_map) # convert a list of correlations to a correlation matrix
    print(len(df.columns))
    print(save_path)
    fa = FactorAnalyzer(n_factors, rotation=None, is_corr_matrix=True)
    fa.fit(df)
    if save_path:
        pickle.dump(fa, open(save_path, 'wb'))
    loadings_df = pd.DataFrame(fa.loadings_, index=df.columns,columns=['Factor{}'.format(i+1) for i in range(n_factors)])
    clusters = {}
    for i in range(10):
        clusters[i] = loadings_df[loadings_df[f'Factor{i+1}'] >= threshold].index
    clusters = get_clusters(clusters)
    return fa, clusters

def build_factor_clusters(fa, corrs, corrs_map, n_factors, threshold=0.5):
    df = corr_matrix_from_corrs(corrs, corrs_map)
    loadings_df = pd.DataFrame(fa.loadings_, index=df.columns, columns=['Factor{}'.format(i+1) for i in range(n_factors)])
    clusters = {}
    covered_vars = 0
    for i in range(10):
        vars_to_add = loadings_df[loadings_df[f'Factor{i+1}'] >= threshold].index
        covered_vars += len(vars_to_add)
        clusters[i] = vars_to_add
    clusters = get_clusters(clusters)
    return clusters, covered_vars

def get_clusters(factor_clusters):
    all_communities = {}
    for i, comp in factor_clusters.items():
        community = defaultdict(list)
        for tbl_var in comp:
            x = tbl_var.split("--")
            tbl, var = x[0], x[1]
            community[tbl].append(var)
        all_communities[f"Cluster {i}"] = community
    return all_communities

if __name__ == "__main__":
    corrs, corr_map = load_corrs_from_dir('/Users/yuegong/chicago_1m_T_GRANU.MONTH_S_GRANU.TRACT/')
    print(len(corrs))
    # corrs = filter_on_signals(corrs, None, [1.0, 1.0, 1.0, 0.8, 0.6, 70])
    # corr_matrix_from_corrs(corrs, corr_map)