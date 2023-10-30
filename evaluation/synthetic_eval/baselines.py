import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from graph.graph_utils import (
    get_cov_ratio,
    get_mod_score,
    get_average_clustering,
)
from graph.threshold_search import Score
from pgmpy.estimators import PC
from causality.inference.search import IC
from causality.inference.independence_tests import RobustRegressionTest
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score, homogeneity_completeness_v_measure, fowlkes_mallows_score
from scipy.stats import pearsonr

def eval_clusters(n, true_clusters, output_clusters):
    true_labels = create_labels(n, true_clusters)
    pred_labels = create_labels(n, output_clusters)
    # compute metrics
    ari = adjusted_rand_score(true_labels, pred_labels)
    homogeneity, completeness, v_measure = homogeneity_completeness_v_measure(true_labels, pred_labels)
    fm = fowlkes_mallows_score(true_labels, pred_labels)

    metrics = {"ari": ari, "homogeneity": homogeneity, "completeness": completeness, "v_measure": v_measure, "fm": fm}
    return metrics

def create_labels(n, clusters):
    # n is the number of variables
    labels = [-1] * n
    for i, cluster in enumerate(clusters):
        for val in cluster:
            labels[val] = i
    
    # label unclustered variables with distinct increasing numbers
    j = len(clusters)
    for i, label in enumerate(labels):
        if label == -1: # meaning it is not clustered
            labels[i] = j
            j += 1
    
    return labels

def get_corr_and_pvals(df):
    corrs = df.corr()
    pvals = df.corr(method=lambda x, y: pearsonr(x, y)[1]) - np.eye(*corrs.shape)
    pvals = pvals.unstack().dropna()
    corrs = corrs.unstack().dropna()
    return corrs, pvals

"""
Baseline1: Direct clustering on top of the correlation graph
using weighted community detection algorithm
"""
def direct_clustering(corr, threshold=0, weighted: bool=False):
    # compute correlation matrix
    G = nx.Graph()

    for i in range(corr.shape[0]):
        for j in range(i + 1, corr.shape[1]):
            corr_val = abs(corr.iloc[i, j])
            if corr_val >= threshold:
                if weighted:
                    G.add_edge(corr.columns[i], corr.columns[j], weight=corr_val)
                else:
                    G.add_edge(corr.columns[i], corr.columns[j])
    
    comps = nx.community.louvain_communities(G)
    res = []
    for comp in comps:
        res.append(sorted([int(x) for x in comp]))
    return G, res

def find_skyline(points):
    # sort points by the first dimension
    points = sorted(points)
    mono_stack = []
    stack_len = 0
    for point in points:
        while stack_len > 0 and point[1] >= mono_stack[-1][1]:
            mono_stack.pop()
            stack_len -= 1
        mono_stack.append(point)
        stack_len += 1
    return mono_stack

def clustering(corr, cov_t, n, min_t, max_t, step, metric, weighted: bool=False):
    res = {}
    max_score = -1
    max_threshold = 0
    max_comps = None
    for t in np.arange(min_t, max_t, step):
        G, comps = direct_clustering(corr, t, weighted)
        cov_ratio = G.number_of_nodes()/n
        if cov_ratio < cov_t:
            break
        if metric == Score.MODULARITY:
            score = round(get_mod_score(G), 2)
        elif metric == Score.CLUSTER:
            score = round(get_average_clustering(G), 2)
       
        if (cov_ratio, score) not in res:
            res[(cov_ratio, score)] = (t, comps)
        if score > max_score:
            max_score = score
            max_threshold = t
            max_comps = comps

    # print(res)
    # points = list(res.keys())
    # skyline = find_skyline(points)
    # print(f"find {len(skyline)} points")

    # for pt in skyline:
    #     print(f"point: {pt}")
    #     print(f"threshold: {res[pt][0]}")
    #     print(res[pt][1])
    print(f"{max_score}, {max_threshold}, max_comps: {max_comps}")
    return max_threshold, max_score, max_comps


def causal_discovery(df, mode):
    # model_pc = cdt.causality.graph.PC()
    # graph = model_pc.predict(df)

    # # Plot the resulting graph
    # nx.draw_spring(graph, with_labels=True)
    # print("done")
    # plt.savefig('graph_causal.png')
    # independence_test = 'chi_square'
    # significance_level = 0.05

    # # learn the structure
    # pc = PC(df)
    # print(df.corr())
    # model = pc.estimate(independence_test=independence_test, significance_level=significance_level)
    # run the search

    if mode == "IC":
        ic_algorithm = IC(RobustRegressionTest)
        graph = ic_algorithm.search(df, {col: 'c' for col in df.columns})

    elif mode == "PC":
        pc = PC(df)
        graph = pc.estimate(ci_test="pearsonr", max_cond_vars=100, return_type="dag")

    # convert to a networkx graph
    nx_graph = nx.DiGraph()
    nx_graph.add_edges_from(graph.edges())

    comps = nx.community.louvain_communities(nx_graph)
    res = []
    for comp in comps:
        res.append(sorted([int(x) for x in comp]))
    
    # output the graph
    nx.draw_spring(nx_graph, with_labels=True)
    plt.savefig(f'graph_causal_{mode}.png')
    return res
