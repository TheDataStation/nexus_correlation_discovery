import pandas as pd
from collections import defaultdict
import os
import networkx as nx
import pandas as pd
import pickle
from pyvis.network import Network
from utils.io_utils import load_json, dump_json
import time


class Signal:
    def __init__(self, name, d, step):
        self.name = name
        self.d = d
        self.step = step


def load_corr(path):
    all_corr = None
    for filename in os.listdir(path):
        if filename.endswith(".csv"):
            df = pd.read_csv(path + filename)
            if all_corr is None:
                all_corr = df
            else:
                all_corr = pd.concat([all_corr, df])
    return all_corr


def build_graph_with_labels(corrs, threshold=0, weighted=False):
    G = nx.Graph()
    total_corr = 0
    covered_corr = 0  # number of corr that are included in the graph
    labels = {}
    grouped = (
        corrs.groupby(["tbl_id1", "tbl_name1", "tbl_id2", "tbl_name2"])
        .size()
        .to_frame(name="count")
        .reset_index()
    )
    for _, row in grouped.iterrows():
        count = int(row["count"])
        total_corr += count
        if count >= threshold:
            tbl_id1, tbl_id2 = row["tbl_id1"], row["tbl_id2"]
            if weighted:
                G.add_edge(tbl_id1, tbl_id2, weight=count)
            else:
                G.add_edge(tbl_id1, tbl_id2)
            if tbl_id1 not in labels:
                labels[tbl_id1] = row["tbl_name1"]
            if tbl_id2 not in labels:
                labels[tbl_id2] = row["tbl_name2"]
            covered_corr += count
    nx.set_node_attributes(G, labels, "label")
    return G


def build_graph(corrs, threshold=0):
    G = nx.Graph()
    total_corr = 0
    start = time.time()
    grouped = (
        corrs.groupby(["tbl_id1", "tbl_id2"])
        .size()
        .to_frame(name="weight")
        .reset_index()
    )
    # print(f"grouping takes {time.time()-start} s")
    start = time.time()
    # for _, row in grouped.iterrows():
    #     count = int(row["count"])
    #     total_corr += count
    #     if count >= threshold:
    #         G.add_edge(row["tbl_id1"], row["tbl_id2"], weight=count)
    G = nx.from_pandas_edgelist(grouped, "tbl_id1", "tbl_id2", ["weight"])
    # print(f"build_graph takes {time.time()-start} s")
    return G


def filter_on_a_signal(corr, signal, t):
    if "missing_ratio" in signal.name or "zero_ratio" in signal.name:
        return corr[
            (corr[signal.name + "1"].values <= t)
            & (corr[signal.name + "2"].values <= t)
        ]
    else:
        if signal.d == 1:
            return corr[corr[signal.name].values >= t]
        else:
            return corr[corr[signal.name].values <= t]


def filter_on_graph_edge_weight(G, threshold):
    new_graph = nx.Graph()
    new_graph.add_edges_from(
        (u, v, attr) for u, v, attr in G.edges(data=True) if attr["weight"] >= threshold
    )
    node_labels = {n: G.nodes[n]["label"] for n in new_graph.nodes()}
    nx.set_node_attributes(new_graph, node_labels, "label")
    return new_graph


def filter_on_signals(corr, signals, ts):
    return corr[
        (corr["missing_ratio1"].values <= ts[0])
        & (corr["zero_ratio1"].values <= ts[1])
        & (corr["missing_ratio2"].values <= ts[0])
        & (corr["zero_ratio2"].values <= ts[1])
        & (corr["missing_ratio_o1"].values <= ts[2])
        & (corr["zero_ratio_o1"].values <= ts[3])
        & (corr["missing_ratio_o2"].values <= ts[2])
        & (corr["zero_ratio_o2"].values <= ts[3])
        & (abs(corr["r_val"]).values >= ts[4])
        & (corr["samples"].values >= ts[5])
    ]
    # signal_t = zip(signals, thresholds)
    # first = True
    # for signal, t in signal_t:
    #     if t == -1:
    #         continue
    #     if first:
    #         df = filter_on_a_signal(corr, signal, t)
    #         first = False
    #     else:
    #         df = filter_on_a_signal(df, signal, t)
    # return df


def get_cov_ratio(corr, n):
    # n is the total number of tables in all correlations
    tbl_num = pd.concat([corr["tbl_id1"], corr["tbl_id2"]]).nunique()
    # print(tbl_num, n)
    # print(tbl_num / n)
    return tbl_num / n


def get_mod_score(G):
    # get modularity score of a graph from a set of correlations
    comps = nx.community.louvain_communities(G)
    return nx.community.modularity(G, comps)


def get_average_clustering(G):
    return nx.average_clustering(G)
