import pandas as pd
from collections import defaultdict
import os
import networkx as nx
import pandas as pd
import pickle
from pyvis.network import Network
from utils.io_utils import load_json, dump_json
import time


stop_words = ["wind_direction", "heading", "dig_ticket_", "uniquekey", "streetnumberto", "streetnumberfrom", "census_block", 
              "stnoto", "stnofrom", "lon", "lat", "northing", "easting", "property_group", "insepctnumber", 'primarykey','beat_',
              "north", "south", "west", "east", "beat_of_occurrence", "lastinspectionnumber", "fax", "latest_dist_res", "majority_dist", "latest_dist",
             "f12", "f13"]

class Signal:
    def __init__(self, name, d, step):
        self.name = name
        self.d = d
        self.step = step


def load_corr(path):
    all_corr = None
    to_include = ['ijzp-q8t2', '85ca-t3if', 'x2n5-8w5q'] 
    for filename in os.listdir(path):
        if filename.endswith(".csv"):
            df = pd.read_csv(path + filename)
            if all_corr is None:
                all_corr = df
            else:
                all_corr = pd.concat([all_corr, df])
    all_corr = all_corr[~(((all_corr['agg_attr1'] == 'count_t1') & (~all_corr['tbl_id1'].isin(to_include))) | (((all_corr['agg_attr2'] == 'count_t2') & (~all_corr['tbl_id2'].isin(to_include)))))]
    return all_corr

def load_all_corrs(path):
    all_corr = None
    for filename in os.listdir(path):
        if filename.endswith(".csv"):
            df = pd.read_csv(path + filename)
            # remove all bad cols
            df = remove_bad_cols(stop_words, df)
            if all_corr is None:
                all_corr = df
            else:
                all_corr = pd.concat([all_corr, df])
    return all_corr

def remove_bad_cols(stop_words, corrs):
    for stop_word in stop_words:
        corrs = corrs[~((corrs['agg_attr1'] == f'avg_{stop_word}_t1') | (corrs['agg_attr2'] == f'avg_{stop_word}_t2'))]
    return corrs

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


def build_graph(corrs, threshold=0, weighted=False):
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
    if weighted:
        G = nx.from_pandas_edgelist(grouped, "tbl_id1", "tbl_id2", ["weight"])
    else:
        G = nx.from_pandas_edgelist(grouped, "tbl_id1", "tbl_id2")
    # print(f"build_graph takes {time.time()-start} s")
    return G

def build_graph_on_vars(corrs, threshold=0, weighted=False):
    G = nx.Graph()
    total_corr = 0
    covered_corr = 0  # number of corr that are included in the graph

    grouped = (
        corrs.groupby([corrs["tbl_id1"]+corrs["agg_attr1"].str[0:-3], corrs["tbl_id2"]+corrs["agg_attr2"].str[0:-3]])
        .size()
        .to_frame(name="weight")
        .reset_index()
    )

    if weighted:
        G = nx.from_pandas_edgelist(grouped, "level_0", "level_1", ["weight"])
    else:
        G = nx.from_pandas_edgelist(grouped, "level_0", "level_1")

    return G

def build_graph_with_labels_on_vars(corrs, threshold=0, weighted=False):
    G = nx.Graph()
    total_corr = 0
    covered_corr = 0  # number of corr that are included in the graph
    labels = {}
    from collections import defaultdict
    tbl_attrs = defaultdict(set)
    for _, row in corrs.iterrows():
        tbl_id1, tbl_id2, tbl_name1, tbl_name2, agg_attr1, agg_attr2 = row["tbl_id1"], row["tbl_id2"], \
        row["tbl_name1"], row["tbl_name2"], row['agg_attr1'][0:-3], row['agg_attr2'][0:-3]
      
        tbl_id1, tbl_id2 = row["tbl_id1"], row["tbl_id2"]
        G.add_edge(f"{tbl_id1}--{agg_attr1}", f"{tbl_id2}--{agg_attr2}")
        tbl_attrs[tbl_id1].add(agg_attr1)
        tbl_attrs[tbl_id2].add(agg_attr2)
        labels[f"{tbl_id1}--{agg_attr1}"] = f"{tbl_name1}--{agg_attr1}"
        labels[f"{tbl_id2}--{agg_attr2}"] = f"{tbl_name2}--{agg_attr2}"

    # for tbl, attrs in tbl_attrs.items():
    #     attrs = list(attrs)
    #     l = len(attrs)
    #     for i in range(l):
    #         for j in range(i, l):
    #             G.add_edge(f"{tbl}-{attrs[i]}", f"{tbl}-{attrs[j]}")
    nx.set_node_attributes(G, labels, "label")
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