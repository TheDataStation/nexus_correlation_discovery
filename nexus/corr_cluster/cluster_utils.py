import networkx as nx
import pandas as pd
from collections import defaultdict
import random

def get_clusters_fa(factor_clusters):
    all_communities = {}
    for i, comp in enumerate(factor_clusters):
        community = defaultdict(list)
        for tbl_var in comp:
            x = tbl_var.split("--")
            tbl, var = x[0], x[1]
            community[tbl].append(var)
        all_communities[f"Cluster {i}"] = community
    return all_communities

def get_clusters(G):
    # print(
    #     f"number of nodes: {G.number_of_nodes()}; number of edges: {G.number_of_edges()}"
    # )
    # print(f"clustering coefficient: {nx.average_clustering(G)}")
    random.seed(9)
    comps = nx.community.louvain_communities(G)
    all_communities = {}
    for i, comp in enumerate(comps):
        community = defaultdict(list)
        for tbl_var in comp:
            tbl_var = G.nodes[tbl_var]["label"]
            x = tbl_var.split("--")
            tbl, var = x[0], x[1]
            community[tbl].append(var)
        all_communities[f"Cluster {i}"] = community
    return all_communities, comps


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


def get_correlation_communities_chicago(corrs):
    filtered_corr = filter_on_signals(corrs, None, [1.0, 1.0, 1.0, 0.8, 0.6, 80])
    original_tbls = pd.concat([corrs["tbl_id1"], corrs["tbl_id2"]]).nunique()
    covered_tbls = pd.concat(
        [filtered_corr["tbl_id1"], filtered_corr["tbl_id2"]]
    ).nunique()
    print(
        f"covered #tbls: {covered_tbls}, original #tbls: {original_tbls}, coverage ratio: {covered_tbls/original_tbls}"
    )
    print(
        f"covered #correlations: {len(filtered_corr)}, original #correlations: {len(corrs)}"
    )
    G = build_graph_on_vars(filtered_corr, 0, False)
    all_communities, comps = get_clusters(G)
    return all_communities, comps


def build_graph_on_vars(corrs, threshold=0, weighted=False):
    G = nx.Graph()
    labels = {}
    from collections import defaultdict

    tbl_attrs = defaultdict(set)
    for _, row in corrs.iterrows():
        tbl_id1, tbl_id2, tbl_name1, tbl_name2, agg_attr1, agg_attr2 = (
            row["table_id1"],
            row["table_id2"],
            row["table_name1"],
            row["table_name2"],
            row["agg_attr1"],
            row["agg_attr2"],
        )
        G.add_edge(f"{tbl_id1}--{agg_attr1}", f"{tbl_id2}--{agg_attr2}")
        tbl_attrs[tbl_id1].add(agg_attr1)
        tbl_attrs[tbl_id2].add(agg_attr2)
        labels[f"{tbl_id1}--{agg_attr1}"] = f"{tbl_name1}--{agg_attr1}"
        labels[f"{tbl_id2}--{agg_attr2}"] = f"{tbl_name2}--{agg_attr2}"

    nx.set_node_attributes(G, labels, "label")
    return G


# Get variable level graph
def build_graph_on_vars_un(corrs, threshold=0, weighted=False):
    G = nx.Graph()
    labels = {}
    for _, row in corrs.iterrows():

        tbl_id1, tbl_id2, attr1, attr2 = (
            row["tbl1"],
            row["tbl2"],
            row["attr1"],
            row["attr2"],
        )

        # do not add edges between vars in the same table
        if tbl_id1 == tbl_id2:
            continue

        if weighted:
            G.add_edge(
                f"{tbl_id1}--{attr1}", f"{tbl_id2}--{attr2}", weight=row["r_val"]
            )
        else:
            G.add_edge(f"{tbl_id1}--{attr1}", f"{tbl_id2}--{attr2}")

        labels[f"{tbl_id1}--{attr1}"] = f"{tbl_id1}--{attr1}"
        labels[f"{tbl_id2}--{attr2}"] = f"{tbl_id2}--{attr2}"
    nx.set_node_attributes(G, labels, "label")
    return G


"""
Functions to retrieve correlations
"""


class CorrCommunity:
    def __init__(self, corrs, name=None, clusters=None, comps=None):
        self.name = name
        self.corrs = corrs
        self.display_attrs = [
            # "table_id1",
            "table_name1",
            "agg_attr1",
            "description1",
            # "table_id2",
            "table_name2",
            "agg_attr2",
            "description2",
            "correlation coefficient",
            "number of samples",
            "spatio-temporal key type",
        ]
        if self.name == "chicago":
            self.display_attrs = [
                "table_id1",
                "table_name1",
                "align_attrs1",
                "agg_attr1",
                "table_id2",
                "table_name2",
                "align_attrs2",
                "agg_attr2",
                "r_val",
                "samples",
                "align_type",
            ]
        elif self.name == "un":
            self.display_attrs = ["tbl1", "attr1", "tbl2", "attr2", "r_val", "samples"]
        if clusters:
            self.all_communities = clusters
            self.comps = defaultdict(list)
            for cluster, comp in self.all_communities.items():
                for tbl_name, var_list in comp.items():
                    for var_name in var_list:
                        self.comps[cluster].append("{}--{}".format(tbl_name, var_name))
            self.comps = list(self.comps.values())
        self.filtered_corr = corrs
      

    def get_correlation_communities(self, signal_thresholds=None):
        if signal_thresholds:
            self.filtered_corr = filter_on_signals(self.corrs, None, signal_thresholds)
        else:
            self.filtered_corr = self.corrs
        self.G = build_graph_on_vars(self.filtered_corr, 0, False)
        self.all_communities = self.get_communities(self.G)
        print(
            f"modularity score: {nx.community.modularity(self.G, self.comps)}"
        )
       

    def get_correlation_communities_chicago(self, signal_thresholds, show_info=False):
        self.filtered_corr = filter_on_signals(self.corrs, None, signal_thresholds)
        original_tbls = pd.concat(
            [self.corrs["table_id1"], self.corrs["table_id2"]]
        ).nunique()
        covered_tbls = pd.concat(
            [self.filtered_corr["table_id1"], self.filtered_corr["table_id2"]]
        ).nunique()
        self.G = build_graph_on_vars(self.filtered_corr, 0, False)
        self.all_communities = self.get_communities(self.G)
        if show_info:
            print(
                f"covered #tbls: {covered_tbls}, original #tbls: {original_tbls}, coverage ratio: {covered_tbls/original_tbls}, modularity score: {nx.community.modularity(self.G, self.comps)}"
            )
            print(
                f"covered #correlations: {len(self.filtered_corr)}, original #correlations: {len(self.corrs)}"
            )

    def get_correlation_communities_un(self, signal_thresholds):
        self.filtered_corr = self.corrs[
            (abs(self.corrs["r_val"]) >= signal_thresholds[0])
            & (self.corrs["samples"] >= signal_thresholds[1])
        ]
        self.G = build_graph_on_vars_un(self.filtered_corr, 0, False)
        self.all_communities = self.get_communities(self.G)
        original_tbls = pd.concat([self.corrs["tbl1"], self.corrs["tbl2"]]).nunique()
        covered_tbls = pd.concat(
            [self.filtered_corr["tbl1"], self.filtered_corr["tbl2"]]
        ).nunique()

        print(
            f"covered #tbls: {covered_tbls}, original #tbls: {original_tbls}, coverage ratio: {covered_tbls/original_tbls}, modularity score: {nx.community.modularity(self.G, self.comps)}"
        )
        print(
            f"covered #correlations: {len(self.filtered_corr)}, original #correlations: {len(self.corrs)}"
        )

    def get_communities(self, G):
        random.seed(10)
        # sort components by the number of variables in the cluster
        self.comps = nx.community.louvain_communities(G)
        tmp = []
        for i, comp in enumerate(self.comps):
            tmp.append(
                (comp, len(comp))
            )
        tmp = sorted(tmp, key=lambda x: x[1])
        self.comps = [x[0] for x in tmp]
        all_communities = {}
        for i, comp in enumerate(self.comps):
            community = defaultdict(list)
            for tbl_var in comp:
                tbl_var = G.nodes[tbl_var]["label"]
                x = tbl_var.split("--")
                tbl, var = x[0], x[1]
                community[tbl].append(var)
            all_communities[f"Cluster {i}"] = community
        return all_communities

    def get_corr_in_cluster_i(self, i, show_corr_in_same_tbl):
        nodes = self.comps[i]
        res = self.get_corr_in_a_community(
            self.filtered_corr, nodes, show_corr_in_same_tbl
        )
        return res

    def get_corr_in_a_community(self, df, tbls, show_corr_in_same_tbl):
        if self.name == "chicago":
            mask = (df["table_id1"] + "--" + df["agg_attr1"]).isin(tbls) & (
                df["table_id2"] + "--" + df["agg_attr2"]
            ).isin(tbls)
            res = df[mask]
            if len(res) == 0:
                mask = (df["table_name1"] + "--" + df["agg_attr1"]).isin(tbls) & (
                df["table_name2"] + "--" + df["agg_attr2"]
                ).isin(tbls)
                res = df[mask]
        elif self.name == "un":
            mask = (df["tbl1"] + "--" + df["attr1"]).isin(tbls) & (
                df["tbl2"] + "--" + df["attr2"]
            ).isin(tbls)
            res = df[mask]
            if not show_corr_in_same_tbl:
                res = res[~(res["tbl1"] == res["tbl2"])]
        else:
            mask = (df["table_id1"] + "--" + df["agg_attr1"]).isin(tbls) & (
                df["table_id2"] + "--" + df["agg_attr2"]
            ).isin(tbls)
            res = df[mask]
        return res[self.display_attrs]
