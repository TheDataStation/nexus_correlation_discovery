from graph_utils import load_corr, filter_on_signals, build_graph_with_labels
import networkx as nx
from pyvis.network import Network


class CommunityDetection:
    def __init__(self, corr_path, thresholds):
        corr_list = load_corr(corr_path)
        self.filtered_corr = filter_on_signals(corr_list, None, thresholds)

    def get_communities(self):
        G = build_graph_with_labels(self.filtered_corr)
        print(
            f"number of nodes: {G.number_of_nodes()}; number of edges: {G.number_of_edges()}"
        )
        comps = nx.community.louvain_communities(G, resolution=1)
        print(f"number of communities: {len(comps)}")
        for i, comp in enumerate(comps):
            print(f"==========community {i}=============")
            for tbl in comp:
                print(G.nodes[tbl]["label"])
        return G

    def visualize(self, G):
        nt = Network()
        nt.from_nx(G)
        nt.show("nx.html")


if __name__ == "__main__":
    corr_path = "/Users/yuegong/Documents/spatio_temporal_alignment/result/chicago_10k/corr_T_GRANU.DAY_S_GRANU.BLOCK_fdr/"
    detector = CommunityDetection(corr_path, [1.0, 0.8, 1.0, 0.2, 1.0, 4])
    G = detector.get_communities()
    nt = Network(notebook=False)
    nt.from_nx(G)
    nt.show("nx.html")
