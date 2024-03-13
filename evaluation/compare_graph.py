from utils.spatial_hierarchy import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from graph.graph_utils import load_all_corrs, build_graph_on_vars
import networkx as nx

def get_mod_score(dir):
    corrs = load_all_corrs(dir)
    corrs = corrs[(abs(corrs['r_val'])>=0.2) & (corrs['missing_ratio_o1']<=0.5) & (corrs['missing_ratio_o2']<=0.5)]
    G = build_graph_on_vars(corrs)
    print(
            f"number of nodes: {G.number_of_nodes()}; number of edges: {G.number_of_edges()}"
        )
    comps = nx.community.louvain_communities(G, resolution=1, seed=1)
    print(f"number of communities: {len(comps)}")
    print("modularity:", nx.community.modularity(G, comps))

if __name__ == '__main__':
    storage_dir = 'correlations12_30'
    dump_dir = 'correlation_quality12_30'
    # baseline = 'polygamy'
    # t_granu, s_granu = T_GRANU.MONTH, S_GRANU.TRACT
    t_granu, s_granu = TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK
    dir_nexus = f'/home/cc/nexus_correlation_discovery/evaluation/correlations12_29/nexus_0.0/chicago_1m_{t_granu}_{s_granu}/'
    dir_sampling = f'/home/cc/nexus_correlation_discovery/evaluation/correlations12_29/nexus_time_sampling/chicago_1m_time_sampling_{t_granu}_{s_granu}/'
    get_mod_score(dir_nexus)
    get_mod_score(dir_sampling)
