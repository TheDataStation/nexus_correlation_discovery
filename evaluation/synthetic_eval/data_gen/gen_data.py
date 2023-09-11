from causaltestdata import variable
import networkx as nx
from gen_gt_graph import gen_graph

def gen_data(g: nx.Graph, out_p: str):
    defaults = {}
    df = variable.generate_all(g, defaults)
    df.to_csv(out_p, index=False)
    return df

if __name__ == "__main__":
    g = gen_graph([10, 10, 10], 0.7, 0.01, seed=0)
 
    gen_data(g, '/home/cc/resolution_aware_spatial_temporal_alignment/evaluation/synthetic_eval/data/data.csv')