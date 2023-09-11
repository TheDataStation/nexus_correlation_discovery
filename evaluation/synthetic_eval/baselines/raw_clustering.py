import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt

def raw_clustering(df, threshold):
    # compute correlation matrix
    corr = df.corr()

    G = nx.Graph()

    for i in range(corr.shape[0]):
        for j in range(i + 1, corr.shape[1]):
            if abs(corr.iloc[i, j]) >= threshold:
                G.add_edge(corr.columns[i], corr.columns[j], weight=abs(corr.iloc[i, j]))
    
    nx.draw_spring(G, with_labels=True)
    plt.savefig('graph_corr.png')
    comps = nx.community.louvain_communities(G)
    res = []
    for comp in comps:
        res.append(sorted([int(x) for x in comp]))
    return G, res

if __name__ == "__main__":
    df = pd.read_csv('/home/cc/resolution_aware_spatial_temporal_alignment/evaluation/synthetic_eval/data/data.csv')
    G, comps = raw_clustering(df, 0.25)
    print(df.corr())
    print(comps)
