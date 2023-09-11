
import networkx as nx
import matplotlib.pyplot as plt
import random

def gen_graph_sbm(sizes, probs):
    g = nx.stochastic_block_model(sizes, probs, seed=0, directed=True)
    return g

def print_degree(G: nx.Graph):
    for node in G.nodes():
        print(f'Node {node}: In-degree = {G.in_degree(node)}, Out-degree = {G.out_degree(node)}')

def gen_graph(sizes, p_intra, p_inter, seed=0):
    random.seed(seed)
    communities = []
    p_sum = 0
    for size in sizes:
        communities.append(list(range(p_sum, p_sum+size)))
        p_sum += size
    
    # Create an empty directed graph
    G = nx.DiGraph()

    # Add nodes to the graph
    for community in communities:
        G.add_nodes_from(community)

    # Add edges within communities
    for community in communities:
        for u in community:
            for v in community:
                if u < v and random.random() < p_intra:
                    G.add_edge(u, v)

    # Add edges between communities
    for i in range(len(communities)):
        for j in range(i + 1, len(communities)):
            for u in communities[i]:
                for v in communities[j]:
                    if random.random() < p_inter:
                        print(f"edge {u} to {v}")
                        G.add_edge(u, v)
    
    comps = nx.community.louvain_communities(G)
    print(comps)
    # Compute the out-degree of each node
    out_degrees = dict(G.out_degree())

    # Compute the edge weights
    edge_weights = {(u, v): 1 / out_degrees[u] for u, v in G.edges()}

    # Set the edge weights
    nx.set_edge_attributes(G, edge_weights, 'weight')
     
   
    nx.draw_spring(G, with_labels=True)
    plt.savefig('graph.png')
    print_degree(G)
    return G


if __name__ == "__main__":
    # # Create a graph
    # sizes = [10, 10, 10]
    # probs = [[0.6, 0.05, 0.02], [0.05, 0.6, 0.07], [0.02, 0.07, 0.6]]
    # G = gen_graph(sizes, probs)
    # # Draw the graph
    # plt.figure()
    # nx.draw_spring(G, with_labels=True)
    # plt.savefig('graph.png')

    # Define the ground truth communities
    G = gen_graph([10, 10, 10], 0.7, 0.02)
    comps = nx.community.louvain_communities(G)
    print(comps)
    print(nx.is_directed_acyclic_graph(G))
    # Draw the graph
    nx.draw_spring(G, with_labels=True)
    plt.savefig('graph.png')

  