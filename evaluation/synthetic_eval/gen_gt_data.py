
import networkx as nx
import matplotlib.pyplot as plt
import random
from enum import Enum
import pickle
from causaltestdata import variable
import numpy as np

# global output path
out_p = "/home/cc/resolution_aware_spatial_temporal_alignment/evaluation/synthetic_eval/data/"

class Weights(str, Enum):
    Uniform = "uniform"
    Outdegree = "outdegree"
    Dominate = "dominate"

def gen_graph_sbm(sizes, probs):
    g = nx.stochastic_block_model(sizes, probs, seed=0, directed=True)
    return g

def print_degree(G: nx.Graph):
    for node in G.nodes():
        print(f'Node {node}: In-degree = {G.in_degree(node)}, Out-degree = {G.out_degree(node)}')

def print_parents_children(G: nx.Graph):
    for node in G.nodes():
        predecessors = list(G.predecessors(node))
        successors = list(G.successors(node))
        print(f'Node {node}: Predecessors: {predecessors}, Successors: {successors}')

def gen_ranked_weights(n):
    numbers = list(range(1, n + 1))
    normalized_numbers = sorted([number / sum(numbers) for number in numbers], reverse=True)
    return normalized_numbers

def gen_rand_weights(n):
    # Generate n-1 small random numbers
    if n == 1:
        return [1]
    numbers = [random.random() for _ in range(n-1)]

    # Add a dominant number to the set
    dominant_number = max(numbers) * 2
    numbers.append(dominant_number)

    # Sum of numbers
    total = sum(numbers)

    # Normalize the numbers
    normalized_numbers = sorted([number / total for number in numbers], reverse=True)
    return normalized_numbers

def gen_graph(sizes, p_intra, p_inter, weights: Weights, seed=0):
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
    inter_edges = set()
    for i in range(len(communities)):
        for j in range(i + 1, len(communities)):
            for u in communities[i]:
                for v in communities[j]:
                    if G.in_degree(v) < len(communities) / 3:
                        continue
                    if random.random() < p_inter:
                        print(f"edge {u} to {v}")
                        G.add_edge(u, v)
                        inter_edges.add((u, v))
    print(inter_edges)
    if weights == Weights.Uniform:
        edge_weights = {(u, v): 1 for u, v in G.edges()}
        for edge in inter_edges:
            edge_weights[edge] = 0.1
        nx.set_edge_attributes(G, edge_weights, 'weight')
    elif weights == Weights.Outdegree:
        # Compute the out-degree of each node
        out_degrees = dict(G.out_degree())
        edge_weights = {(u, v): 1 / out_degrees[u] for u, v in G.edges()}
        nx.set_edge_attributes(G, edge_weights, 'weight')
    elif weights == Weights.Dominate:
        for node in G.nodes():
            predecessors = list(G.predecessors(node))
            if len(predecessors) == 0:
                continue
            # cur_weights = gen_rand_weights(len(predecessors))
            cur_weights = gen_ranked_weights(len(predecessors))
            random.shuffle(cur_weights)
            for i, predecessor in enumerate(predecessors):
                if (predecessor, node) in inter_edges:
                    G[predecessor][node]['weight'] = min(cur_weights)
                    print(f"{(predecessor, node)}, {min(cur_weights)}")
                else:
                    G[predecessor][node]['weight'] = cur_weights[i]
    return G


def gen_data(community_size, N, N_noise, p_intra, p_inter, weights, seed: int, serialize: bool):
    G = gen_graph([community_size]*N , p_intra, p_inter, weights, seed)
    defaults = {}
    df = variable.generate_all(G, defaults)
    
    # generate noise variables
    num_var = N * community_size
    for var in range(num_var, num_var + N_noise):
        df[var] = np.random.normal(0, 1, 1000)

    if serialize:
        # serialize the network
        with open(f'{out_p}/graph_{community_size}_{N}_{N_noise}_{weights.value}.pkl', 'wb') as f:
            pickle.dump(G, f)
        # serialize data
        df.to_csv(f"{out_p}/data_{community_size}_{N}_{N_noise}_{weights.value}.csv", index=False)
    
    return G, df

if __name__ == "__main__":
    community_size, n_cluster, n_noise = 10, 3, 10
    p_intra, p_inter = 0.7, 0.02
    weights = Weights.Uniform
    seed = 0
    G, df = gen_data(community_size, n_cluster, n_noise, p_intra, p_inter, weights, seed, True)
    print(df.head())
    # Draw the graph
    nx.draw_spring(G, with_labels=True)
    plt.savefig('graph.png')

  