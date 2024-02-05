from gen_gt_data import Weights, out_p
import pandas as pd
from baselines import direct_clustering, clustering, causal_discovery, eval_clusters
from graph.threshold_search import Score

def load_gt_data(community_size, N , N_noise, weights):
    path = f"{out_p}/data_{community_size}_{N}_{N_noise}_{weights.value}.csv"
    df = pd.read_csv(path)
    gt_clusters = []
    p_sum = 0
    for _ in range(N):
        gt_clusters.append(list(range(p_sum, p_sum+community_size)))
        p_sum += community_size
    for i in range(N*community_size, N*community_size+N_noise):
        gt_clusters.append([i])
    return df, gt_clusters

if __name__ == "__main__":
    weights = Weights.Uniform
    community_size, N, N_noise = 10, 3, 10
    total_var = 30 + N_noise
    df, true_clusters = load_gt_data(community_size, N, N_noise, weights)

    # Baseline 1
    print("Baseline 1 - Direct Clustering (consider only strong correlations)")
    G, comps = direct_clustering(df.corr(), 0.6, True)
    print(f"clusters 1: {comps}")
    metrics = eval_clusters(total_var, true_clusters, comps)
    print(metrics)

    # Baseline 2
    print("Baseline 2 - Direct Clustering (consider all correlations)")
    G, comps = direct_clustering(df.corr(), 0, True)
    print(f"clusters 2: {comps}")
    metrics = eval_clusters(total_var, true_clusters, comps)
    print(metrics)

    # Baseline 3
    print("Baseline 3 - Causal Discovery Algorithm IC")
    comps = causal_discovery(df, "IC")
    print(f"clusters 3: {comps}")
    metrics = eval_clusters(total_var, true_clusters, comps)
    print(metrics)

    # Baseline 4
    print("Baseline 4 - Causal Discovery Algorithm PC")
    comps = causal_discovery(df, "PC")
    print(f"clusters 4: {comps}")
    metrics = eval_clusters(total_var, true_clusters, comps)
    print(metrics)

    # Our method
    print("Our method")
    cov_ratio = N/total_var
    max_threshold, max_score, comps = clustering(df.corr(), cov_ratio, total_var, 0.1, 1, 0.05, Score.MODULARITY, weighted=True)
    print(f"clusters 5: {comps}")
    metrics = eval_clusters(total_var, true_clusters, comps)
    print(metrics)

    # G, comps = raw_clustering(df.corr(), 0.8, weighted=True)
    # print(df.corr())
    # print(comps)
    # clustering(df.corr(), 0.9, 30, Score.MODULARITY, weighted=True)
