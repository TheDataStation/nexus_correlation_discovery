from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
from data_search.commons import FIND_JOIN_METHOD
import matplotlib.pyplot as plt
import utils.io_utils as io_utils

def load_data(data_path, threshold, lowest):
    data = io_utils.load_json(f"{data_path}/result_{lowest}.json")
    results = [eval(x) for x in data.keys()]
    results.sort(reverse=True)
    # print(results)
    score = 0
    original_score = 0
    max_thresholds = []
    max_cov = 0
    for res in results:
        cov, cur_score = res[0], res[1]
        if cov >= threshold:
            score = cur_score
            max_thresholds = data[str(res)][0]
            max_cov = cov
        if cov == 1:
            original_score = cur_score
    print(max_cov, max_thresholds)
    return score, original_score

def plot(result_path, thresholds, save_path):
    scores, original_scores = [], []
    for t in thresholds:
        score, original_score = load_data(result_path, t, thresholds[-1])
        scores.append(score)
        original_scores.append(original_score)
    vars = [original_scores, scores]
    fig, ax = plt.subplots(nrows=1, ncols=1, squeeze=True)
    params = {
        "ylabel": "Clustering Coefficient",
        "xlabel": "Coverage Ratio Threshold",
        "save_path": save_path,
        "legend": 'upper right',
        "ylim": (0, 1)
    }

    grouped_bar_plot(
        ax, ["original", "noise reduction"], thresholds, vars, params
    )

if __name__ == '__main__':
    root_path = "/home/cc/nexus_correlation_discovery/evaluation/"
    corr_dirs = ["chicago_1m_T_GRANU.DAY_S_GRANU.BLOCK", "chicago_1m_T_GRANU.MONTH_S_GRANU.TRACT"]
    for corr_dir in corr_dirs:
        result_path = f"{root_path}/graph_results4/{corr_dir}/"
        plot(result_path, [0.6, 0.4, 0.2], f'{root_path}/plots/{corr_dir}_cc.png')