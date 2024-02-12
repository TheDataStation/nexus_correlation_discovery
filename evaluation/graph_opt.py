from graph.graph_utils import (
    Signal,
    load_corr,
    filter_on_a_signal,
    build_graph,
    filter_on_signals,
    get_cov_ratio,
    get_mod_score,
    get_average_clustering,
    filter_on_graph_edge_weight,
)
import numpy as np

import pandas as pd

# import modin.pandas as pd
import itertools
from tqdm import tqdm
import time
import networkx as nx
from utils.io_utils import dump_json
import os
from copy import deepcopy

from enum import Enum
from graph.threshold_search import Threshold_Search, Score

def run(corr_path, result_path, cov_ratio):
    signal_names = [
        "missing_ratio",
        "zero_ratio",
        "missing_ratio_o",
        "zero_ratio_o",
        "r_val",
        "samples",
    ]

    signals = []
    for signal_name in signal_names:
        if "missing_ratio" in signal_name or "zero_ratio" in signal_name:
            signals.append(Signal(signal_name, -1, 0.2))
        elif signal_name == "r_val":
            signals.append(Signal(signal_name, 1, 0.1))
        elif signal_name == "samples":
            signals.append(Signal(signal_name, 1, 10))

    searcher = Threshold_Search(
        corr_path, signal_names, signals, cov_ratio, metric=Score.MODULARITY, level="VARIABLE"
    )
    
    ranges = searcher.determine_signal_ranges()
    for signal, range in ranges.items():
        print(signal.name)
        print(range)
    
    start = time.time()

    searcher.search_for_thresholds()
    result_path = f"{result_dir}/result_{cov_ratio}.json"
    searcher.persist(result_path)
    total_time = time.time() - start
    dump_json(f"{result_dir}/total_time_{cov_ratio}.json", {"total_time": total_time})
    print(f"used {time.time() -start} s")
    # print(f"found {searcher.count} valid thresholds")

if __name__ == '__main__':
    root_path = "/home/cc/nexus_correlation_discovery/"
    # corr_dir_name = "chicago_1m_T_GRANU.DAY_S_GRANU.BLOCK"
    # corr_dir_name = "chicago_1m_T_GRANU.MONTH_S_GRANU.TRACT"
    corr_dir_names = ["chicago_1m_T_GRANU.MONTH_S_GRANU.TRACT"]
    for corr_dir_name in corr_dir_names:
        corr_path = f"{root_path}/evaluation/correlations2/{corr_dir_name}/"
        result_dir = f"{root_path}/evaluation/graph_results2/{corr_dir_name}/"
        for cov_ratio in [0.5]:
            run(corr_path, result_dir, cov_ratio)