from graph.graph_utils import (
    Signal,
    load_corr,
    filter_on_a_signal,
    filter_on_signals,
    get_cov_ratio,
    get_mod_score,
)
import numpy as np
import pandas as pd
import itertools
from tqdm import tqdm
import time
import networkx as nx
from graph.threshold_search import Threshold_Search

corr_path = "/Users/yuegong/Documents/spatio_temporal_alignment/result/cdc_10k/corr_T_GRANU.DAY_S_GRANU.STATE_fdr/"

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

searcher = Threshold_Search(corr_path, signal_names, signals, 0.7)
start = time.time()

searcher.search_for_thresholds()
searcher.persist(
    "/Users/yuegong/Documents/spatio_temporal_alignment/evaluation/graph_result/cdc"
)
print(f"used {time.time() -start} s")
print(f"found {searcher.count} valid thresholds")
