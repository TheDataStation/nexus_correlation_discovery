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

# import modin.pandas as pd
import itertools
from tqdm import tqdm
import time
import networkx as nx
from utils.io_utils import dump_json
import os
from copy import deepcopy


class Threshold_Search:
    def __init__(self, path, names, signals, cov_t) -> None:
        self.corr = load_corr(path)
        print("finished loading correlation, begin to search for thresholds")
        self.signals = signals
        self.signal_names = names
        self.n = pd.concat([self.corr["tbl_id1"], self.corr["tbl_id2"]]).nunique()
        # print(self.n)
        self.cov_t = cov_t
        self.count = 0
        self.initial_mod = get_mod_score(self.corr)
        self.max_mod = 0
        self.max_thresholds = None
        # persist all thresholds whose modularity score is larger than the original graph
        self.valid_threholds = {}  # tuple of thresholds -> modularity score
        self.perf_profile = {}

    def get_tbl_num(self, corr):
        return pd.concat([corr["tbl_id1"], corr["tbl_id2"]]).nunique()

    def determine_signal_ranges(self):
        signal_ranges = {}

        for signal in self.signals:
            if "missing_ratio" in signal.name or "zero_ratio" in signal.name:
                min_v, max_v = 0, 1
            else:
                min_v, max_v = (
                    self.corr[signal.name].min(),
                    self.corr[signal.name].max(),
                )
            t_range = np.arange(min_v, max_v, signal.step)
            if not np.any(t_range == max_v):
                t_range = np.append(t_range, max_v)
            if signal.d == -1:
                t_range = t_range[::-1]
            signal_ranges[signal] = t_range

        # keep thresholds that achieve the coverage ratio
        valid_ranges = {}
        for s, t_range in signal_ranges.items():
            valid_t = []
            for t in t_range:
                corr_filtered = filter_on_a_signal(self.corr, s, t)
                if get_cov_ratio(corr_filtered, self.n) < self.cov_t:
                    break
                if round(get_mod_score(corr_filtered), 3) <= round(self.initial_mod, 3):
                    continue
                # print(f"mod score: {get_mod_score(corr_filtered)}")
                # print(f"thresholds: {s.name}, {t}")
                valid_t.append(t)
            if len(valid_t) == 0:
                valid_t.append(t_range[0])
            valid_ranges[s] = valid_t
        return valid_ranges

    def is_valid(self, corr_filtered):
        # start = time.time()
        # corr_filtered = filter_on_signals(self.corr, self.signals, thresholds)
        # print("filtering", time.time() - start)
        # start = time.time()
        if get_cov_ratio(corr_filtered, self.n) < self.cov_t:
            return False
        # print("get coverage ratio", time.time() - start)
        # print(time.time() - start)
        return True

    def enumerate_combinations(self, lists, result, idx):
        # Base case: If we have a complete combination
        if idx == len(lists):
            self.count += 1
            return False

        # Recursive case: Iterate over the remaining elements of the current list
        #     print(result)
        for i, item in enumerate(lists[idx]):
            result[idx] = item
            if idx == len(lists) - 1:
                corr_filtered = filter_on_signals(self.corr, self.signals, result)
                if not self.is_valid(corr_filtered):
                    result[idx] = -1
                    return i == 0
                else:
                    start = time.time()
                    mod_score = get_mod_score(corr_filtered)
                    # print(f"calulate mod score took {time.time() - start}")
                    if mod_score > self.max_mod:
                        self.max_mod = mod_score
                        print(f"max mod score is {self.max_mod}")
                        print(f"thresholds: {result}")
                        self.max_thresholds = deepcopy(result)
                    if mod_score > self.initial_mod:
                        self.valid_threholds[
                            ",".join([str(round(i, 3)) for i in result])
                        ] = mod_score
            if self.enumerate_combinations(lists, result, idx + 1):
                result[idx] = -1
                return i == 0
        result[idx] = -1
        return False

    def search_for_thresholds(self):
        start = time.time()
        signal_ranges = self.determine_signal_ranges()
        # generate all possible combinations of thresholds
        vals = list(signal_ranges.values())
        print("begin to get all combinations")
        for val in vals:
            print(len(val))
        self.enumerate_combinations(vals, [-1] * len(vals), 0)
        end = time.time()
        self.perf_profile["num_valid_thresholds"] = self.count
        self.perf_profile["total_time"] = end - start
        self.perf_profile["max_mod"] = self.max_mod
        self.perf_profile["max_thresholds"] = tuple(
            [float(round(i, 3)) for i in self.max_thresholds]
        )

    def persist(self, path):
        dump_json(os.path.join(path, "perf_profile.json"), self.perf_profile)
        dump_json(os.path.join(path, "valid_thresholds.json"), self.valid_threholds)


if __name__ == "__main__":
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
    # ranges = searcher.determine_signal_ranges()
    # for signal, range in ranges.items():
    #     print(signal.name)
    #     print(range)
    searcher.search_for_thresholds()
    print(f"used {time.time() -start} s")
    print(f"found {searcher.count} valid thresholds")
