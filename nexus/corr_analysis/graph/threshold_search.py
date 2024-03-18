from graph.graph_utils import (
    Signal,
    load_corr,
    remove_bad_cols,
    filter_on_a_signal,
    build_graph,
    build_graph_on_vars,
    filter_on_signals,
    get_cov_ratio,
    get_mod_score,
    get_average_clustering,
)
import numpy as np

import pandas as pd

# import modin.pandas as pd
import time
from nexus.utils.io_utils import dump_json
from copy import deepcopy

from enum import Enum
from collections import defaultdict

class Score(Enum):
    MODULARITY = "mod"
    CLUSTER = "cluster"


class Threshold_Search:
    def __init__(self, path, names, signals, cov_t, metric: Score, level) -> None:
        self.corr = load_corr(path)
      
        stop_words = ["wind_direction", "heading", "dig_ticket_", "uniquekey", "streetnumberto", "streetnumberfrom", "census_block", 
              "stnoto", "stnofrom", "lon", "lat", "northing", "easting", "property_group", "insepctnumber", 'primarykey','beat_',
              "north", "south", "west", "east", "beat_of_occurrence", "lastinspectionnumber", "fax", "latest_dist_res", "majority_dist", "latest_dist",
             "f12", "f13"]
        self.corr = remove_bad_cols(stop_words, self.corr)
        print("finished loading correlation, begin to search for thresholds")
        self.signals = signals
        self.signal_names = names
        self.n = pd.concat([self.corr["tbl_id1"], self.corr["tbl_id2"]]).nunique()
        self.metric = metric
        self.level = level
        self.cov_t = cov_t
        self.count = 0
        # self.initial_mod = get_mod_score(self.corr)
        self.max_mod = 0
        self.max_clustering = 0
        self.max_cov = 0
        self.max_thresholds = set()  # skyline points
        self.metrics_thresholds = defaultdict(list)
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
            elif "r_val" in signal.name:
                min_v = abs(self.corr[signal.name]).min()
                max_v = abs(self.corr[signal.name]).max()
            else:
                min_v, max_v = (
                    self.corr[signal.name].min(),
                    self.corr[signal.name].max(),
                )
            print(signal.name, signal.step)
            print(np.arange(min_v, max_v, signal.step))
            t_range = np.arange(min_v, max_v, signal.step)
            if not np.any(t_range == max_v):
                t_range = np.append(t_range, max_v)
            if signal.d == -1:
                t_range = t_range[::-1]
            signal_ranges[signal] = t_range
        print(signal_ranges)
        # keep thresholds that achieve the coverage ratio
        valid_ranges = {}
        for s, t_range in signal_ranges.items():
            valid_t = []
            for t in t_range:
                corr_filtered = filter_on_a_signal(self.corr, s, t)
                cov_ratio = get_cov_ratio(corr_filtered, self.n)
                print(f"signal name: {s.name}, threshold: {t}, cov_ratio: {cov_ratio}")
                if cov_ratio < self.cov_t:
                    break
                # print(
                #     round(get_mod_score(corr_filtered), 3), round(self.initial_mod, 3)
                # )
                # if round(get_mod_score(corr_filtered), 3) <= round(self.initial_mod, 3):
                #     continue
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
            if self.count % 1000 == 0:
                print(f"progress: {self.count}")
            return False

        # Recursive case: Iterate over the remaining elements of the current list
        #     print(result)
        for i, item in enumerate(lists[idx]):
            result[idx] = item
            if idx == len(lists) - 1:
                start = time.time()
                corr_filtered = filter_on_signals(self.corr, self.signals, result)
                # print(f"filter used: {time.time() - start} s")
                curr_cov = round(get_cov_ratio(corr_filtered, self.n), 2)
                if curr_cov < self.cov_t:
                    result[idx] = -1
                    return i == 0
                else:
                    start = time.time()
                    if self.level == "TABLE":
                        G = build_graph(corr_filtered, 0, False)
                    elif self.level == "VARIABLE":
                        G = build_graph_on_vars(corr_filtered, 0, False)
                    # print(f"build graph took {time.time() - start}")
                    if self.metric == Score.MODULARITY:
                        score = round(get_mod_score(G), 2)
                    elif self.metric == Score.CLUSTER:
                        score = round(get_average_clustering(G), 2)
                    # if (curr_cov, score) not in self.metrics_thresholds:
                    self.metrics_thresholds[(curr_cov, score)].append(deepcopy(result))
                    # print(f"calulate mod score took {time.time() - start}")
                    # if mod_score > self.max_mod:
                    #     self.max_mod = mod_score
                    #     print(f"max mod score is {self.max_mod}")
                    #     print(f"thresholds: {result}")
                    #     self.max_thresholds = deepcopy(result)
                    if score > self.max_clustering:
                        self.max_clustering = score
                        print(f"max clustering score is {self.max_clustering}")
                        print(f"tbl coverage: {curr_cov}")
                        print(f"thresholds: {result}")
                    #     print(f"coverage score: {get_cov_ratio(corr_filtered, self.n)}")
                    #     self.max_thresholds_cluster = deepcopy(result)
                    # if mod_score > self.initial_mod:
                    #     self.valid_threholds[
                    #         ",".join([str(round(i, 3)) for i in result])
                    #     ] = mod_score

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
        points = list(self.metrics_thresholds.keys())
        skyline = self.find_skyline(points)
        print(f"find {len(skyline)} points")
        self.skyline_map = {}
        print(skyline)
        for point in skyline:
            thresholds = self.metrics_thresholds[point]
            self.skyline_map[str(point)] = [self.round_thresholds(t) for t in thresholds]
        # print(self.skyline_map)
        # self.perf_profile["num_valid_thresholds"] = self.count
        # self.perf_profile["total_time"] = end - start
        # self.perf_profile["max_mod"] = self.max_mod
        # self.perf_profile["max_thresholds"] = tuple(
        #     [float(round(i, 3)) for i in self.max_thresholds]
        # )

    def round_thresholds(self, thresholds):
        return [float(round(threshold, 2)) for threshold in thresholds]
    
    def find_skyline(self, points):
        # sort points by the first dimension
        points = sorted(points)
        mono_stack = []
        stack_len = 0
        for point in points:
            while stack_len > 0 and point[1] >= mono_stack[-1][1]:
                mono_stack.pop()
                stack_len -= 1
            mono_stack.append(point)
            stack_len += 1
        return mono_stack

    def persist(self, path):
        dump_json(path, self.skyline_map)
        # dump_json(os.path.join(path, "perf_profile.json"), self.perf_profile)
        # dump_json(os.path.join(path, "valid_thresholds.json"), self.valid_threholds)


if __name__ == "__main__":
    # corr_path = "/Users/yuegong/Documents/spatio_temporal_alignment/result/cdc_10k/corr_T_GRANU.DAY_S_GRANU.STATE_fdr/"
    # corr_path = "/Users/yuegong/Documents/spatio_temporal_alignment/result/chicago_10k/corr_T_GRANU.DAY_S_GRANU.BLOCK_fdr/"
    # corr_path = "/Users/yuegong/Documents/spatio_temporal_alignment/result/chicago_10k/day_block/"
    corr_path = "/home/cc/nexus_correlation_discovery/evaluation/correlations/chicago_1m_T_GRANU.DAY_S_GRANU.BLOCK/"
    result_path = "/Users/yuegong/Documents/spatio_temporal_alignment/evaluation/graph_result/chicago/"
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
            signals.append(Signal(signal_name, 1, 0.2))
        elif signal_name == "samples":
            signals.append(Signal(signal_name, 1, 10))
    for signal in signals:
        print(signal.name, signal.step)
    searcher = Threshold_Search(
        corr_path, signal_names, signals, 0.4, metric=Score.CLUSTER
    )
    print(searcher.n)
    # max_score = 0
    # max_threshold = 0
    # G = build_graph(searcher.corr)
    # print(G)
    # for t in range(0, 500, 20):
    #     print(t)
    #     new_G = filter_on_graph_edge_weight(G, t)
    #     print(new_G.number_of_nodes() / searcher.n)
    #     if new_G.number_of_nodes() / searcher.n < searcher.cov_t:
    #         break
    #     comps = nx.community.louvain_communities(G)
    #     score = nx.community.modularity(G, comps)
    #     if score > max_score:
    #         max_score = score
    #         max_threshold = t
    #         print(f"max score: {max_score}")
    #         print(f"max threshold: {max_threshold}")

    start = time.time()
    # ranges = searcher.determine_signal_ranges()
    # for signal, range in ranges.items():
    #     print(signal.name)
    #     print(range)
    searcher.search_for_thresholds()
    searcher.persist(result_path)
    print(f"used {time.time() -start} s")
    print(f"found {searcher.count} valid thresholds")
