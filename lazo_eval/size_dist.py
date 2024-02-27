import os
import json
import matplotlib.pyplot as plt
import matplotlib as mpl
from utils import io_utils
from utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from joinable_accuracy import calculate_precision_recall
import seaborn as sns

lookup = {}

def get_jaccard(jk1, jk2, category):
    jk1_set, jk2_set = set(lookup[jk1]), set(lookup[jk2])
    return len(jk1_set.intersection(jk2_set)) / len(jk1_set.union(jk2_set))

def load():
    for category in ["temporal", "spatial", "st"]:
        # load each json file from a directory
        set_size = {}
        dir = f"/home/cc/nexus_correlation_discovery/join_key_data/chicago_1m/time_2_space_1/{category}"
        for filename in os.listdir(dir):
            if filename.endswith(".json"):
                filepath = os.path.join(dir, filename)
                with open(filepath) as f:
                    tbl_name = filename[:-5]
                    data = json.load(f)
                    lookup[tbl_name] = data[tbl_name]
                    # set_size[tbl_name] = len(data[tbl_name])
                continue
            else:
                continue
        # # dump set size to a json file
        # with open(f"{category}_set_size.json", "w") as f:
        #     json.dump(set_size, f)

def plot_dist(ax, values):
    # ax.hist(values, bins=50)
    # create a box plot on values
    green_diamond = dict(markerfacecolor='g', marker='D')
    ax.boxplot(values)
    ax.set_yscale('log', flierprops=green_diamond)

def plot_size_dist():
    fig, axs = plt.subplots(1, 3, figsize=(15, 5))
    for i, category in enumerate(["temporal", "spatial", "st"]):
        with open(f"{category}_set_size.json") as f:
            data = json.load(f)
        plot_dist(axs[i], data.values())
        axs[i].set_title(category)
        axs[i].set_xlabel("Set Size")
        axs[i].set_ylabel("Frequency")
    plt.savefig("set_size_dist.png")

def plot_size_dist_in_one():
    fig, axs = plt.subplots(1, 1, figsize=(5, 5))
    sizes = []
    for i, category in enumerate(["temporal", "spatial", "st"]):
        with open(f"{category}_set_size.json") as f:
            data = json.load(f)
        sizes.extend(data.values())
    plot_dist(axs, sizes)
    # axs[i].set_title(category)
    axs.set_xlabel("Granularity: Day, Census Block")
    axs.set_ylabel("#Rows (log scale)")
    plt.savefig("set_size_dist.png")

if __name__ == "__main__":
    plot_size_dist_in_one()
    # t_granu, s_granu = T_GRANU.DAY, S_GRANU.BLOCK
    # o_t = 100
    # js_fn = []
    # # load js_fn.json and plot the distribution
    # js_fn = io_utils.load_json("js_all.json")
    # # in the histgram, add a line with x = 0.0
    # plt.hist(js_fn, label='CDF', cumulative=True, histtype='step', density=True, bins=100)
    # # plt.axvline(x = 0.05, color = 'red', linestyle = '--')
    # # plt.savefig("js_fn_dist.png")
    # # sns.displot(js_fn, kde=True)
    # plt.savefig("js_all_cde.png")
    # plt.boxplot(js_fn)
    # plt.savefig("js_fn_boxplot.png")
    # load()
    # print("finish load")
    # for category in ["temporal", "spatial", "st"]:
    #     size_recall = []
    #     sizes = io_utils.load_json(f"{category}_set_size.json")
    #     gt = io_utils.load_json(f"lazo_eval/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json")
    #     lazo = io_utils.load_json(f"lazo_eval/lazo_joinable/jc_0/{category}_joinable_10.json")
    #     # print(len(gt.keys()), len(lazo.keys()))
    #     for join_key in gt.keys():
    #         gt_list = gt[join_key]
    #         gt_list = [x[0] for x in gt_list if x[1]>=o_t]
    #         for cand in gt_list:
    #             js_fn.append(get_jaccard(join_key, cand, category))
    #         # p, fn_l = calculate_precision_recall(join_key, gt, lazo, o_t, True)
    #         # if p != 0:
    #         #     continue
    #         # for fn in fn_l:
    #         #     js_fn.append(get_jaccard(join_key, fn, category))
    # io_utils.dump_json("js_all.json", js_fn)
            # join_key, precision, recall, f_score = calculate_precision_recall(join_key, gt, lazo, o_t)
        #     size_recall.append([sizes[join_key], recall])
        # # plot recall vs. set size
        # size_recall = sorted(size_recall, key=lambda x: x[0])
        # fig, ax = plt.subplots()
        # ax.scatter([x[0] for x in size_recall], [x[1] for x in size_recall])
        # plt.savefig(f"recall_vs_set_size_{category}.png")