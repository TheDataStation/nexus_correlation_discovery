from utils import io_utils
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import matplotlib.pyplot as plt
import matplotlib as mpl


def calculate_precision_recall(join_key, gt, lazo, o_t):
    if join_key not in gt:
        gt_list = [] 
    else:
        gt_list = gt[join_key]
        gt_list = [x[0] for x in gt_list if x[1]>=o_t]
    # print(gt_list)
    lazo_list = lazo[join_key]
    lazo_list = [x['l'] for x in lazo_list]
    # print(lazo_list)
    TP = len(set(gt_list).intersection(set(lazo_list)))
    FP = len(set(lazo_list).difference(set(gt_list)))
    FN = len(set(gt_list).difference(set(lazo_list)))
    # print(TP, FP, FN)
    if TP + FP == 0:
        precision = None
    else:
        precision = TP / (TP + FP)
    if TP+FN == 0:
        recall = None
    else:
        recall = TP / (TP + FN)
    if precision is None and recall is None:
        f_score = None
    elif precision == 0 or recall == 0:
        f_score = 0
    else:
        f_score = 2 * precision * recall / (precision + recall)
    # print(join_key, round(precision,2) if precision else None, round(recall,2) if recall else None, round(f_score, 2) if f_score else None)
    return join_key, round(precision,2) if precision is not None else None, round(recall,2) if recall is not None else None, round(f_score, 2) if f_score is not None else None

def plot_dist(values, save_path):
    data = [v for v in values if v is not None]
    plt.hist(data, bins=50)
    plt.savefig(save_path)

def box_plot(ax, data_i, o_t):
    data = []
    for d in data_i:
        data.append([v for v in d if v is not None])
    ax.boxplot(data)
    ax.set_title('overlap={}'.format(o_t))
    ax.set_ylabel('Value')
    ax.set_xticks([1, 2, 3], ['Precision', 'Recall', 'F1-Score'])

if __name__ == "__main__":
    t_granu, s_granu = T_GRANU.DAY, S_GRANU.BLOCK
    o_t_l = [10, 100, 1000]
    # gt = io_utils.load_json(f"lazo_eval/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json")
    # lazo = io_utils.load_json(f"lazo_eval/lazo_joinable/k_512/st_joinable_1000.json")
    # print(calculate_precision_recall('t28b-ys7j_creation_date_2_location_1', gt, lazo, 1000))
    fig, axs = plt.subplots(1, 3, figsize=(15, 5))
    for i, o_t in enumerate(o_t_l):
        precision_l, recall_l, f_score_l = [], [], []
        for category in ["temporal", "spatial", "st"]:
            gt = io_utils.load_json(f"lazo_eval/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json")
            lazo = io_utils.load_json(f"lazo_eval/lazo_joinable/k_512/{category}_joinable_{o_t}.json")
            print(len(gt.keys()), len(lazo.keys()))
            for join_key in lazo.keys():
                join_key, precision, recall, f_score = calculate_precision_recall(join_key, gt, lazo, o_t)
                precision_l.append(precision)
                recall_l.append(recall)
                f_score_l.append(f_score)
            print(category)
            print(recall_l)
        box_plot(axs[i], [precision_l, recall_l, f_score_l], o_t)
    plt.savefig(f'box_plots.png')
    print(len(recall_l))
    plot_dist(recall_l, 'recall.png')
    plot_dist(precision_l, 'precision.png')
    plot_dist(f_score_l, 'f_score.png')