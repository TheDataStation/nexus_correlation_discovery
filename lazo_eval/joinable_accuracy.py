from utils import io_utils
from utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
import matplotlib.pyplot as plt
import matplotlib as mpl


def calculate_precision_recall(join_key, gt, lazo, o_t, fn_p=False, validate=False):
    if join_key not in gt:
        gt_list = [] 
    else:
        gt_list = gt[join_key]
        gt_list = [x[0] for x in gt_list if x[1]>=o_t]
    # print(gt_list)
    lazo_list = lazo[join_key]
    if validate:
        lazo_list = [x['l'] for x in lazo_list if x['r']>=o_t]
    else:
        lazo_list = [x['l'] for x in lazo_list]
    # print(lazo_list)
    TP = len(set(gt_list).intersection(set(lazo_list)))
    FP = len(set(lazo_list).difference(set(gt_list)))
    if FP != 0:
        print(join_key)
        print(set(lazo_list).difference(set(gt_list)))
    FN_elements = set(gt_list).difference(set(lazo_list))
    if fn_p:
        return TP+FP, FN_elements
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

# def plot_dist(values, save_path):
#     data = [v for v in values if v is not None]
#     plt.hist(data, bins=50)
#     plt.savefig(save_path)

def box_plot(ax, data_i, jc_value):
    data = []
    for d in data_i:
        data.append([v for v in d if v is not None])
    ax.boxplot(data)
    ax.set_title('jc={}'.format(jc_value))
    ax.set_ylabel('Value')
    ax.set_xticks([1, 2, 3], ['Precision', 'Recall', 'F1-Score'])

if __name__ == "__main__":
    # t_granu, s_granu = T_GRANU.DAY, S_GRANU.BLOCK
    t_granu, s_granu = TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT
    o_t_l = [10]
    jc_values = [0.0, 0.2, 0.4, 0.6]
    jc_values = [0.2]
    validate = False
    data_sources = ['chicago_1m']
    data_sources = ['nyc_open_data', 'chicago_open_data']
    for o_t in o_t_l:
        fig, axs = plt.subplots(1, len(jc_values), figsize=(15, 5))
        for i, jc_value in enumerate(jc_values):
            precision_l, recall_l, f_score_l = [], [], []
            for category in ["temporal", "spatial", "st"]:
                gt = io_utils.load_json(f"lazo_eval/ground_truth_join/{'_'.join(data_sources)}/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json")
                if validate:
                    lazo = io_utils.load_json(f"lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{t_granu.value}_space_{s_granu.value}/{category}_joinable_jc_{str(jc_value)}.json")
                else:
                    lazo = io_utils.load_json(f"lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{t_granu.value}_space_{s_granu.value}/{category}_joinable_jc_{str(jc_value)}_validate_false.json")
                for join_key in lazo.keys():
                    join_key, precision, recall, f_score = calculate_precision_recall(join_key, gt, lazo, o_t, validate)
                    precision_l.append(precision)
                    recall_l.append(recall)
                    f_score_l.append(f_score)
            if len(jc_values) == 1:
                box_plot(axs, [precision_l, recall_l, f_score_l], jc_value)
            else:
                box_plot(axs[i], [precision_l, recall_l, f_score_l], jc_value)
        # if a directory does not exist, create it
        io_utils.create_dir(f'lazo_eval/figure/{"_".join(data_sources)}')
        plt.savefig(f'lazo_eval/figure/{"_".join(data_sources)}/box_plots_{t_granu}_{s_granu}_o_t_{o_t}_validate_{validate}.png')
    
    # folder = "jc_01_k_512"
    # for i, o_t in enumerate(o_t_l):
    #     precision_l, recall_l, f_score_l = [], [], []
    #     for category in ["temporal", "spatial", "st"]:
    #         gt = io_utils.load_json(f"lazo_eval/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json")
    #         lazo = io_utils.load_json(f"lazo_eval/lazo_joinable/{folder}/{category}_joinable_10.json")
    #         # print(len(gt.keys()), len(lazo.keys()))
    #         for join_key in lazo.keys():
    #             join_key, precision, recall, f_score = calculate_precision_recall(join_key, gt, lazo, o_t)
    #             precision_l.append(precision)
    #             recall_l.append(recall)
    #             f_score_l.append(f_score)
    #         print(category)
    #         print(recall_l)
    #     box_plot(axs[i], [precision_l, recall_l, f_score_l], o_t)
    # plt.savefig(f'box_plots_{folder}.png')