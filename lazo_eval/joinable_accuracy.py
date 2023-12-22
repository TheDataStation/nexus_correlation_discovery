from utils import io_utils
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU

t_granu, s_granu = T_GRANU.DAY, S_GRANU.BLOCK
o_t = 100
gt = io_utils.load_json(f"lazo_eval/join_ground_truth_{t_granu}_{s_granu}_overlap_10.json")
lazo = io_utils.load_json(f"lazo_eval/lazo_joinable/k_512/st_joinable_{o_t}.json")

def calculate_precision_recall(join_key):
    if join_key not in gt:
        return
    gt_list = gt[join_key]
    gt_list = [x[0] for x in gt_list if x[1]>=o_t]
    lazo_list = lazo[join_key]
    lazo_list = [x['l'] for x in lazo_list]
    TP = len(set(gt_list).intersection(set(lazo_list)))
    FP = len(set(lazo_list).difference(set(gt_list)))
    FN = len(set(gt_list).difference(set(lazo_list)))
    if TP + FP == 0:
        return
    precision = TP / (TP + FP)
    if TP+FN == 0:
        return
    recall = TP / (TP + FN)
    if precision + recall == 0:
        f_score = 0
    else:
        f_score = 2 * precision * recall / (precision + recall)
    print(join_key, round(precision,2), round(recall,2), f_score)

if __name__ == "__main__":
    for join_key in lazo.keys():
        calculate_precision_recall(join_key)