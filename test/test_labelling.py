from nexus.utils.io_utils import load_json
def compare_two_labels():
    ground_truth_catalog = load_json('resource/chicago_test/chicago_test_gt.json')
    test_catalog = load_json('resource/chicago_test/chicago_test.json')
    stats = {"temporal": {"tp": 0, "fp": 0, "fn": 0},
             "spatial": {"tp": 0, "fp": 0, "fn": 0},
             "num": {"tp": 0, "fp": 0, "fn": 0}}
    for tbl_id, gt_info in ground_truth_catalog.items():
        test_info = test_catalog[tbl_id]
        # calculate the accuracy of test_info
        # calculate the accuracy of temporal_attrs
        gt_temporal_attrs = [t_attr["name"] for t_attr in gt_info['t_attrs']]
        test_temporal_attrs = [t_attr["name"] for t_attr in test_info['t_attrs']]
        for gt_attr in gt_temporal_attrs:
            if gt_attr in test_temporal_attrs:
                stats["temporal"]["tp"] += 1
            else:
                stats["temporal"]["fp"] += 1
        for test_attr in test_temporal_attrs:
            if test_attr not in gt_temporal_attrs:
                stats["temporal"]["fn"] += 1
        # calculate the accuracy of spatial_attrs
        gt_spatial_attrs = [s_attr["name"] for s_attr in gt_info['s_attrs']]
        test_spatial_attrs = [s_attr["name"] for s_attr in test_info['s_attrs']]
        for gt_attr in gt_spatial_attrs:
            if gt_attr in test_spatial_attrs:
                stats["spatial"]["tp"] += 1
            else:
                stats["spatial"]["fp"] += 1
        for test_attr in test_spatial_attrs:
            if test_attr not in gt_spatial_attrs:
                stats["spatial"]["fn"] += 1
        
        # calculate the accuracy of num_attrs
        gt_num_attrs = gt_info['num_columns']
        test_num_attrs = test_info['num_columns']
        for gt_attr in gt_num_attrs:
            if gt_attr in test_num_attrs:
                stats["num"]["tp"] += 1
            else:
                stats["num"]["fp"] += 1
        for test_attr in test_num_attrs:
            if test_attr not in gt_num_attrs:
                stats["num"]["fn"] += 1
    stats["temporal"]["precision"] = stats["temporal"]["tp"] / (stats["temporal"]["tp"] + stats["temporal"]["fp"])
    stats["temporal"]["recall"] = stats["temporal"]["tp"] / (stats["temporal"]["tp"] + stats["temporal"]["fn"])
    stats["spatial"]["precision"] = stats["spatial"]["tp"] / (stats["spatial"]["tp"] + stats["spatial"]["fp"])
    stats["spatial"]["recall"] = stats["spatial"]["tp"] / (stats["spatial"]["tp"] + stats["spatial"]["fn"])
    stats["num"]["precision"] = stats["num"]["tp"] / (stats["num"]["tp"] + stats["num"]["fp"])
    stats["num"]["recall"] = stats["num"]["tp"] / (stats["num"]["tp"] + stats["num"]["fn"])
    print(stats)

if __name__ == "__main__":
    compare_two_labels()