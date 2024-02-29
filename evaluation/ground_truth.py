"""
In this experiment, we aim to run different baselines and compare their runtimes.
"""
from utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from data_search.search_corr import CorrSearch
import time
from utils.io_utils import dump_json, load_json
import utils.io_utils as io_utils
from tqdm import tqdm
from data_search.commons import FIND_JOIN_METHOD
from data_ingestion.data_profiler import Profiler


data_source = "chicago_1m"
config = io_utils.load_config(data_source)
conn_str = config["db_path"]
granu_lists = [[TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK]]
overlaps = [1000]
st_schemas = io_utils.load_pickle(f"evaluation/input/{data_source}/full_st_schemas.json")


def get_ground_truth(input):
    for granu_list in granu_lists:
        for o_t in overlaps:
            optimal_time = 0
            index_search_time = 0
            join_all_time = 0
            optimal = {}
            profile_index_p = f"evaluation/run_time/{data_source}/{input}/perf_time_per_tbl_{granu_list[0]}_{granu_list[1]}_{FIND_JOIN_METHOD.INDEX_SEARCH.value}_{o_t}.json"
            profile_index = load_json(profile_index_p)
            profile_join_p = f"evaluation/run_time/{data_source}/{input}/perf_time_per_tbl_{granu_list[0]}_{granu_list[1]}_{FIND_JOIN_METHOD.JOIN_ALL.value}_{o_t}.json"
            profile_join = load_json(profile_join_p)
            for tbl_name, l in profile_index.items():
                run_time1 = l[0]
                index_search_time += run_time1
                run_time2 = profile_join[tbl_name][0]
                join_all_time += run_time2
                optimal_time += min(run_time1, run_time2)
                if abs(run_time1 - run_time2) <= 0.1:
                    optimal[tbl_name] = ["BOTH", run_time1]
                elif run_time2 <= run_time1:
                    optimal[tbl_name] = ["JOIN_ALL", run_time2, run_time1 - run_time2]
                else:
                    optimal[tbl_name] = ["FIND_JOIN", run_time1, run_time2 - run_time1]

            dump_json(
                f"evaluation/run_time/{data_source}/{input}/ground_truth_{granu_list[0]}_{granu_list[1]}_{o_t}.json",
                optimal,
            )
            print(f"optimal_time: {optimal_time}; index search time: {index_search_time}; join all time: {join_all_time};")


def test_cost_model(input):
    data_source = "chicago_1m"
    config = io_utils.load_config(data_source)
    conn_str = config["db_path"]
    granu_lists = [[TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK]]
    st_schemas = io_utils.load_pickle(
        f"evaluation/input/{data_source}/full_st_schemas.json"
    )
    for granu_list in granu_lists:
        profiler = Profiler(data_source, granu_list[0], granu_list[1])
        # sort st_schemas
        sorted_st_schemas = []
        for st_schema in tqdm(st_schemas):
            tbl, schema = st_schema[0], st_schema[1]
            cnt = profiler.get_row_cnt(tbl, schema)
            sorted_st_schemas.append((st_schema, cnt))
        sorted_st_schemas = sorted(sorted_st_schemas, key=lambda x: x[1], reverse=False)
        for o_t in overlaps:
            ground_truth = load_json(
                f"evaluation/run_time/{data_source}/{input}/ground_truth_{granu_list[0]}_{granu_list[1]}_{o_t}.json"
            )
            join_costs = profiler.get_join_cost(granu_list[0], granu_list[1], o_t)
            join_method = FIND_JOIN_METHOD.COST_MODEL
            print(f"current join method: {join_method}; overlap threshold: {o_t}")
            corr_search = CorrSearch(
                conn_str,
                data_source,
                join_method,
                join_costs,
                "AGG",
                "MATRIX",
                ["impute_avg", "impute_zero"],
                False,
                "FDR",
                0.05,
            )
            overhead = 0
            correct = 0
            both = 0
            wrong = 0
            wrong_list = []
            lost_time = 0
            for st_schema in tqdm(sorted_st_schemas):
                tbl, schema = st_schema[0]
                agg_name = schema.get_agg_tbl_name(tbl)
                if agg_name not in join_costs:
                    continue
                best_method = ground_truth[agg_name][0]
                print("best_method", best_method)
                print(agg_name)
                _start = time.time()
                if agg_name not in join_costs:
                    continue
                method, schemas = corr_search.determine_find_join_method(
                    tbl, schema, o_t, join_costs[agg_name].cnt
                )
                _end = time.time()
                overhead += _end - _start
                if best_method == "BOTH":
                    both += 1
                else:
                    if best_method != method:
                        wrong += 1
                        lost_time += ground_truth[agg_name][2]
                        wrong_list.append(
                            (
                                agg_name,
                                ground_truth[agg_name][0],
                                ground_truth[agg_name][2],
                                join_costs[agg_name].cnt,
                            ),
                        )
                    else:
                        correct += 1
            print(
                f"correct: {correct}, both: {both}, wrong: {wrong}, ratio: {(correct+both)/(correct+wrong+both)}, overhead: {overhead}, lost_time: {lost_time}"
            )
            print(wrong_list)
            print(corr_search.perf_profile)


if __name__ == "__main__":
    # get_ground_truth("full_st_schemas")
    test_cost_model("full_st_schemas")
