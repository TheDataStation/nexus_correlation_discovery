from data_ingestion.data_profiler import Profiler
from data_search.commons import FIND_JOIN_METHOD
from evaluation.persist_correlations import load_lazo_join_res
from utils import io_utils
from utils.spatial_hierarchy import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from evaluation.plot_lib.plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
import matplotlib.pyplot as plt

def lazo_corr_sketch_runtime_comparison():
    granu_list = [TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT]
    # granu_list = [T_GRANU.MONTH, S_GRANU.TRACT]
    o_t, r_t = 10, 0.0
    find_join_method = FIND_JOIN_METHOD.COST_MODEL
    data_source = "chicago_1m"
    storage_dir = "runtime12_29"
    sketch_size = 256
    sketch_perf_path = f"evaluation/{storage_dir}/{data_source}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{o_t}_{r_t}_{sketch_size}_correlation_sketch.json"
    nexus_perf_path = f"evaluation/{storage_dir}/{data_source}/full_tables/perf_time_{granu_list[0]}_{granu_list[1]}_{find_join_method}_{o_t}_{r_t}.json"

    vars = [Stages.TOTAL, Stages.FIND_JOIN, Stages.MATERIALIZATION, Stages.CORRELATION]
    # vars = [Stages.MATERIALIZATION]
    perf_sketch = load_data(sketch_perf_path, vars, mode='sketch', granu_list=granu_list)
    perf_nexus = load_data(nexus_perf_path, vars)
    values = np.array([perf_sketch, perf_nexus])
    fig, axs = plt.subplots(1, 1, sharey=True, figsize=(15, 5))
    # print(values)
    params = {
        "ylabel": "Run time(s)",
        # "title": f"Overlap threshold {o_t}",
        "save_path": f"lazo_eval/figure/{data_source}_{granu_list[0]}_{granu_list[1]}_corr_sketch_nexus_run_time_comparison.png",
    }
    # make a bar plot for values using matplotlib
    # print(perf_nexus)
    # plt.bar(['Lazo', 'Ground Truth Join Time'], [perf_lazo[0], perf_nexus[0]])
    # plt.savefig(params["save_path"])
    grouped_bar_plot(
        axs, ['Sketch', 'Nexus'], [var.value for var in vars], values, params
    )

if __name__ == '__main__':
    lazo_corr_sketch_runtime_comparison()