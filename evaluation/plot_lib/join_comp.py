from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np

data_source = "chicago_1m"

vars = [Stages.TOTAL, Stages.FIND_JOIN, Stages.MATERIALIZATION]
for o_t in [10, 100, 1000]:
    index_search_data = load_data(
        f"../run_time/{data_source}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_FIND_JOIN_{o_t}.json",
        vars,
    )

    join_all_data = load_data(
        f"../run_time/{data_source}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_JOIN_ALL_{o_t}.json",
        vars,
    )

    values = np.array([index_search_data, join_all_data])

    params = {
        "ylabel": "Run time(s)",
        "title": f"Run time comparison on {data_source} with overlap threshold {o_t}",
        "save_path": f"../plots/{data_source}_run_time_{o_t}.png",
    }

    grouped_bar_plot(
        ["Index Search", "JOIN ALL"], [var.value for var in vars], values, params
    )
