from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np

data_source = "chicago_1m"

vars = [Stages.TOTAL, Stages.FIND_JOIN, Stages.MATERIALIZATION, Stages.CORRELATION]

no_outer_join = load_data(
    f"../run_time/{data_source}/perf_time_outer_join_False.json",
    vars,
)

outer_join = load_data(
    f"../run_time/{data_source}/perf_time_outer_join_True.json", vars
)

values = np.array([no_outer_join, outer_join])
print(values)

params = {
    "ylabel": "Run time(s)",
    "title": f"Run time comparison on {data_source}",
    "save_path": f"../plots/{data_source}_run_time_corr.png",
}

grouped_bar_plot(
    ["Use Stats", "Outer Join"], [var.value for var in vars], values, params
)
