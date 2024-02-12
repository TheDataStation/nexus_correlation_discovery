from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
import matplotlib.pyplot as plt

data_source = "chicago_1m"
root_path = "/home/cc/nexus_correlation_discovery"
vars = [Stages.TOTAL, Stages.MATERIALIZATION, Stages.CORRELATION]
o_t = 100
input = 'full_st_schemas'
no_outer_join = load_data(
    f"{root_path}/evaluation/run_time/{data_source}/{input}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_Index_Search_100_inner_join.json",
    vars,
)

outer_join = load_data(
    f"{root_path}/evaluation/run_time/{data_source}/{input}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_Index_Search_100_outer_join.json", vars
)

values = np.array([no_outer_join, outer_join])
print(values)

params = {
    "ylabel": "Run time(s)",
    # "title": f"Run time comparison on {data_source} overlap threshold: {o_t}",
    "save_path": f"{root_path}/evaluation/plots/{data_source}_run_time_corr.png",
}

fig, ax = plt.subplots(nrows=1, ncols=1, squeeze=True)

grouped_bar_plot(
    ax, ["Data Profiles+Inner Join", "Outer Join"], [var.value for var in vars], values, params
)

# plt.savefig(f"{root_path}/evaluation/plots/{data_source}_run_time_outer_join_comp.png")