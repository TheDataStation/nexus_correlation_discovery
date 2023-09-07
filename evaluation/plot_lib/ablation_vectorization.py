from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
import matplotlib.pyplot as plt

data_source = "chicago_1m"
root_path = "/home/cc/resolution_aware_spatial_temporal_alignment"
vars = [Stages.CORRELATION]

o_t = 100
input = 'full_st_schemas'
no_outer_join = load_data(
    f"{root_path}/evaluation/run_time/{data_source}/{input}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_Cost_Model_100.json",
    vars,
)

outer_join = load_data(
    f"{root_path}/evaluation/run_time/{data_source}/{input}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_Cost_Model_100_no_vec.json", vars
)

values = np.array([no_outer_join, outer_join])
print(values)

params = {
    "ylabel": "Run time(s)",
    "title": f"Run time comparison on {data_source} overlap threshold: {o_t}",
    "save_path": f"{root_path}/evaluation/plots/{data_source}_run_time_corr.png",
}

fig, ax = plt.subplots(nrows=1, ncols=1, squeeze=True)

grouped_bar_plot(
    ax, ["Use Stats", "Outer Join"], [var.value for var in vars], values, params
)

plt.savefig(f"{root_path}/evaluation/plots/{data_source}_run_time_vectorization_comp.png")