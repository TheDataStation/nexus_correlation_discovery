from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
import matplotlib.pyplot as plt

data_source = "chicago_1m"
root_path = "/home/cc/nexus_correlation_discovery"
vars = [Stages.CORRELATION]
labels = ["10", "100", "1000"]
o_t_l = [10, 100, 1000]

with_vec = []
without_vec = []

input = 'full_st_schemas'

for o_t in o_t_l:
    x1 = load_data(
        f"{root_path}/evaluation/run_time/{data_source}/{input}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_Cost_Model_{o_t}.json",
        vars,
    )
    with_vec.append(x1[0])

    x2 = load_data(
        f"{root_path}/evaluation/run_time/{data_source}/{input}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_Cost_Model_{o_t}_no_vec.json", vars
    )
    without_vec.append(x2[0])

print(with_vec, without_vec)

values = np.array([with_vec, without_vec])
print(values)

params = {
    "ylabel": "Run time(s)",
    "xlabel": "Overlap Threshold",
    # "title": f"Run time comparison on {data_source} overlap threshold: {o_t}",
    "save_path": f"{root_path}/evaluation/plots/{data_source}_run_time_vec.png",
}

fig, ax = plt.subplots(nrows=1, ncols=1, squeeze=True)

grouped_bar_plot(
    ax, ["Vectorization", "Pair-Wise"], [var for var in labels], values, params
)

# plt.savefig(f"{root_path}/evaluation/plots/{data_source}_run_time_vectorization_comp.png")