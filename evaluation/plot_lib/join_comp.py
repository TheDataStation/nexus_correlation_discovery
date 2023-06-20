from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
from data_search.commons import FIND_JOIN_METHOD
import matplotlib.pyplot as plt

data_source = "chicago_1m"

vars = [Stages.TOTAL, Stages.FIND_JOIN, Stages.MATERIALIZATION]

fig, axs = plt.subplots(1, 3, sharey=True, figsize=(15, 5))

for i, o_t in enumerate([10, 100, 1000]):
    ax = axs[i]
    labels = [
        FIND_JOIN_METHOD.INDEX_SEARCH,
        FIND_JOIN_METHOD.JOIN_ALL,
        FIND_JOIN_METHOD.COST_MODEL,
    ]
    data_list = []
    for label in labels:
        data = load_data(
            f"../run_time/{data_source}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_{label.value}_{o_t}.json",
            vars,
        )
        data_list.append(data)

    values = np.array(data_list)

    params = {
        "ylabel": "Run time(s)",
        "title": f"Overlap threshold {o_t}",
        "save_path": f"../plots/{data_source}_run_time_{o_t}.png",
    }

    grouped_bar_plot(
        ax, [l.value for l in labels], [var.value for var in vars], values, params
    )

plt.savefig(f"../plots/{data_source}_run_time_comp.png")
plt.show()
