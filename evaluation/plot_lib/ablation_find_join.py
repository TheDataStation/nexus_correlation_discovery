from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
from data_search.commons import FIND_JOIN_METHOD
import matplotlib.pyplot as plt

plt.rcParams["font.family"] = "Times New Roman"
plt.rcParams["font.size"] = 25
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

data_source = "chicago_1m"

vars = [Stages.FIND_JOIN_AND_MATER, Stages.FIND_JOIN, Stages.MATERIALIZATION]

values = [[], [], []]
for i, o_t in enumerate([10, 100, 1000]):
    labels = [
        FIND_JOIN_METHOD.INDEX_SEARCH,
        FIND_JOIN_METHOD.JOIN_ALL,
        FIND_JOIN_METHOD.COST_MODEL,
    ]
    data_list = []
    opt_join_time = None
    for label in labels:
        data = load_data(
            f"/home/cc/nexus_correlation_discovery/evaluation/run_time/{data_source}/full_st_schemas/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_{label.value}_{o_t}.json",
            vars,
        )
        if label == FIND_JOIN_METHOD.INDEX_SEARCH:
            values[0].append(data[1])
            opt_join_time = data[2]
        elif label == FIND_JOIN_METHOD.JOIN_ALL:
            values[1].append(data[0]-opt_join_time)
        else:
            values[2].append(data[0]-opt_join_time)
      
params = {
    "ylabel": "Run time(s)",
    "xlabel": "Overlap Threshold",
    # "title": f"Run time comparison on {data_source} overlap threshold: {o_t}",
    "save_path": f"/home/cc/nexus_correlation_discovery/evaluation/camera_ready_plots/{data_source}_run_time_comp.pdf",
}

fig, ax = plt.subplots(figsize=(9, 5))

grouped_bar_plot(
    ax, ["Index Search", "Exhaustive Join", "Cost Model"], ["10", "100", "1000"],  np.array(values), params
)