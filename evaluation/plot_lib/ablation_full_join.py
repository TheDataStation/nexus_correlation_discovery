from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams["font.family"] = "Times New Roman"
plt.rcParams["font.size"] = 25
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42

data_source = "chicago_1m"
root_path = "/home/cc/nexus_correlation_discovery/"
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
    "xlabel": "Stages",
    # "title": f"Run time comparison on {data_source} overlap threshold: {o_t}",
    "save_path": f"{root_path}/evaluation/camera_ready_plots/{data_source}_run_time_corr.pdf",
}

fig, ax = plt.subplots(figsize=(9, 5))

grouped_bar_plot(
    ax, ["Data Profiles+Inner Join", "Outer Join"], ["Total Time", "Materialization", "Correlation"], values, params
)

# plt.savefig(f"{root_path}/evaluation/plots/{data_source}_run_time_outer_join_comp.png")