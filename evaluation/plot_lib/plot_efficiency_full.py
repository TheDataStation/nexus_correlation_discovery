from plot_utils import Stages, load_data, grouped_bar_plot
import numpy as np
import matplotlib.pyplot as plt
import json
import matplotlib.font_manager
matplotlib.font_manager._rebuild()

plt.rcParams["font.family"] = "Times New Roman"
plt.rcParams["font.size"] = 12

data_source = "chicago_1m"
input_path = "/home/cc/resolution_aware_spatial_temporal_alignment/evaluation/run_time/chicago_1m/full_st_schemas/"
overlap_ts = [10, 100, 1000]
# Define the label locations and the width of the bars
labels = ['10', '100', '1000']
x = np.arange(len(labels))
width = 0.28

# Sample data for the bars
nexus = []
baseline = []

def load_data(method):
    if method == "baseline":
        for o_t in overlap_ts:
            path = f"{input_path}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_Index_Search_{o_t}_baseline.json"
            with open(path, 'r') as file:
                data = json.load(file)
            baseline.append(data["total_time"])
    elif method == "nexus":
        for o_t in overlap_ts:
            path = f"{input_path}/perf_time_T_GRANU.DAY_S_GRANU.BLOCK_Cost_Model_{o_t}.json"
            with open(path, 'r') as file:
                data = json.load(file)
            nexus.append(data["total_time"])

load_data("nexus")
load_data("baseline")
fig, ax = plt.subplots()

# Create bars
rects1 = ax.bar(x - width/2, nexus, width, label='Nexus', color='royalblue')
rects2 = ax.bar(x + width/2, baseline, width, label='Baseline', color='forestgreen')

# Add some text for labels, title, and custom x-axis tick labels, etc.
ax.set_xlabel('Overlap Thresholds')
ax.set_ylabel('Run time(s)')
# ax.set_title('Grouped bar chart')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

# Optionally add the value on top of each bar (you can remove this block if not needed)
def autolabel(rects):
    for rect in rects:
        height = round(rect.get_height(), 2)
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

autolabel(rects1)
autolabel(rects2)

fig.tight_layout()
plt.savefig(f"/home/cc/resolution_aware_spatial_temporal_alignment/evaluation/plots/{data_source}_run_time_full_comp.png")
