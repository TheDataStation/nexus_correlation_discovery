import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import utils.io_utils as io_utils
from enum import Enum


class Stages(Enum):
    TOTAL = "Total Time"
    FIND_JOIN_AND_MATER = "Find Join+Materialization"
    FIND_JOIN = "Find Join"
    FILTER = "Filter"
    VALIDATION = "Validation"
    MATERIALIZATION = "Materialization"
    CORRELATION = "Correlation"
    CORRECTION = "Correction"


def load_data(path, vars, mode=None, granu_list=None, jc_t=None, data_sources=None, validate=None):
    profile = io_utils.load_json(path)
    data = []
    for var in vars:
        if var == Stages.TOTAL:
            if mode =='lazo':
                if validate == "False":
                    path = f"evaluation/lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{granu_list[0].value}_space_{granu_list[1].value}/jc_{jc_t}_perf_validate_{validate.lower()}.json"
                else:
                    path = f"evaluation/lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{granu_list[0].value}_space_{granu_list[1].value}/jc_{jc_t}_perf.json"
                res = io_utils.load_json(path)
                data.append(profile["total_time"] + res["TotalTime"]/1000 - profile["time_dump_csv"]["total"])
            elif mode == 'sketch':
                path = f"evaluation/lazo_eval/find_joinable_time_{granu_list[0]}_{granu_list[1]}_overlap_30.json"
                res = io_utils.load_json(path)
                data.append(profile["total_time"] + res["total_time"]-profile["time_dump_csv"]["total"])
            else:
                data.append(profile["total_time"]-profile["time_dump_csv"]["total"])
        elif var == Stages.FIND_JOIN_AND_MATER:
            data.append(
                profile["time_find_joins"]["total"] + profile["time_join"]["total"]
            )
        elif var == Stages.FIND_JOIN:
            if mode =='lazo':
                path = f"evaluation/lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{granu_list[0].value}_space_{granu_list[1].value}/jc_{jc_t}_perf.json"
                res = io_utils.load_json(path)
                data.append(profile["time_find_joins"]["total"] + res["TotalTime"]/1000)
            elif mode == 'sketch':
                path = f"evaluation/lazo_eval/find_joinable_time_{granu_list[0]}_{granu_list[1]}_overlap_30.json"
                res = io_utils.load_json(path)
                data.append(profile["time_find_joins"]["total"] + res["total_time"])
            else:
                data.append(
                    profile["time_find_joins"]["total"],
                )
        elif var == Stages.FILTER:
            if validate is "False":
                path = f"evaluation/lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{granu_list[0].value}_space_{granu_list[1].value}/jc_{jc_t}_perf_validate_{validate.lower()}.json"
            else:
                path = f"evaluation/lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{granu_list[0].value}_space_{granu_list[1].value}/jc_{jc_t}_perf.json"
            res = io_utils.load_json(path)
            data.append(res["filterTime"]/1000)
        elif var == Stages.VALIDATION:
            if mode =='lazo':
                if validate == "False":
                    path = f"evaluation/lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{granu_list[0].value}_space_{granu_list[1].value}/jc_{jc_t}_perf_validate_{validate.lower()}.json"
                else:
                    path = f"evaluation/lazo_eval/lazo_join_res/{'_'.join(data_sources)}/time_{granu_list[0].value}_space_{granu_list[1].value}/jc_{jc_t}_perf.json"
                res = io_utils.load_json(path)
                data.append(res["validateTime"]/1000)
            else:
                data.append(0)
        elif var == Stages.MATERIALIZATION:
            data.append(profile["time_join"]["total"])
        elif var == Stages.CORRELATION:
            data.append(profile["time_correlation"]["total"])
        elif var == Stages.CORRECTION:
            data.append(profile["time_correction"]["total"])
    print("data", data)
    return data


def grouped_bar_plot(ax, categories, group_names, values, params):
    # Set the bar width and spacing
    bar_width = 0.31
    space = 0
    print(categories)
    print(values)
    # Calculate the positions of the bars on the x-axis
    x = np.arange(len(group_names))
    hatches = ['/', '-', '|']
    # colors = ['royalblue', 'salmon', 'gold']
    colors = ['gold', 'royalblue']
    # Plot the bars for each category and group
    for i, category in enumerate(categories):
        offset = (bar_width + space) * i
        ax.bar(x + offset, values[i], width=bar_width, label=category, edgecolor='black', hatch=hatches[i], color=colors[i])

    # Set the x-axis ticks and labels
    ax.set_xticks(
        x + bar_width/2,
        group_names,
        fontsize=30
    )
    def custom_formatter(value):
        return f"{value:.1f}"


    # Add labels, title, and legend
    if "xlabel" in params:
        ax.set_xlabel(params["xlabel"], fontsize=35)
    if "ylabel" in params:
        ax.set_ylabel(params["ylabel"], fontsize=35)
        ax.set_ylim(0, 3200)
    if "ylim" in params:
        s, e = params["ylim"][0], params["ylim"][1]
        ax.set_ylim(s, e)
    if "title" in params:
        ax.set_title(params["title"])
    
    for text_obj in ax.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        # text_obj.set_fontsize(30)

    if "legend" in params:
        ax.legend(loc=params["legend"])
    else:
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.21), ncol=3, prop={"weight": 'bold', 'size': 23})
    


    for bars in ax.containers:
        ax.bar_label(bars, fmt=custom_formatter, weight='bold', fontsize=23)
    if "save_path" in params:
        plt.savefig(params["save_path"], bbox_inches="tight")
    # Show the plot
    # plt.show()
