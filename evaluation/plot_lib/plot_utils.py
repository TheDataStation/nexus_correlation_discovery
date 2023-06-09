import matplotlib.pyplot as plt
import numpy as np
import utils.io_utils as io_utils
from enum import Enum


class Stages(Enum):
    TOTAL = "Total Time"
    FIND_JOIN = "Find Join"
    MATERIALIZATION = "Materialization"
    CORRELATION = "Correlation"
    CORRECTION = "Correction"


def load_data(path, vars):
    profile = io_utils.load_json(path)
    data = []
    for var in vars:
        if var == Stages.TOTAL:
            data.append(profile["total_time"])
        elif var == Stages.FIND_JOIN:
            data.append(
                profile["time_find_joins"]["total"],
            )
        elif var == Stages.MATERIALIZATION:
            data.append(profile["time_join"]["total"])
        elif var == Stages.CORRELATION:
            data.append(profile["time_correlation"]["total"])
        elif var == Stages.CORRECTION:
            data.append(profile["time_correction"]["total"])
    return data


def grouped_bar_plot(categories, group_names, values, params):
    # Set the bar width and spacing
    bar_width = 0.2
    space = 0

    # Calculate the positions of the bars on the x-axis
    x = np.arange(len(group_names))

    # Plot the bars for each category and group
    for i, category in enumerate(categories):
        offset = (bar_width + space) * i
        plt.bar(x + offset, values[i], width=bar_width, label=category)

    # Set the x-axis ticks and labels
    plt.xticks(x + ((bar_width + space) * len(categories) - space) / 2, group_names)

    # Add labels, title, and legend
    if "xlabel" in params:
        plt.xlabel(params["xlabel"])
    if "ylabel" in params:
        plt.ylabel(params["ylabel"])
    if "title" in params:
        plt.title(params["title"])
    plt.legend()
    if "save_path" in params:
        plt.savefig(params["save_path"])
    # Show the plot
    plt.show()
