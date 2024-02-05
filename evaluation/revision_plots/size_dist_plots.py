import matplotlib.pyplot as plt
import json
import matplotlib as mpl
import numpy as np

mpl.rcParams['font.family'] = 'Times New Roman'
mpl.rcParams['font.size'] = 16

def plot_dist(ax, values):
    # ax.hist(values, bins=50)
    # create a box plot on values
    green_diamond = dict(markerfacecolor='g', marker='D')
    ax.boxplot(values, flierprops=green_diamond)
    ax.set_yscale('log')

def plot_size_dist_in_one():
    fig, axs = plt.subplots(1, 1, figsize=(5, 5))
    sizes = []
    for i, category in enumerate(["temporal", "spatial", "st"]):
        with open(f"{category}_set_size.json") as f:
            data = json.load(f)
        sizes.extend(data.values())
    data = np.array(sizes)
    Q1 = np.percentile(data, 25)
    Q3 = np.percentile(data, 75)
    IQR = Q3 - Q1
    outliers = data[(data < Q1 - 1.5 * IQR) | (data > Q3 + 1.5 * IQR)]
    num_outliers = len(outliers)
    print(len(sizes), len([size for size in sizes if size > 10000]), num_outliers, max(sizes))
    plot_dist(axs, sizes)
    # axs[i].set_title(category)
    axs.set_xticks([1], ["Granularity: Day, Census Block"], fontsize=25)
    axs.set_ylabel("#Rows (log scale)", fontsize=25)
    for text_obj in axs.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
    
    plt.savefig('evaluation/final_plots/size_dist.pdf',  bbox_inches="tight")

if __name__ == "__main__":
    plot_size_dist_in_one()