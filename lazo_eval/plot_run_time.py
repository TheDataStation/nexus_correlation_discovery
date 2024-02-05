from matplotlib import pyplot as plt
from utils import io_utils
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
import matplotlib as mpl
import pandas as pd

mpl.rcParams['font.family'] = 'Times New Roman'
mpl.rcParams['font.size'] = 20

def lazo_joinable_run_time(data_source, t_granu, s_granu):
    fig, ax = plt.subplots()
    lazo_thresholds = [0.0, 0.2, 0.4, 0.6]
    jc_values = ["jc=0", "jc=0.2", "jc=0.4", "jc=0.6"]
    filter_time = []
    validate_time = []
    for t in lazo_thresholds:
        path = f"lazo_eval/lazo_join_res/{data_source}/time_{t_granu.value}_space_{s_granu.value}/jc_{t}_perf.json"
        res = io_utils.load_json(path)
        filter_time.append(res["filterTime"]/1000)
        validate_time.append(res["validateTime"]/1000)
    
    df = pd.DataFrame({'Filter Time': filter_time, 'Validate Time': validate_time}, index=jc_values)
    # # Create a bar plot for filter times
    # ax.bar(jc_values, filter_time, label='Filter Time', width=0.5)

    # # Stack validate times on top of filter times
    # ax.bar(jc_values, validate_time, bottom=filter_time, label='Validate Time', width=0.5)
    ax = df.plot.bar(stacked=True, rot=0)
    # Annotate the filter time on each bar
    for rect, runtime in zip(ax.patches, filter_time):
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width() / 2, height / 2, str(round(runtime, 1)),
                ha='center', va='center', color='white')

    # Annotate the validate time on each stacked bar
    for rect, runtime in zip(ax.patches[len(filter_time):], validate_time):
        height = rect.get_height()
        plt.text(rect.get_x() + rect.get_width() / 2, rect.get_y() + height / 2, str(round(runtime, 1)),
                ha='center', va='center', color='white')
        
    plt.xlabel('Jaccard Containment Threshold')
    plt.ylabel('Runtime(s)')
    # plt.title('Join Search Runtime')
    plt.legend()
    plt.savefig(f'lazo_eval/figure/jc_runtime_{t_granu}_{s_granu}.png')

if __name__ == "__main__":
    # t_granu, s_granu = T_GRANU.DAY, S_GRANU.BLOCK
    t_granu, s_granu = T_GRANU.MONTH, S_GRANU.TRACT
    lazo_joinable_run_time(t_granu, s_granu)