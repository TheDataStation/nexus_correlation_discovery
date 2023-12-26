from matplotlib import pyplot as plt
from utils import io_utils

def lazo_joinable_run_time():
    fig, ax = plt.subplots()
    lazo_thresholds = [0.0, 0.1, 0.2]
    jc_values = ["jc=0", "jc=0.1", "jc=0.2"]
    filter_time = []
    validate_time = []
    for t in lazo_thresholds:
        path = f"lazo_eval/lazo_join_res/jc_{t}_perf.json"
        res = io_utils.load_json(path)
        filter_time.append(res["filterTime"]/1000)
        validate_time.append(res["validateTime"]/1000)
    
    # Create a bar plot for filter times
    ax.bar(jc_values, filter_time, label='Filter Time', width=0.5)

    # Stack validate times on top of filter times
    ax.bar(jc_values, validate_time, bottom=filter_time, label='Validate Time', width=0.5)
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
    plt.title('Join Search Runtime')
    plt.legend()
    plt.savefig('lazo_eval/figure/jc_runtime.png')

if __name__ == "__main__":
    lazo_joinable_run_time()