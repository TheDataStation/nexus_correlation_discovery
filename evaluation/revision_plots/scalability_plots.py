import pandas as pd
from nexus.data_search.commons import FIND_JOIN_METHOD
from nexus.utils.coordinate import SPATIAL_GRANU

from utils.time_point import TEMPORAL_GRANU
from evaluation.plot_lib.plot_utils import Stages, load_data
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['font.family'] = 'Times New Roman'
mpl.rcParams['font.size'] = 25

ext = 'pdf'

def nexus_lazo_compare():
    t_granu, s_granu = TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT
    join_method = FIND_JOIN_METHOD.COST_MODEL
    o_t = 10
    validate = "True"
    jc_threshold = 0.2
    data_sources = ['nyc_open_data', 'chicago_open_data']
    find_join, validation, materialization, correlation, other, total = [], [], [], [], [], []
    nexus_path = f"evaluation/runtime12_29/{'_'.join(data_sources)}/full_tables/perf_time_{t_granu}_{s_granu}_{join_method}_{o_t}_0.0.json"
    nexus_run_time = load_data(nexus_path, [Stages.TOTAL, Stages.FIND_JOIN, Stages.VALIDATION, Stages.MATERIALIZATION, Stages.CORRELATION])
    load_array(nexus_run_time, find_join, validation, materialization, correlation, other, total)
    lazo_path = f"evaluation/runtime12_29/{'_'.join(data_sources)}/full_tables/perf_time_{t_granu}_{s_granu}_lazo_jc_{jc_threshold}_{o_t}_{validate}.json"
    lazo_run_time_validate = load_data(lazo_path, [Stages.TOTAL, Stages.FILTER, Stages.VALIDATION, Stages.MATERIALIZATION, Stages.CORRELATION], mode='lazo', granu_list=[t_granu, s_granu], jc_t=jc_threshold, data_sources= data_sources, validate=validate)
    load_array(lazo_run_time_validate, find_join, validation, materialization, correlation, other, total)
    # lazo_path = f"evaluation/runtime12_29/{'_'.join(data_sources)}/full_tables/perf_time_{t_granu}_{s_granu}_lazo_jc_{jc_threshold}_{o_t}_False.json"
    # lazo_run_time_no_validate = load_data(lazo_path, [Stages.TOTAL, Stages.FILTER, Stages.VALIDATION, Stages.MATERIALIZATION, Stages.CORRELATION], mode='lazo', granu_list=[t_granu, s_granu], jc_t=jc_threshold, data_sources= data_sources, validate="False")
    # load_array(lazo_run_time_no_validate, find_join, validation, materialization, correlation, other, total)
    # df = pd.DataFrame({'find_join': find_join, 'validation': validation, 'materialization': materialization, 'correlation': correlation, 'other': other}, index=['Nexus', 'Lazo+Validate', 'Lazo+NoValidate'])
    df = pd.DataFrame({'Filter': find_join, 'Validate': validation, 'Materialize': materialization, 'Correlation': correlation}, index=['Nexus', 'CorrLazo'])
    print(df)
    offset = 0
    ax = df.plot.barh(stacked=True, rot=90, figsize=(15,5), edgecolor='black', align='center')
    for i, bar in enumerate(ax.patches):
        print(i, bar.get_width())
        if bar.get_width() == 0:
            continue
        if i == 0:
            offset = 40
        if i == 3:
            offset = 105
        ax.text(bar.get_x() + bar.get_width() / 2 + offset,
            bar.get_y()+0.2,
            round(bar.get_width(), 1), ha = 'center',
            color = 'w', weight = 'bold', size = 10)
    for i in range(6, 8):
        print(i)
        bar = ax.patches[i]
        ax.text(bar.get_x() + bar.get_width() + 95,
            bar.get_y() + 0.2,
            round(total[i-6], 1), ha = 'center',
            color = 'black', weight = 'bold')
    
    ax.set_xlim(0, 1950)
    ax.legend(ncol=4, bbox_to_anchor=(0.5, 1.22), loc='upper center')
    ax.set_xlabel('Runtime (s)')
    for text_obj in ax.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        text_obj.set_fontsize(30)
    plt.savefig(f'evaluation/final_plots/lazo_nexus_large.{ext}', bbox_inches="tight")

def load_array(run_time_profile, find_join, validation, materialization, correlation, other, total):
    find_join.append(run_time_profile[1])
    validation.append(run_time_profile[2])
    materialization.append(run_time_profile[3])
    correlation.append(run_time_profile[4])
    # correction.append(nexus_run_time[4])
    other.append(run_time_profile[0] - run_time_profile[1] - run_time_profile[2] - run_time_profile[3] - run_time_profile[4])
    total.append(run_time_profile[0])


def nexus_scalability_plot():
    t_granu, s_granu = TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT
    join_method = FIND_JOIN_METHOD.COST_MODEL
    o_t = 10
    find_join, materialization, correlation, other, total = [], [], [], [], []
    data_sources_l = [['chicago_1m'], ['nyc_open_data', 'chicago_open_data'], ['all_sources']]
    for data_sources in data_sources_l:
        nexus_path = f"evaluation/runtime12_29/{'_'.join(data_sources)}/full_tables/perf_time_{t_granu}_{s_granu}_{join_method}_{o_t}_0.0.json"
        nexus_run_time = load_data(nexus_path, [Stages.TOTAL, Stages.FIND_JOIN, Stages.MATERIALIZATION, Stages.CORRELATION])
        find_join.append(nexus_run_time[1])
        materialization.append(nexus_run_time[2])
        correlation.append(nexus_run_time[3])
        # correction.append(nexus_run_time[4])
        other.append(nexus_run_time[0] - nexus_run_time[1] - nexus_run_time[2] - nexus_run_time[3])
        total.append(nexus_run_time[0])
    df = pd.DataFrame({'Filter': find_join, 'Materialization': materialization, 'Correlation': correlation}, index=['Chicago', 'Chicago+NYC', 'OpenDataLarge'])
    print(df)
    ax = df.plot.bar(stacked=True, rot=0, logy=True, edgecolor='black', figsize=(9,9))
    offset = 3
    for i, bar in enumerate(ax.patches):
        print(i, bar.get_height(), bar.get_height() / 2 + bar.get_y())
        # annotate total time
        # if i == 9 or i == 10 or i == 11:
        # if i == 6 or i == 7 or i == 8:
        #     ax.text(bar.get_x() + bar.get_width() / 2,
        #         bar.get_height() + bar.get_y()+12,
        #         round(total[i-9], 1), ha = 'center',
        #         color = 'black', weight = 'bold', size = 10)
        # else:
        if i == 0:
            offset = 8
        if i == 6:
            offset=0
        if i in range(7,10):
            offset = 1
        ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height()/3+ bar.get_y()+offset,
            round(bar.get_height(), 1), ha = 'center',
            color = 'w', weight = 'bold', size = 10)
    
    for i in range(6, 9):
        print(i)
        bar = ax.patches[i]
        ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + bar.get_y()+10,
            round(total[i-6], 1), ha = 'center',
            color = 'black', weight = 'bold')
    
    ax.set_ylabel('Runtime (s) (log scale)')
    # plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3)
    for text_obj in ax.findobj(mpl.text.Text):
        text_obj.set_fontweight('bold')
        text_obj.set_fontsize(24)
    plt.savefig('evaluation/plots/nexus_scalability.png',  bbox_inches="tight")


if __name__ == "__main__":
    # nexus_scalability_plot()
    nexus_lazo_compare()
    # df = pd.DataFrame(columns=['Context', 'Parameter', 'Val1', 'Val2', 'Val3'],
    #                 data=[[1, 'JC=0.0', 43.312347, 9.507902, 1.580367],
    #                         [2,'JC=0.0', 42.862649, 9.482205, 1.310549],
    #                         [1, 'JC=0.2', 43.710651, 9.430811, 1.400488],
    #                         [2, 'JC=0.2', 43.209559, 9.803418, 1.349094],
                     
    #                         ]
    #                         )
    # # df.groupby(['Context', 'Parameter']).size().unstack().plot(kind='bar', stacked=True)
    # df.set_index(['Context', 'Parameter'], inplace=True)
    # df0 = df.reorder_levels(['Parameter', 'Context']).sort_index()
    # print(df0)
    # colors = plt.cm.Paired.colors

    # df0 = df0.unstack(level=-1) # unstack the 'Context' column
    # # df0 = df0.unstack(level=0)
    # fig, ax = plt.subplots()
    # (df0['Val1']+df0['Val2']+df0['Val3']).plot(kind='bar', color=[colors[1], colors[0]], rot=0, ax=ax, edgecolor='black')
    # (df0['Val2']+df0['Val3']).plot(kind='bar', color=[colors[3], colors[2]], rot=0, ax=ax, edgecolor='black')
    # df0['Val3'].plot(kind='bar', color=[colors[5], colors[4]], rot=0, ax=ax, edgecolor='black')

    # legend_labels = [f'{val} ({context})' for val, context in df0.columns]
    # ax.legend(legend_labels, loc='upper center', bbox_to_anchor=(0.5, 1.1), ncol=3)


    # Create a sample DataFrame
    # data = {'Category': ['A', 'A', 'B', 'B', 'C', 'C'],
    #         'Subcategory': ['X', 'Y', 'X', 'Y', 'X', 'Y'],
    #         'Value': [10, 15, 12, 8, 20, 18]}
    # df = pd.DataFrame(data)

    # # Pivot the DataFrame to create a grouped stacked bar plot
    # pivot_df = df.pivot(index='Category', columns='Subcategory', values='Value')

    # # Plot the grouped stacked bar plot
    # ax = pivot_df.plot(kind='bar', stacked=True)

    # # Customize the plot (labels, legend, etc.)
    # plt.xlabel('Category')
    # plt.ylabel('Value')
    # plt.title('Grouped Stacked Bar Plot')
    # plt.legend(title='Subcategory')

    # # Show the plot
    # plt.show()