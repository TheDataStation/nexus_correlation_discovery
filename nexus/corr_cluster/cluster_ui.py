import ipywidgets as widgets
from IPython.display import display, clear_output
from nexus.corr_cluster.cluster_utils import CorrCommunity
import json

def show_df(df, name, prov=None):
    display(df)
    if prov:
        print("provenance:")
        display(prov)

    def download_data(b):
        df.to_csv(f'{name}.csv', index=False)
        if prov:
            json.dump(prov, open(f'{name}_prov.json', 'w'))
    # show a download button to download the dataframe
    download_button = widgets.Button(description="Download Data")
    download_button.on_click(download_data)
    display(download_button)
        
def show_communities(corr_community: CorrCommunity, show_corr_in_same_tbl):
    clusters = corr_community.all_communities
    # Function to be triggered when the dropdown value changes
    def on_dropdown_change(change):
        if change["type"] == "change" and change["name"] == "value":
            with main_output:
                clear_output(wait=True)
                cluster_name = change["new"]
                print(cluster_name)

                for table in clusters[cluster_name]:
                    print(f"  - {table}")

                # Create a button and set its click action for the selected cluster
                btn = widgets.Button(description="Show Variables")
                btn.on_click(
                    lambda change, cluster_name=cluster_name: on_button_click(
                        cluster_name, change
                    )
                )

                btn_corr = widgets.Button(description="Show Correlations")
                btn_corr.on_click(
                    lambda change, cluster_name=cluster_name: on_corr_button_click(
                        cluster_name, change
                    )
                )

                display(btn)
                display(btn_corr)
            with variable_output:
                clear_output()
            with corr_output:
                clear_output()

    # Function to be triggered when the button is clicked
    def on_button_click(cluster_name, btn_object):
        with variable_output:
            clear_output(wait=True)
            cluster = clusters[cluster_name]
            for table, variables in cluster.items():
                print(f"{table}")
                for var in variables:
                    print(f" - {var}")
                # print(f"{table}: {', '.join(variables)}")

    def on_corr_button_click(cluster_name, btn_object):
        with corr_output:
            clear_output(wait=True)
            cluster_id = int(cluster_name.split()[1])
            res = corr_community.get_corr_in_cluster_i(
                cluster_id, show_corr_in_same_tbl
            )
            display(f"{cluster_name} has {len(res)} correlations")
            display(res)
          
    # Create the dropdown widget and set the initial value
    dropdown = widgets.Dropdown(
        options=list(clusters.keys()),
        description="Show:",
        layout=widgets.Layout(
            width="200px"
        ),  # Adjust width to fit the dropdown content
    )
    dropdown.observe(on_dropdown_change)

    # Create output widgets to hold main content and variable details
    main_output = widgets.Output()
    variable_output = widgets.Output()
    corr_output = widgets.Output()

    # Display dropdown and outputs
    display(dropdown)
    display(main_output)
    display(variable_output)
    display(corr_output)

    # Trigger the display for the default cluster (Cluster 1) when the cell is run
    on_dropdown_change({"type": "change", "name": "value", "new": "Cluster 0"})