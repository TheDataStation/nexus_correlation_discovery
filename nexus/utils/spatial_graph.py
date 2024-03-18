import geopandas as gpd
from tqdm import tqdm
from nexus.utils.coordinate import SPATIAL_GRANU
from nexus.utils.io_utils import dump_json
from utils.time_point import TEMPORAL_GRANU

def build_spatial_graph(shapefile_path):
    # Load the shapefile
    gdf = gpd.read_file(shapefile_path)

    # Create an empty adjacency list
    adjacency_list = {}
    region_id_l = []
    # Iterate through the geometries (regions) in the GeoDataFrame
    for index, row in gdf.iterrows():
        # block = row["blockce10"]
        # tract = row["tractce10"]
        # county = row["countyfp10"]
        # state = row["statefp10"]
        tract = row["TRACTCE10"]
        county = row["COUNTYFP10"]
        state = row["STATEFP10"]
        # region_id = '-'.join(str(x) for x in [state, county, tract, block])
        region_id = '-'.join(str(x) for x in [state, county, tract])
        # Add a node for each region
        region_id_l.append(region_id)
        adjacency_list[region_id] = []  # Initialize an empty list for neighbors

    print(len(region_id_l), len(set(region_id_l)))
    # Iterate through the geometries again to find adjacent regions and add edges
    count = 0
    for index, row in tqdm(gdf.iterrows()):
        count += 1
        if count % 100 == 0:
            print(count)
        neighbors = gdf[gdf.geometry.touches(row['geometry'])]
        for neighbor_index, _ in neighbors.iterrows():
            if index != neighbor_index:  # Avoid adding self-loops
                adjacency_list[region_id_l[index]].append(region_id_l[neighbor_index])
                adjacency_list[region_id_l[neighbor_index]].append(region_id_l[index])  # If the graph is undirected, add both directions
    return adjacency_list


if __name__ == "__main__":
    # shapefile_path = '/home/cc/nexus_correlation_discovery/data/shape_chicago_blocks/geo_export_8e927c91-3aad-4b67-86ff-bf4de675094e.shp'
    # graph = load_json('/home/cc/nexus_correlation_discovery/evaluation/adjacency_list_T_GRANU.DAY_S_GRANU.BLOCK.json')
    # for k, v in graph.items():
    #     graph[k] = list(set(v))
    t_granu, s_granu = TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT
    # dump_json(f'evaluation/data_polygamy_indices/chicago_1m/spatial_graph_{t_granu}_{s_granu}.json', graph)
    # shapefile_path = "resource/chicago_shapes/shape_chicago_tracts/geo_export_c59e0875-addc-4363-a542-497356e993cf.shp"
    shapefile_path = "resource/chicago_tracts/tl_2010_17031_tract10.shp"
    graph = build_spatial_graph(shapefile_path)
    for k, v in graph.items():
        graph[k] = list(set(v))
    dump_json(f'evaluation/data_polygamy_indices/chicago_1m/spatial_graph_{t_granu}_{s_granu}.json', graph)
    # df = gpd.read_file(shapefile_path)
    # print(df.iloc[1])