import pandas as pd
import io_utils
from config import DATA_PATH, INDEX_PATH
import coordinate
import mydatetime 
from collections import defaultdict
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import geopandas as gpd

shape_file_path = "shape_chicago_blocks/geo_export_8e927c91-3aad-4b67-86ff-bf4de675094e.shp"

def read_data(tbl_id, t_attr, s_attr):
    if t_attr and s_attr:
        df = io_utils.read_columns(DATA_PATH + tbl_id, [t_attr, s_attr])
        df[t_attr] = pd.to_datetime(df[t_attr], infer_datetime_format=True, utc=True, errors='coerce').apply(mydatetime.parse_datetime)
        df[s_attr] = df[s_attr].apply(coordinate.parse_coordinate)
        gdf = gpd.GeoDataFrame(df[t_attr], geometry=df[s_attr]).dropna(how='all')
        gdf.set_crs(epsg=4326, inplace=True)
        if len(gdf):
            df = coordinate.resolve_resolution_hierarchy(gdf, s_attr, shape_file_path)
            if df is None:
                return None
            return df[[t_attr, s_attr]]
        else:
            return None
        
    elif t_attr:
        df = io_utils.read_columns(DATA_PATH + tbl_id, [t_attr])
        df[t_attr] = pd.to_datetime(df[t_attr], infer_datetime_format=True, utc=True, errors='coerce').apply(mydatetime.parse_datetime).dropna()
        if len(df[t_attr]):
            return df[[t_attr]]
        else:
            return None
    else:
        df = io_utils.read_columns(DATA_PATH + tbl_id, [s_attr])
        df[s_attr] = df[s_attr].apply(coordinate.parse_coordinate)
        gdf = gpd.GeoDataFrame(geometry=df[s_attr]).dropna()
        gdf.set_crs(epsg=4326, inplace=True)
        if len(gdf):
            df = coordinate.resolve_resolution_hierarchy(gdf, s_attr, shape_file_path)
            if df is None:
                return None
            return df[[s_attr]]
        else:
            return None

def aggregate(data, t_attr: str, t_granu: mydatetime.T_GRANU, s_attr: str, s_granu: coordinate.S_GRANU):
    hash_index = defaultdict(list)
    if t_attr and s_attr:
        for idx, row in data.iterrows():
            key = (tuple(row[t_attr].transform(t_granu)), tuple(row[s_attr].transform(s_granu)))
            hash_index[key].append(idx)
    elif t_attr:
        for idx, row in data.iterrows():
            key = tuple(row[t_attr].transform(t_granu))
            hash_index[key].append(idx)
    elif s_attr:
        for idx, row in data.iterrows():
            key = tuple(row[s_attr].transform(s_granu))
            hash_index[key].append(idx)
    return hash_index

def build_index_for_tbl(tbl_id, t_attr, s_attr, index_path):
    print("loading data for " + tbl_id)
    data = read_data(tbl_id, t_attr, s_attr)
    if data is None:
        return
    print("build index for " + tbl_id)
    full_index = {}
    if t_attr and s_attr:
        for t_granu in mydatetime.T_GRANU:
            for s_granu in coordinate.S_GRANU:
                index = aggregate(data, t_attr, t_granu, s_attr, s_granu)
                full_index[(t_granu, s_granu)] = index
                if t_granu not in full_index:
                    index = aggregate(data, t_attr, t_granu, None, None)
                    full_index[t_granu] = index
                if s_granu not in full_index:
                    index = aggregate(data, None, None, s_attr, s_granu)
                    full_index[s_granu] = index                
    elif t_attr:
        for t_granu in mydatetime.T_GRANU:
            index = aggregate(data, t_attr, t_granu, None, None)
            full_index[t_granu] = index
    else:
        for s_granu in coordinate.S_GRANU:
            index = aggregate(data, None, None, s_attr, s_granu)
            full_index[s_granu] = index
    io_utils.persist_to_pickle(index_path + 'index_{}.pkl'.format(tbl_id[:-4]), full_index)



    