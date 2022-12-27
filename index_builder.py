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

def aggregate_in_one(data, t_attr, s_attr):
    full_index = {}
    t_index = {}
    s_index = {}
    if t_attr and s_attr:
        for idx, row in data.iterrows():
            for t_granu in mydatetime.T_GRANU:
                for s_granu in coordinate.S_GRANU: 
                    if (t_granu, s_granu) not in full_index:
                        hash_index_all = defaultdict(list)
                        full_index[(t_granu, s_granu)] = hash_index_all
                    else:
                        hash_index_all = full_index[(t_granu, s_granu)]
                    key = (tuple(row[t_attr].transform(t_granu)), tuple(row[s_attr].transform(s_granu)))
                    hash_index_all[key].append(idx)
                    if t_granu not in t_index:
                        hash_index_t = defaultdict(list)
                        t_index[t_granu] = hash_index_t
                    else:
                        hash_index_t = t_index[t_granu]
                    key = tuple(row[t_attr].transform(t_granu))
                    hash_index_t[key].append(idx)
                    if s_granu not in s_index:
                        hash_index_s = defaultdict(list)
                        s_index[s_granu] = hash_index_s
                    else:
                        hash_index_s = s_index[s_granu]
                    key = tuple(row[s_attr].transform(s_granu))
                    hash_index_s[key].append(idx)
        return (full_index, t_index, s_index)
    elif t_attr:
        for idx, row in data.iterrows():
            for t_granu in mydatetime.T_GRANU:
                if t_granu not in full_index:
                    hash_index_t = defaultdict(list)
                    full_index[t_granu] = hash_index_t
                else:
                    hash_index_t = full_index[t_granu]
                key = tuple(row[t_attr].transform(t_granu))
                hash_index_t[key].append(idx)
            
        return (full_index)
    elif s_attr:
        for idx, row in data.iterrows():
            for s_granu in coordinate.S_GRANU:
                if s_granu not in full_index:
                    hash_index_s = defaultdict(list)
                    full_index[s_granu] = hash_index_s
                else:
                    hash_index_s = full_index[s_granu]
                key = tuple(row[s_attr].transform(s_granu))
                hash_index_s[key].append(idx)
            
        return (full_index)


def build_index_for_tbl(tbl_id, t_attr, s_attr, index_path):
    log = open('log.txt', 'w', buffering=1)
    print("loading data for {}".format(tbl_id))
    log.write("loading data for {}\n".format(tbl_id))
    start = time.time()
    data = read_data(tbl_id, t_attr, s_attr)
    print("loading data finished {} s".format(time.time() - start))
    log.write("loading data finished {} s\n".format(time.time() - start))
    if data is None:
        return
    print("build index for {}".format(tbl_id))
    log.write("build index for {}\n".format(tbl_id))
    start = time.time()
    full_index = aggregate_in_one(data, t_attr, s_attr)
    print("building index finished {} s".format(time.time() - start))
    log.write("building index finished {} s\n".format(time.time() - start))

    print("begin dumping")
    log.write("begin dumping\n")
    start = time.time()
    if t_attr and s_attr:
        io_utils.persist_to_pickle(index_path + 'index_{} {} {}.pkl'.format(tbl_id[:-4], t_attr, s_attr), full_index[0])
        io_utils.persist_to_pickle(index_path + 'index_{} {}.pkl'.format(tbl_id[:-4], t_attr), full_index[1])
        io_utils.persist_to_pickle(index_path + 'index_{} {}.pkl'.format(tbl_id[:-4], s_attr), full_index[2])
    elif t_attr:
        io_utils.persist_to_pickle(index_path + 'index_{} {}.pkl'.format(tbl_id[:-4], t_attr), full_index)
    else:
        io_utils.persist_to_pickle(index_path + 'index_{} {}.pkl'.format(tbl_id[:-4], s_attr), full_index)
    
    
    print("finished dumping {} s".format(time.time() - start))
    log.write("finished dumping {} s\n".format(time.time() - start))



    