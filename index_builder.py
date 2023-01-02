import pandas as pd
import io_utils
from config import DATA_PATH, INDEX_PATH, SHAPE_PATH, META_PATH
import coordinate
import mydatetime 
from collections import defaultdict
import time
from tqdm import tqdm
import geopandas as gpd

def read_data(tbl_id, t_attr, s_attr):
    if t_attr and s_attr:
        df = io_utils.read_columns(DATA_PATH + tbl_id, [t_attr, s_attr])
        df[t_attr] = pd.to_datetime(df[t_attr], infer_datetime_format=True, utc=True, errors='coerce').apply(mydatetime.parse_datetime)
        df[s_attr] = df[s_attr].apply(coordinate.parse_coordinate)
        gdf = gpd.GeoDataFrame(df[t_attr], geometry=df[s_attr]).dropna(how='all')
        gdf.set_crs(epsg=4326, inplace=True)
        if len(gdf):
            df = coordinate.resolve_resolution_hierarchy(gdf, s_attr, SHAPE_PATH)
            if df is None:
                return None
            return df[[t_attr, s_attr]]
        else:
            return None
        
    elif t_attr:
        df = io_utils.read_columns(DATA_PATH + tbl_id, [t_attr])
        df[t_attr] = pd.to_datetime(df[t_attr], infer_datetime_format=True, utc=True, errors='coerce').apply(mydatetime.parse_datetime).dropna().apply(mydatetime.parse_datetime)
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
            df = coordinate.resolve_resolution_hierarchy(gdf, s_attr, SHAPE_PATH)
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
                    resolution = str([t_granu, s_granu])
                    if resolution not in full_index:
                        hash_index_all = defaultdict(list)
                        full_index[resolution] = hash_index_all
                    else:
                        hash_index_all = full_index[resolution]
                   
                    key = str([row[t_attr].transform_to_key(t_granu), row[s_attr].transform_to_key(s_granu)])
                    hash_index_all[key].append(idx)
                    
                    if t_granu not in t_index:
                        hash_index_t = defaultdict(set)
                        t_index[t_granu] = hash_index_t
                    else:
                        hash_index_t = t_index[t_granu]
                    key = row[t_attr].transform_to_key(t_granu)
                    hash_index_t[key].add(idx)
                    
                    if s_granu not in s_index:
                        hash_index_s = defaultdict(set)
                        s_index[s_granu] = hash_index_s
                    else:
                        hash_index_s = s_index[s_granu]
                    key = row[s_attr].transform_to_key(s_granu)
                    hash_index_s[key].add(idx)

        for _, v in t_index.items():
            for k, s in v.items():
                v[k] = list(s)
        
        for _, v in s_index.items():
            for k, s in v.items():
                v[k] = list(s)
                
        return (full_index, t_index, s_index)
    elif t_attr:
        for idx, row in data.iterrows():
            for t_granu in mydatetime.T_GRANU:
                if t_granu not in full_index:
                    hash_index_t = defaultdict(list)
                    full_index[t_granu] = hash_index_t
                else:
                    hash_index_t = full_index[t_granu]
                key = row[t_attr].transform_to_key(t_granu)
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
                key = row[s_attr].transform_to_key(s_granu)
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
        st_idx_name = 'index_{} {} {}.json'.format(tbl_id[:-4], t_attr, s_attr)
        io_utils.dump_json(index_path + st_idx_name, full_index[0])
        t_idx_name = 'index_{} {}.json'.format(tbl_id[:-4], t_attr)
        io_utils.dump_json(index_path + t_idx_name, full_index[1])
        s_idx_name = 'index_{} {}.json'.format(tbl_id[:-4], s_attr)
        io_utils.dump_json(index_path + s_idx_name, full_index[2])

        print("finished dumping {} s".format(time.time() - start))
        log.write("finished dumping {} s\n".format(time.time() - start)) 

        return st_idx_name, t_idx_name, s_idx_name
    elif t_attr:
        t_idx_name = 'index_{} {}.json'.format(tbl_id[:-4], t_attr)
        io_utils.dump_json(index_path + t_idx_name, full_index)

        print("finished dumping {} s".format(time.time() - start))
        log.write("finished dumping {} s\n".format(time.time() - start))

        return t_idx_name
    else:
        s_idx_name = 'index_{} {}.json'.format(tbl_id[:-4], s_attr)
        io_utils.dump_json(index_path + s_idx_name, full_index)

        print("finished dumping {} s".format(time.time() - start))
        log.write("finished dumping {} s\n".format(time.time() - start))

        return s_idx_name
    

def build_indices_for_all_tables():
    meta_data = io_utils.load_json(META_PATH)
    idx_map = defaultdict(list)
    for obj in meta_data:
        domain, tbl_id, tbl_name, t_attrs, s_attrs = obj['domain'], obj['tbl_id'], obj['tbl_name'], obj['t_attrs'], obj['s_attrs']
        if len(t_attrs) and len(s_attrs):
            for t_attr in t_attrs:
                for s_attr in s_attrs:
                    st_idx, t_idx, s_idx = build_index_for_tbl(tbl_id + '.csv', t_attr, s_attr, INDEX_PATH)
                    idx_map['spatial_temporal'].append(st_idx)
                    idx_map['temporal'].append(t_idx)
                    idx_map['spatial'].append(s_idx)
        elif len(t_attrs):
            for t_attr in t_attrs:
                t_idx = build_index_for_tbl(tbl_id + '.csv', t_attr, None, INDEX_PATH)
                idx_map['temporal'].append(t_idx)
        elif len(s_attrs):
            for s_attr in s_attrs:
                s_idx = build_index_for_tbl(tbl_id + '.csv', None, s_attr, INDEX_PATH)
                idx_map['spatial'].append(s_idx)
    
    io_utils.dump_json(INDEX_PATH + 'metadata.json', idx_map)
   


    