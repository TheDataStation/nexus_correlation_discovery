import pandas as pd
import io_utils
from config import DATA_PATH, INDEX_PATH
from coordinate import S_GRANU, parse_coordinate
from mydatetime import T_GRANU, Datetime
from collections import defaultdict
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

def read_data(tbl_id, t_attr, s_attr):
    if t_attr:
        df = io_utils.read_columns(DATA_PATH + tbl_id, [t_attr])
        df[t_attr] = pd.to_datetime(df[t_attr], infer_datetime_format=True, utc=True, errors='coerce')
        time_list = df[t_attr].tolist()
        time_list = [Datetime(x) for x in time_list]
    
    if s_attr:
        df = io_utils.read_columns(DATA_PATH + tbl_id, [s_attr])
        tmp_list = df[s_attr].tolist() 
        with ThreadPoolExecutor() as tpe:
            spatial_list = list(tqdm(tpe.map(parse_coordinate, iter(tmp_list)), total=len(df)))

        print(spatial_list[0:10])
   
    if t_attr and s_attr:
        spatio_temporal_list = list(zip(spatial_list, time_list))
        return spatio_temporal_list
    elif t_attr:
        return time_list
    else:
        return spatial_list

def aggregate(data, s_granu: S_GRANU, t_granu: T_GRANU):
    hash_index = defaultdict(list)
    for i, x in enumerate(data):
        if t_granu and s_granu:
            x = (tuple(x[0].transform(s_granu)), tuple(x[1].transform(t_granu)))
        elif t_granu:
            x = tuple(x.transform(t_granu))
        else:
            x = tuple(x.transform(s_granu))
        
        hash_index[x].append(i)   
    return hash_index

def build_index_for_tbl(tbl_id, t_attr, s_attr):
    print("loading data for " + tbl_id)
    data = read_data(tbl_id, t_attr, s_attr)
    print("build index for " + tbl_id)
    full_index = {}
    if t_attr and s_attr:
        for s_granu in S_GRANU:
            for t_granu in T_GRANU:
                index = aggregate(data, s_granu, t_granu)
                full_index[(s_granu, t_granu)] = index
    elif t_attr:
        for t_granu in T_GRANU:
            index = aggregate(data, None, t_granu)
            full_index[t_granu] = index
    else:
        for s_granu in S_GRANU:
            index = aggregate(data, s_granu, None)
            full_index[s_granu] = index
    io_utils.persist_to_pickle(INDEX_PATH + 'index_{}.pkl'.format(tbl_id[:-4]), full_index)



    