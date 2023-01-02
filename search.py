from coordinate import S_GRANU
from mydatetime import T_GRANU
import os
from config import INDEX_PATH
import io_utils
import time
from coordinate import S_GRANU, pt_to_str
from mydatetime import T_GRANU, dt_to_str
import pandas as pd

def search(tbl_id: str, t_attr: str=None, s_attr: str=None, t_granu: T_GRANU=None, s_granu: S_GRANU=None):
    metadata = io_utils.load_json(INDEX_PATH + 'metadata.json')
    idx_list = []
    if t_attr and s_attr:
        primary_path = os.path.join(INDEX_PATH, "index_{} {} {}.json".format(tbl_id, t_attr, s_attr))
        idx_list = metadata['spatial_temporal'] 
    elif t_attr:
        primary_path = os.path.join(INDEX_PATH, "index_{} {}.json".format(tbl_id, t_attr)) 
        idx_list = metadata['temporal'] 
    else:
        primary_path = os.path.join(INDEX_PATH, "index_{} {}.json".format(tbl_id, s_attr)) 
        idx_list = metadata['spatial'] 

    pri_idx = io_utils.load_json(primary_path)
    if t_granu and s_granu:
        resolution = str([t_granu, s_granu])
    elif t_granu:
        resolution = str(int(t_granu))
    else:
        resolution = str(int(s_granu))

    pri_keys = pri_idx[resolution].keys()
    result = []
    
    for filename in idx_list:
        if tbl_id in filename:
            continue
        f = os.path.join(INDEX_PATH, filename)
        idx = io_utils.load_json(f)
        if resolution in idx:
            count = len(pri_keys & idx[resolution].keys())
            if count != 0:
                tokens = filename[6:-5].split()
                result.append((tokens + [count]))
            
    result = sorted(result, key = lambda x: x[-1], reverse=True)
    return result

def agg_join(self, tbl1: str, t_attr1: str, s_attr1: str, tbl2: str, t_attr2: str, s_attr2: str, t_granu: T_GRANU, s_granu: S_GRANU):
        if t_granu and s_granu:
            resolution = (t_granu, s_granu)
        elif t_granu:
            resolution = t_granu
        else:
            resolution = s_granu

        tbl1_idx_path = self.get_idx_path(tbl1, t_attr1, s_attr1)
        tbl1_idx = io_utils.load_pickle(tbl1_idx_path)[resolution]
        tbl2_idx_path = self.get_idx_path(tbl2, t_attr2, s_attr2)
        tbl2_idx = io_utils.load_pickle(tbl2_idx_path)[resolution]
       
        overlap_units = tbl1_idx.keys() & tbl2_idx.keys()
        rows = []
        for unit in overlap_units:
            if t_granu and s_granu:
                row = [unit[0], unit[1], len(tbl1_idx[unit]), len(tbl2_idx[unit])]
            else:
                row = [unit, len(tbl1_idx[unit]), len(tbl2_idx[unit])]
            rows.append(row)
        if t_granu and s_granu:
            df =  pd.DataFrame(rows, columns=['Time', 'Location', 'Count tbl1', 'Count tbl2'])
            df['Time'] = df['Time'].apply(dt_to_str)
            df['Location'] = df['Location'].apply(pt_to_str)
            return df
        elif t_granu:
            df = pd.DataFrame(rows, columns=['Time', 'Count tbl1', 'Count tbl2'])
            df['Time'] = df['Time'].apply(dt_to_str)
            return df
        else:
            df = pd.DataFrame(rows, columns=['Location', 'Count tbl1', 'Count tbl2'])
            df['Location'] = df['Location'].apply(pt_to_str)
            return df

def calculate_corr(tbl1, t_attr1, s_attr1, tbl2, t_attr2, s_attr2, t_granu, s_granu):
    merged = agg_join(tbl1, t_attr1, s_attr1, tbl2, t_attr2, s_attr2, t_granu, s_granu)
    corr_matrix = merged.corr(method ='pearson', numeric_only=True)
    return corr_matrix.iloc['Count tbl1', 'Count tbl2']

# start = time.time()
# res = search('ijzp-q8t2', 'date', 'location', T_GRANU.DAY, S_GRANU.BLOCK)
# print(res)
# print(time.time() - start)