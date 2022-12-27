from coordinate import S_GRANU
from mydatetime import T_GRANU
import os
from config import INDEX_PATH
import io_utils
import time

def search(tbl_id: str, t_attr: str=None, s_attr: str=None, t_granu: T_GRANU=None, s_granu: S_GRANU=None):
    if t_attr and s_attr:
        primary_path = os.path.join(INDEX_PATH, "index_{} {} {}.pkl".format(tbl_id, t_attr, s_attr)) 
    elif t_attr:
        primary_path = os.path.join(INDEX_PATH, "index_{} {}.pkl".format(tbl_id, t_attr)) 
    else:
        primary_path = os.path.join(INDEX_PATH, "index_{} {}.pkl".format(tbl_id, s_attr)) 
    pri_idx = io_utils.load_pickle(primary_path)
    if t_granu and s_granu:
        resolution = (t_granu, s_granu)
    elif t_granu:
        resolution = t_granu
    else:
        resolution = s_granu
    pri_keys = pri_idx[resolution].keys()
    result = []
    
    for filename in os.listdir(INDEX_PATH):
        if tbl_id in filename:
            continue
        f = os.path.join(INDEX_PATH, filename)
        idx = io_utils.load_pickle(f)
        if resolution in idx:
            count = len(pri_keys & idx[resolution].keys())
            if count != 0:
                tokens = filename[6:-4].split()
                result.append((tokens + [count]))
            
    result = sorted(result, key = lambda x: x[-1], reverse=True)
    return result

# start = time.time()
# res = search('ijzp-q8t2', 'date', 'location', T_GRANU.DAY, S_GRANU.BLOCK)
# print(res)
# print(time.time() - start)