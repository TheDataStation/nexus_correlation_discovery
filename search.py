from coordinate import S_GRANU
from mydatetime import T_GRANU
import os
from config import INDEX_PATH
import io_utils
import time

def search(tbl_id: str, t_granu: T_GRANU, s_granu: S_GRANU):
    primary_path = os.path.join(INDEX_PATH, "index_{}.pkl".format(tbl_id)) 
    pri_idx = io_utils.load_pickle(primary_path)
    if t_granu and s_granu:
        resolution = (t_granu, s_granu)
    elif t_granu:
        resolution = t_granu
    else:
        resolution = s_granu
    print(resolution)
    pri_keys = pri_idx[resolution].keys()
    result = []
    
    for filename in os.listdir(INDEX_PATH):
        if tbl_id in filename:
            continue
        f = os.path.join(INDEX_PATH, filename)
        idx = io_utils.load_pickle(f)
        if resolution in idx:
            count = len(pri_keys & idx[resolution].keys())
            result.append((filename[6:-4], count))
            
    result = sorted(result, key = lambda x: x[1], reverse=True)
    for x in result[:5]:
        print(x)

start = time.time()
search('ijzp-q8t2', T_GRANU.DAY, S_GRANU.BLOCK)
print(time.time() - start)