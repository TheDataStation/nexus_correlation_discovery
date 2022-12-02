from coordinate import S_GRANU
from mydatetime import T_GRANU
import os
from config import INDEX_PATH
import io_utils
import time

def search(tbl_id: str, t_attr: str, t_granu: T_GRANU, s_attr: str, s_granu: S_GRANU):
    primary_path = os.path.join(INDEX_PATH, "index_{}.pkl".format(tbl_id)) 
    pri_idx = io_utils.load_pickle(primary_path)
    pri_keys = pri_idx[t_granu].keys()
    result = []
    if t_attr:
        for filename in os.listdir(INDEX_PATH):
            if tbl_id in filename:
                continue
            f = os.path.join(INDEX_PATH, filename)
            idx = io_utils.load_pickle(f)
            # print(idx[t_granu].keys())
            count = len(pri_keys & idx[t_granu].keys())
            result.append((filename[6:-4], count))
            
    result = sorted(result, key = lambda x: x[1], reverse=True)
    for x in result:
        print(x)

start = time.time()
search('6iiy-9s97', 'service_date', T_GRANU.DAY, None, None)
print(time.time() - start)