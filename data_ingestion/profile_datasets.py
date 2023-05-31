from utils import io_utils
from config import ATTR_PATH, DATA_PATH
import pandas as pd
from tqdm import tqdm

metadata = io_utils.load_json(ATTR_PATH)
profiles = {}

for tbl_id, info in tqdm(metadata.items()):
    profile = {}
    f_path = "{}/{}.csv".format(DATA_PATH, tbl_id)
    df = pd.read_csv(f_path)
    for num_col in info["num_columns"]:
        missing_ratio = round(df[num_col].isnull().sum() / len(df[num_col]), 2)
        zero_ratio = round((df[num_col] == 0).sum() / len(df[num_col]), 2)
        num_col_name1 = "avg_{}_t1".format(num_col)[:63]
        num_col_name2 = "avg_{}_t2".format(num_col)[:63]
        profile[num_col_name1] = {
            "missing_ratio": missing_ratio,
            "zero_ratio": zero_ratio,
        }
        profile[num_col_name2] = {
            "missing_ratio": missing_ratio,
            "zero_ratio": zero_ratio,
        }

    profiles[tbl_id] = profile

io_utils.dump_json(
    "/Users/yuegong/Documents/spatio_temporal_alignment/data/profile_chicago_10k.json",
    profiles,
)
