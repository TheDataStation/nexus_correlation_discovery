import json
import pandas as pd
import dill as pickle
import yaml
import os
from utils.coordinate import S_GRANU

from utils.time_point import T_GRANU


def dump_json(path: str, obj):
    dir = os.path.dirname(path)
    if dir and not os.path.exists(dir):
        os.makedirs(dir)
    with open(path, "w") as f:
        json.dump(obj, f, indent=4)


def load_json(path: str):
    with open(path, "r") as f:
        return json.load(f)


def load_pickle(path: str):
    idx_file = open(path, "rb")
    object = pickle.load(idx_file)
    return object


def persist_to_pickle(path, object):
    pickle.dump(object, open(path, "wb"))


def read_columns(path, fields):
    df = pd.read_csv(path, usecols=fields)
    return df


def read_csv(path):
    df = pd.read_csv(path, engine="c", on_bad_lines="skip", low_memory=False)
    return df


def persist_to_csv(path, df):
    df.to_csv(path)


def load_config(source):
    config_path = "/home/cc/resolution_aware_spatial_temporal_alignment/config.yaml"
    with open(config_path, "r") as f:
        yaml_data = yaml.load(f, Loader=yaml.FullLoader)
        config = yaml_data[source]
        return config

def create_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)    

def load_corrs_to_df(data):
    df = pd.DataFrame(
        [corr.to_list() for corr in data],
        columns=[
            "domain1",
            "tbl_id1",
            "tbl_name1",
            "agg_tbl1",
            "agg_attr1",
            "missing_ratio1",
            "zero_ratio1",
            "missing_ratio_o1",
            "zero_ratio_o1",
            "cv1",
            "domain2",
            "tbl_id2",
            "tbl_name2",
            "agg_tbl2",
            "agg_attr2",
            "missing_ratio2",
            "zero_ratio2",
            "missing_ratio_o2",
            "zero_ratio_o2",
            "cv2",
            "r_val",
            "r_impute_avg_val",
            "r_impute_zero_val",
            "p_val",
            "samples",
            "align_type",
        ],
    )
    return df