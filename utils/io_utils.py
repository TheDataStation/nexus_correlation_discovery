import json
import pandas as pd
import dill as pickle
import yaml
import os


def dump_json(path: str, obj):
    dir = os.path.dirname(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    with open(path, "w") as f:
        json.dump(obj, f)


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


def read_csv(path, t_attrs=None, s_attrs=None):
    df = pd.read_csv(path, engine="python", on_bad_lines="skip")
    if t_attrs:
        for t_attr in t_attrs:
            df[t_attr] = pd.to_datetime(
                df[t_attr], infer_datetime_format=True, utc=True, errors="coerce"
            )
    return df


def persist_to_csv(path, df):
    df.to_csv(path)


def load_config(source):
    config_path = "/home/cc/resolution_aware_spatial_temporal_alignment/config.yaml"
    with open(config_path, "r") as f:
        yaml_data = yaml.load(f, Loader=yaml.FullLoader)
        config = yaml_data[source]
        return config
