import json
import pandas as pd
import dill as pickle
import yaml
import os
import numpy as np
from nexus.utils.spatial_hierarchy import SPATIAL_GRANU, SpatialHierarchy
from typing import Dict, List

stop_words = ["wind_direction", "heading", "dig_ticket_", "uniquekey", "streetnumberto", "streetnumberfrom",
              "census_block",
              "stnoto", "stnofrom", "lon", "lat", "northing", "easting", "property_group", "insepctnumber",
              'primarykey', 'beat_',
              "north", "south", "west", "east", "beat_of_occurrence", "lastinspectionnumber", "fax", "latest_dist_res",
              "majority_dist", "latest_dist",
              "f12", "f13", "bin"]


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
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df


def persist_to_csv(path, df):
    df.to_csv(path)


def load_config(source):
    config_path = os.environ.get("CONFIG_FILE_PATH", "config.yaml")
    with open(config_path, "r") as f:
        yaml_data = yaml.load(f, Loader=yaml.FullLoader)
        config = yaml_data[source]
        if "spatial_hierarchies" in config:
            raw_spatial_hierarchies = config["spatial_hierarchies"]
            spatial_hierarchies = []
            for spatial_hierarchy in raw_spatial_hierarchies:
                spatial_hierarchies.append(SpatialHierarchy.from_yaml(spatial_hierarchy))
            config["spatial_hierarchies"] = spatial_hierarchies
        return config

def create_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def load_corrs_to_df(data, metadata: Dict[str, str]=None, drop_count: bool=True) -> pd.DataFrame:
    df = pd.DataFrame(
        [corr.to_list(metadata) for corr in data],
        columns=[
            "domain1",
            "table_id1",
            "table_name1",
            "agg_table1",
            "agg_attr1",
            "description1",
            "agg_attr1_missing_ratio",
            "agg_attr1_zero_ratio",
            "original_attr1_missing_ratio",
            "original_attr1_zero_ratio",
            "cv1",
            "domain2",
            "table_id2",
            "table_name2",
            "agg_table2",
            "agg_attr2",
            "description2",
            "agg_attr2_missing_ratio",
            "agg_attr2_zero_ratio",
            "original_attr2_missing_ratio",
            "original_attr2_zero_ratio",
            "cv2",
            "correlation coefficient",
            "correlation coefficient after imputing avg",
            "correlation coefficient after imputing zero",
            "p value",
            "number of samples",
            "spatio-temporal key type",
        ],
    )
   
    df['agg_attr1'] = df['agg_attr1'].str[:-3]
    df['agg_attr2'] = df['agg_attr2'].str[:-3]
    if drop_count:
        df = df[(df['agg_attr1'] != 'count') & (df['agg_attr2'] != 'count')]
    return df.reset_index(drop=True)


def remove_bad_cols(stop_words, corrs):
    for stop_word in stop_words:
        corrs = corrs[~((corrs['agg_attr1'] == f'avg_{stop_word}_t1') | (corrs['agg_attr2'] == f'avg_{stop_word}_t2'))]
    return corrs


# def load_corrs_from_dir(path):
#     all_corr = None
#     for filename in os.listdir(path):
#         if filename.endswith(".csv"):
#             df = pd.read_csv(path + filename)
#             df = remove_bad_cols(stop_words, df)
#             if all_corr is None:
#                 all_corr = df
#             else:
#                 all_corr = pd.concat([all_corr, df])
#     return all_corr

def load_corrs_from_dir(path, index='name', remove_perfect_corrs=False):
    all_corr = None
    to_include = ['ijzp-q8t2', '85ca-t3if', 'x2n5-8w5q']
    corr_map = {}
    for filename in os.listdir(path):
        if filename.endswith(".csv"):
            df = pd.read_csv(path + filename)
            if all_corr is None:
                all_corr = df
            else:
                all_corr = pd.concat([all_corr, df])
    all_corr = all_corr[~(((all_corr['agg_attr1'] == 'count_t1') & (~all_corr['tbl_id1'].isin(to_include))) | (((all_corr['agg_attr2'] == 'count_t2') & (~all_corr['tbl_id2'].isin(to_include)))))]
    all_corr = remove_bad_cols(stop_words, all_corr)
    if remove_perfect_corrs:
        all_corr = all_corr[~(abs(all_corr['r_val']) == 1)]
    all_corr['agg_attr1'] = all_corr['agg_attr1'].str[:-3]
    all_corr['agg_attr2'] = all_corr['agg_attr2'].str[:-3]
    for _, row in all_corr.iterrows():
        if index == 'id':
            corr_map[tuple(sorted(["{}--{}".format(row['tbl_id1'], row['agg_attr1']),
                                   "{}--{}".format(row['tbl_id2'], row['agg_attr2'])]))] = row['r_val']
        elif index == 'name':
            corr_map[tuple(sorted(["{}--{}".format(row['tbl_name1'], row['agg_attr1']),
                                   "{}--{}".format(row['tbl_name2'], row['agg_attr2'])]))] = row['r_val']
    return all_corr, corr_map
