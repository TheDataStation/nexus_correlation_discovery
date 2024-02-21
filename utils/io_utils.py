import json
import pandas as pd
import dill as pickle
import yaml
import os
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU

stop_words = ["wind_direction", "heading", "dig_ticket_", "uniquekey", "streetnumberto", "streetnumberfrom", "census_block", 
            "stnoto", "stnofrom", "lon", "lat", "northing", "easting", "property_group", "insepctnumber", 'primarykey','beat_',
            "north", "south", "west", "east", "beat_of_occurrence", "lastinspectionnumber", "fax", "latest_dist_res", "majority_dist", "latest_dist",
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
    return df


def persist_to_csv(path, df):
    df.to_csv(path)


def load_config(source):
    config_path = "/home/cc/nexus_correlation_discovery/config.yaml"
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
    # all_corr = all_corr[~(((all_corr['agg_attr1'] == 'count_t1') & (~all_corr['tbl_id1'].isin(to_include))) | (((all_corr['agg_attr2'] == 'count_t2') & (~all_corr['tbl_id2'].isin(to_include)))))]
    all_corr = remove_bad_cols(stop_words, all_corr)
    if remove_perfect_corrs:
        all_corr = all_corr[~(abs(all_corr['r_val'])==1)]
    all_corr['agg_attr1'] = all_corr['agg_attr1'].str[:-3]
    all_corr['agg_attr2'] = all_corr['agg_attr2'].str[:-3]
    for _, row in all_corr.iterrows():
        if index == 'id':
            corr_map[tuple(sorted(["{}--{}".format(row['tbl_id1'], row['agg_attr1']), "{}--{}".format(row['tbl_id2'], row['agg_attr2'])]))] = row['r_val']
        elif index == 'name':
            corr_map[tuple(sorted(["{}--{}".format(row['tbl_name1'], row['agg_attr1']), "{}--{}".format(row['tbl_name2'], row['agg_attr2'])]))] = row['r_val']
    return all_corr, corr_map
