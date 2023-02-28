import utils.io_utils
from config import DATA_PATH, ATTR_PATH
import pandas as pd
import numpy as np


def load_meta_data():
    # info about t_attrs and s_attrs in a table
    tbl_attrs = utils.io_utils.load_json(ATTR_PATH)
    return tbl_attrs


def is_num_column_valid(col_name):
    stop_words = [
        "id",
        "longitude",
        "latitude",
        "ward",
        "date",
        "zipcode",
        "district",
        "coordinate",
    ]
    for stop_word in stop_words:
        if stop_word in col_name:
            return False
    return True


def get_numerical_columns(tbl_id, t_attrs):
    df = pd.read_csv(DATA_PATH + tbl_id + ".csv", engine="python", on_bad_lines="skip")
    numerical_columns = list(df.select_dtypes(include=[np.number]).columns.values)
    valid_num_columns = []
    # exclude columns that contain stop words and timestamp columns
    for col in numerical_columns:
        if is_num_column_valid(col) and col not in t_attrs:
            valid_num_columns.append(col)
    return valid_num_columns


if __name__ == "__main__":
    tbl_attrs = load_meta_data()
    for tbl_id in tbl_attrs.keys():
        num_columns = get_numerical_columns(tbl_id, tbl_attrs[tbl_id])
        tbl_attrs[tbl_id]["num_columns"] = num_columns

    utils.io_utils.dump_json(
        ATTR_PATH,
        tbl_attrs,
    )
