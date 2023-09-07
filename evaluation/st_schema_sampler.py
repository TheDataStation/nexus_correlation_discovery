import utils.io_utils as io_utils
import numpy as np
import pandas as pd
from data_search.data_model import Unit, T_GRANU, S_GRANU, ST_Schema
from tqdm import tqdm
import time
import pandas as pd
from data_search.search_db import DBSearch
import os
from utils import corr_utils
from collections import defaultdict
from dataclasses import dataclass
from typing import List
import data_search.db_ops as db_ops
import random

# data_source = "chicago_1m"
# t_scale, s_scale = T_GRANU.DAY, S_GRANU.BLOCK
data_source = "cdc_1m"
t_scale, s_scale = T_GRANU.DAY, S_GRANU.STATE
config = io_utils.load_config(data_source)
attr_path = config["attr_path"]
tbl_attrs = io_utils.load_json(attr_path)
all_tbls = set(tbl_attrs.keys())
# load all spatio-temporal schemas
st_schema_list = []
for tbl in all_tbls:
    t_attrs, s_attrs = (
        tbl_attrs[tbl]["t_attrs"],
        tbl_attrs[tbl]["s_attrs"],
    )

    for t in t_attrs:
        st_schema_list.append((tbl, ST_Schema(t_unit=Unit(t, t_scale))))

    for s in s_attrs:
        st_schema_list.append((tbl, ST_Schema(s_unit=Unit(s, s_scale))))

    for t in t_attrs:
        for s in s_attrs:
            st_schema_list.append((tbl, ST_Schema(Unit(t, t_scale), Unit(s, s_scale))))

sample_num = 0
# print(len(st_schema_list))
if sample_num == 0:
    sampled_elements = st_schema_list
    io_utils.persist_to_pickle(
        f"evaluation/input/{data_source}/full_st_schemas.json", sampled_elements
    )
else:
    sampled_elements = random.sample(st_schema_list, sample_num)
# print(sampled_elements)
    io_utils.persist_to_pickle(
        f"evaluation/input/{data_source}/{sample_num}_st_schemas.json", sampled_elements
    )
