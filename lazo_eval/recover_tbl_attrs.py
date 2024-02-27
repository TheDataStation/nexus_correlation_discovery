from data_ingestion.index_builder_agg import DBIngestorAgg
from utils import io_utils
import os
from utils.data_model import Table
import numpy as np
import pandas as pd
## recover tbl_attrs for chicago 1m

def construct_tbl_attrs(data_source):
    config = io_utils.load_config(data_source)
    meta_path = config["meta_path"]
    meta_data = io_utils.load_json(meta_path)
    max_limit = 2
    tbls = {}
    for obj in meta_data:
        t_attrs, s_attrs = DBIngestorAgg.select_valid_attrs(
            obj["t_attrs"], max_limit
        ), DBIngestorAgg.select_valid_attrs(obj["s_attrs"], max_limit)
        if len(t_attrs) == 0 and len(s_attrs) == 0:
            continue
        first = False
        if first:
            # when first is specified, only take the first t_attr and s_attr
            if t_attrs:
                t_attrs = [t_attrs[0]]
            if s_attrs:
                s_attrs = [s_attrs[0]]
        tbl = Table(
            domain=obj["domain"],
            tbl_id=obj["tbl_id"],
            tbl_name=obj["tbl_name"],
            t_attrs=t_attrs,
            s_attrs=s_attrs,
            num_columns=obj["num_columns"],
        )
        tbl_path = os.path.join(config["data_path"], f"{tbl.tbl_id}.csv")
        df = pd.read_csv(tbl_path, nrows=1000)
        all_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        numerical_columns = DBIngestorAgg.get_numerical_columns(all_columns, tbl)
        tbls[tbl.tbl_id] = {
            "name": tbl.tbl_name,
            "t_attrs": tbl.t_attrs,
            "s_attrs": tbl.s_attrs,
            "num_columns": numerical_columns,
        }
    io_utils.dump_json('resource/chicago_1m/_tbl_attrs_chicago_1m.json', tbls)



if __name__ == "__main__":
    construct_tbl_attrs('chicago_1m')