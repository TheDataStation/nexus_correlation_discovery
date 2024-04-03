import os
import pandas as pd
from nexus.utils.io_utils import load_json, load_config, dump_json
import numpy as np
from nexus.utils.profile_utils import is_num_column_valid
from nexus.utils.data_model import Table, Attr
from nexus.utils.coordinate import SPATIAL_GRANU

spatial_granu_map = {
    "geoCoordinate": "POINT",
    "tract": "TRACT",
    "zipCode": "ZIPCODE"
}

def label_using_regex(data: pd.DataFrame):
        spatial_patterns = load_json('nexus/data_prep/spatial_patterns.json')
        temporal_patterns = load_json('nexus/data_prep/temporal_patterns.json')  
        temporal_attrs, spatial_attrs, num_attrs = [], [], []
        # get numerical columns in a dataframe using dataframe types
        num_columns = list(data.select_dtypes(include=[np.number]).columns.values)
        for num_column in num_columns:
            if num_column == "Unnamed: 0":
                continue
            if is_num_column_valid(num_column):
                num_attrs.append(num_column)
        for col in data.columns:
            # if col in num_attrs or data[col].dtype == 'int64' or data[col].dtype == 'float64':
            #     continue
            if col == "Unnamed: 0":
                continue
            data[col] = data[col].astype(str).str.replace('\n', '').str.strip().dropna()
            temporal_flag, spatial_flag = False, False
            for granu, patterns in temporal_patterns.items():
                if temporal_flag:
                    break
                for pattern in patterns:
                    if data[col].str.match(pattern).sum() > len(data[col])*0.5:
                        temporal_attrs.append(Attr(col, granu))
                        if col in num_attrs:
                            num_attrs.remove(col)
                        temporal_flag = True
                        break
            for granu, patterns in spatial_patterns.items():    
                if temporal_flag or spatial_flag:
                    break
                for pattern in patterns:
                    if data[col].str.match(pattern).sum() > len(data[col])*0.5:
                        spatial_attrs.append(Attr(col, spatial_granu_map[granu]))
                        if col in num_attrs:
                            num_attrs.remove(col)
                        spatial_flag = True
                        break
        return temporal_attrs, spatial_attrs, num_attrs

def label_data_source(data_source_name, num_sample: int):
    source_config = load_config(data_source_name)
    data_path = source_config['data_path']
    metadata_path = source_config['meta_path']
    table_info = {}
    for filename in os.listdir(data_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(data_path, filename)
            df = pd.read_csv(file_path, nrows=num_sample)
            temporal_attrs, spatial_attrs, num_attrs = label_using_regex(df)
            tbl_id = filename[:-4]
            table = Table(tbl_id=tbl_id, tbl_name=tbl_id, 
                  temporal_attrs=temporal_attrs, spatial_attrs=spatial_attrs, 
                  num_columns=num_attrs, link=file_path)
            table_info[tbl_id] = table.to_json()
    dump_json(metadata_path, table_info)

if __name__ == "__main__":
    label_data_source('chicago_test', 1000)