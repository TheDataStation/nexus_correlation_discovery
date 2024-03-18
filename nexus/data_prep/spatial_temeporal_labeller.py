import pandas as pd
from nexus.utils.io_utils import load_json
import re
import numpy as np
from nexus.utils.profile_utils import is_num_column_valid
from utils.data_model import Attr

class SpatialTemporalLabeller:
    def __init__(self):
        pass
    
    def label(self, data: pd.DataFrame):
        spatial_patterns = load_json('data_prep/spatial_patterns.json')
        temporal_patterns = load_json('data_prep/temporal_patterns.json')  
        temporal_attrs, spatial_attrs, num_attrs = [], [], []
        # get numerical columns in a dataframe using dataframe types
        num_columns = list(data.select_dtypes(include=[np.number]).columns.values)
        for num_column in num_columns:
            if is_num_column_valid(data[num_column]):
                num_attrs.append(num_column)
        for col in data.columns:
            if col in num_attrs:
                continue
            for granu, patterns in temporal_patterns:
                for pattern in patterns:
                    if data[col].str.match(pattern).all():
                        temporal_attrs.append(Attr(col, granu))
                        break
            for granu, patterns in spatial_patterns["geoCoordinate"]:    
                for pattern in patterns:
                    if data[col].str.match(pattern).all():
                        spatial_attrs.append(Attr(col, granu))
                        break
        return temporal_attrs, spatial_attrs, num_attrs
if __name__ == "__main__":
    spatial_pattern = load_json('data_prep/spatial_patterns.json')
    for pattern in spatial_pattern["geoCoordinate"]:
        if re.search(pattern, "POINT (-87.697142196394 41.852899220472)"):
            print("yes")
   