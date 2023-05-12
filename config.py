DATA_PATH = (
    # "/Users/yuegong/Documents/spatio_temporal_alignment/data/chicago_open_data_10k/"
    "/Users/yuegong/Documents/spatio_temporal_alignment/data/cdc_open_data_10k/"
)

META_PATH = (
    # "/Users/yuegong/Documents/spatio_temporal_alignment/data/st_table_chicago_open_data.json"
    "/Users/yuegong/Documents/spatio_temporal_alignment/data/cdc_open_data.json"
)

# SHAPE_PATH = "/Users/yuegong/Documents/spatio_temporal_alignment/data/shape_chicago_blocks/geo_export_8e927c91-3aad-4b67-86ff-bf4de675094e.shp"
SHAPE_PATH = "/Users/yuegong/Documents/spatio_temporal_alignment/data/shape_usa_counties/cb_2018_us_county_20m.shp"

# ATTR_PATH = "/Users/yuegong/Documents/spatio_temporal_alignment/data/tbl_attrs_10k.json"
ATTR_PATH = (
    "/Users/yuegong/Documents/spatio_temporal_alignment/data/tbl_attrs_cdc_10k.json"
)

PROFILE_PATH = (
    # "/Users/yuegong/Documents/spatio_temporal_alignment/data/profile_chicago_10k.json"
    "/Users/yuegong/Documents/spatio_temporal_alignment/data/profile_cdc_10k.json"
)


"""
data prep settings
"""
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

APP_TOKEN = "voGyQiv0JulAwW8e2YwTCUlBS"
