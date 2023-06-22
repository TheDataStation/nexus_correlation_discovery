from utils import io_utils

data_source_metadata = (
    "/Users/yuegong/Documents/spatio_temporal_alignment/data/tbl_attrs_chicago_1m.json"
)

total_st_schemas = 0
total_cols = 0
# count the number of st_schemas
tbl_infos = io_utils.load_json(data_source_metadata)
for tbl, info in tbl_infos.items():
    st_schemas = 0
    t, s = len(info["t_attrs"]), len(info["s_attrs"])
    if len(info["t_attrs"]):
        st_schemas += 1
    if len(info["s_attrs"]):
        st_schemas += 1
    if len(info["t_attrs"]) and len(info["s_attrs"]):
        st_schemas += 1
    # st_schemas = t + s + t * s
    total_st_schemas += st_schemas
    total_cols += st_schemas * (len(info["num_columns"]) + 1)


print(total_st_schemas)
print(total_cols)
