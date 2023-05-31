from utils import io_utils

data_source_metadata = (
    "/Users/yuegong/Documents/spatio_temporal_alignment/data/tbl_attrs_10k.json"
)

total_st_schemas = 0
total_cols = 0
# count the number of st_schemas
tbl_infos = io_utils.load_json(data_source_metadata)
for tbl, info in tbl_infos.items():
    t, s = len(info["t_attrs"]), len(info["s_attrs"])
    st_schemas = t + s + t * s
    total_st_schemas += st_schemas
    total_cols += st_schemas * (len(info["num_columns"]) + 1)


print(total_st_schemas)
print(total_cols)
