from nexus.utils.data_model import KeyType
from nexus.utils import io_utils
from nexus.data_ingestion.data_profiler import Profiler
from nexus.utils.spatial_hierarchy import SPATIAL_GRANU
from nexus.utils.time_point import TEMPORAL_GRANU

def profile_data_sources(data_sources, t_granu, s_granu):
    tbl_cnt_total, key_cnt_total, time_total, space_total, ts_total, var_total = 0, 0, 0, 0, 0, 0
    for data_source in data_sources:
        tbl_cnt, time, space, ts, key_cnt, num_var = profile_data_source(data_source, t_granu, s_granu)
        tbl_cnt_total += tbl_cnt
        key_cnt_total += key_cnt
        time_total += time
        space_total += space
        ts_total += ts
        var_total += num_var
    return tbl_cnt_total, key_cnt_total, time_total, space_total, ts_total, var_total
    

def profile_data_source(data_source, t_granu, s_granu):
    config = io_utils.load_config(data_source)
    tbl_attrs = io_utils.load_json(config['attr_path'])
    num_tbls = len(tbl_attrs.keys())
    num_var = 0
    for tbl, info in tbl_attrs.items():
        if len(info["num_columns"]):
            num_var += len(info["num_columns"])
        else:
            num_var += 1
    all_st_schemas = Profiler.load_all_spatio_temporal_keys(tbl_attrs, t_granu, s_granu, type_aware=True)
    # profile["key_cnt"] = len(all_st_schemas)
    time = len(all_st_schemas[KeyType.TIME])
    tbl_cnt_time = set([x[0] for x in all_st_schemas[KeyType.TIME]])
    space = len(all_st_schemas[KeyType.SPACE])
    tbl_cnt_space = set([x[0] for x in all_st_schemas[KeyType.SPACE]])
    time_space = len(all_st_schemas[KeyType.TIME_SPACE])
    tbl_cnt_st = set([x[0] for x in all_st_schemas[KeyType.TIME_SPACE]])
    return len(tbl_cnt_time.union(tbl_cnt_space).union(tbl_cnt_st)), time, space, time_space, time + space + time_space, num_var, num_tbls

if __name__ == "__main__":
    # data_sources = [
    #                 'ny_open_data', 'ct_open_data', 'maryland_open_data', 'pa_open_data',
    #                 'texas_open_data', 
    #                 'wa_open_data', 'sf_open_data', 'la_open_data', 
    #                 'nyc_open_data', 'chicago_open_data'
    # ]
    # # data_sources = ['nyc_open_data', 'chicago_open_data']
    # print(profile_data_sources(data_sources, TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT))
    data_source = 'data_commons'
    print(profile_data_source(data_source, None, SPATIAL_GRANU.TRACT))