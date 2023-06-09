from data_ingestion.profile_datasets import Profiler
from data_search.data_model import T_GRANU, S_GRANU


def test_profile_col_stats():
    t_scales = [T_GRANU.DAY, T_GRANU.MONTH]
    s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]
    profiler = Profiler("chicago_10k", t_scales, s_scales)
    profiler.collect_agg_tbl_col_stats()


def test_profile_original_data():
    t_scales = [T_GRANU.DAY, T_GRANU.MONTH]
    s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]
    profiler = Profiler("chicago_1m", t_scales, s_scales)
    profiler.profile_original_data()


test_profile_col_stats()
# test_profile_original_data()
