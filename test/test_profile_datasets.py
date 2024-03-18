from nexus.data_ingestion.data_profiler import Profiler
from utils.data_model import TEMPORAL_GRANU, SPATIAL_GRANU


def test_profile_col_stats():
    t_scales = [TEMPORAL_GRANU.DAY, TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.BLOCK, SPATIAL_GRANU.TRACT]
    profiler = Profiler("chicago_10k", t_scales, s_scales)
    profiler.collect_agg_tbl_col_stats()


def test_profile_original_data():
    t_scales = [TEMPORAL_GRANU.DAY, TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.BLOCK, SPATIAL_GRANU.TRACT]
    profiler = Profiler("chicago_1m", t_scales, s_scales)
    profiler.profile_original_data()


def test_get_avg_rows():
    t_scales = [TEMPORAL_GRANU.DAY]
    s_scales = [SPATIAL_GRANU.BLOCK]
    profiler = Profiler("chicago_1m", t_scales, s_scales)
    profiler.count_avg_rows(t_scales[0], s_scales[0], 1000)


test_get_avg_rows()
# test_profile_col_stats()
# test_profile_original_data()
