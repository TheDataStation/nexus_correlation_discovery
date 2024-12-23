from nexus.data_ingestion.data_profiler import Profiler
import nexus.utils.io_utils as io_utils
import time
from utils.spatial_hierarchy import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from nexus.data_ingestion.data_ingestor import DBIngestor
from tqdm import tqdm
from nexus.utils.io_utils import dump_json

if __name__ == "__main__":
    data_source = "chicago_1m"
    config = io_utils.load_config(data_source)
    conn_string = config["db_path"]
    conn_string = "postgresql://yuegong@localhost/chicago_1m_new"
    t_scales = [TEMPORAL_GRANU.DAY, TEMPORAL_GRANU.MONTH]
    s_scales = [SPATIAL_GRANU.BLOCK, SPATIAL_GRANU.TRACT]

    ingestor = DBIngestor(conn_string, data_source, t_scales, s_scales)
    perf_profile = {}
    granu_lists = [[TEMPORAL_GRANU.DAY, SPATIAL_GRANU.BLOCK], [TEMPORAL_GRANU.MONTH, SPATIAL_GRANU.TRACT]]
    for granu_list in granu_lists:
        print(granu_list)
        start =   time.time()
        profiler = Profiler(data_source, t_scales, s_scales)
        all_schemas = profiler.load_all_spatio_temporal_keys(granu_list[0], granu_list[1])
        k = 256
        for tbl, schema in tqdm(all_schemas):
            agg_name = schema.get_agg_tbl_name(tbl)
            ingestor.create_correlation_sketch(agg_name, k)
        perf_profile[tuple(granu_list)] = time.time() - start

    dump_json('perf_profile_create_corr_sketch.json', perf_profile)