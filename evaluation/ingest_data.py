import time
import utils.io_utils as io_utils
import time
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from data_ingestion.index_builder_agg import DBIngestorAgg

start_time = time.time()
data_source = "chicago_1m"
config = io_utils.load_config(data_source)
conn_string = config["db_path"]
conn_string = "postgresql://yuegong@localhost/chicago_open_data_1m"
t_scales = [T_GRANU.DAY, T_GRANU.MONTH]
s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]

ingestor = DBIngestorAgg(conn_string, data_source, t_scales, s_scales)
ingestor.ingest_data_source(clean=True, persist=True)

print(f"ingesting data finished in {time.time() - start_time} s")
