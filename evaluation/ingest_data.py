import time
from tqdm import tqdm
import time
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from data_ingestion.index_builder_agg import DBIngestorAgg

start_time = time.time()
conn_string = "postgresql://yuegong@localhost/chicago_open_data_1m"
t_scales = [T_GRANU.DAY, T_GRANU.MONTH]

s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]

ingestor = DBIngestorAgg(conn_string, t_scales, s_scales)
ingestor.ingest_data_source("chicago_1m", clean=True, persist=True)

time.time() - start_time
