import time
import utils.io_utils as io_utils
import time
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from data_ingestion.index_builder_agg import DBIngestorAgg
from tqdm import tqdm

start_time = time.time()
data_source = "cdc_1m"
config = io_utils.load_config(data_source)
conn_string = config["db_path"]
t_scales = [T_GRANU.DAY]
s_scales = [S_GRANU.STATE]

ingestor = DBIngestorAgg(conn_string, data_source, t_scales, s_scales)
ingestor.ingest_data_source(clean=True, persist=True)

print(f"ingesting data finished in {time.time() - start_time} s")
