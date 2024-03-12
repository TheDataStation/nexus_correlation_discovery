import time
import utils.io_utils as io_utils
import time
from utils.spatial_hierarchy import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU
from data_ingestion.data_ingestor import DBIngestor
from tqdm import tqdm

start_time = time.time()
data_source = "cdc_1m"
config = io_utils.load_config(data_source)
conn_string = config["db_path"]
t_scales = [TEMPORAL_GRANU.DAY]
s_scales = [SPATIAL_GRANU.STATE]

ingestor = DBIngestor(conn_string, data_source, t_scales, s_scales)
ingestor.ingest_data_source(None, clean=True, persist=True)

print(f"ingesting data finished in {time.time() - start_time} s")
