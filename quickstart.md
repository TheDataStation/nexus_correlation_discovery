## Data Ingestion

Step 1: create a `DBIngestor` that connects to a database. Here, we use duckdb.

Step 2: specify data sources

Step 3: speicify spatial and temporal granularities

Step 4: use `ingest_data_source` to ingest data to the database

```python
from data_ingestion.data_ingestor import DBIngestor
from utils.coordinate import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU

ingestor = DBIngestor(conn_string='data/quickstart.db', engine='duckdb')
data_sources = ['chicago_1m_zipcode', 'chicago_factors', 'asthma']
temporal_granu_l = []
spatial_granu_l = [SPATIAL_GRANU.ZIPCODE]
for data_source in data_sources:
    ingestor.ingest_data_source(data_source, temporal_granu_l=temporal_granu_l, spatial_granu_l=spatial_granu_l)
```

## Query Correlations
