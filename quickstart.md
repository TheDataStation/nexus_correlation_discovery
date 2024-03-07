[WIP] Nexus QuickStart Documentation

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

## Configure a data source

An example data source configuration.

Users need to specify the following information to configure a data source

`data_path`: the path to store the raw data

`meta_path`: path to store the metadata for the raw data. 

`shape_path`: the shape file for the desired spatial granularity hierarchy. 

`geo_chain`: spatial granularity names following the spatial hierarchy order

`geo_keys`: spatial granularity names as in the shape file following the spatial herarchy order.


The following fields are created automatically by Nexus and do not need to be specified manually by the users.

`catalog_path`: path to store the metadata for the ingested data. This metadata file stores information about the ingested data. Its format is the same as the one `meta_path`. The only difference is that it only stores the attributes that are ingested successfully.

`profile_path`: store statistics about the raw data

`col_stats_path`: store statistics about column data in the aggregated tables

`failed_tbl_path`: tables that failed to be loaded into the database


```yml
asthma:
    data_path: "data/asthma/"
    meta_path: "resource/asthma/asthma_data.json"
    shape_path: "resource/chicago_shapes/shape_chicago_zipcodes/geo_export_a86acac7-4554-4a8c-b482-7e49844799cf.shp"
    geo_chain: "zipcode"
    geo_keys: "zip"
    catalog_path: "resource/asthma/tbl_attrs.json"
    profile_path: "resource/asthma/profile.json"
    col_stats_path: "resource/asthma/col_stats.json"
    failed_tbl_path: "resource/asthma/failed_tbl_ids.json"
```

Example metadata format for a dataset. Nexus labels spatial/temporal attributes and their granularities automatically. But it relies on the labels from open data portals right now. If users want to add datasets that are not from those open data portals, they'll need to label attributes manually.

```json
{
    "asthma": {
        "tbl_id": "asthma",
        "tbl_name": "asthma",
        "t_attrs": [],
        "s_attrs": [
            {
                "name": "Zip5",
                "granu": "ZIPCODE"
            }
        ],
        "num_columns": [
            "enc_asthma",
            "encAsthmaExac",
            "AttackPer"
        ],
        "domain": "uchicago medical school",
        "link": "unknown"
    }
}

```

### Caveats

- []  Nexus labels spatial/temporal attributes and their granularities automatically. But it relies on the labels from open data portals right now. If users want to add datasets that are not from those open data portals, they'll need to label attributes manually.

- [] The current setup only allows users to specify a single spatial hierarchy. If they want to specify a different spatial hierarchy, they'll need to create a new data source. Need to fix that and allow users to specify multiple spatial hierarchies within a data source configuration.

- [] Add APIs for adding data sources instead of letting users edit config.yaml manually.

