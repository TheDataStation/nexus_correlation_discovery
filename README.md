# Nexus QuickStart

## Add a data source

In this stage, a data source is added, and Nexus will annotate spatial/temporal and numerical attributes for datasets within this source, storing this information accordingly.

To add a data source, you need to specify the name of the data source, the path to the data and a list of spatial hierarchies. 



```python
spatial_hierarchy1 = SpatialHierarchy('resource/chicago_shapes/shape_chicago_blocks/geo_export_8e927c91-3aad-4b67'
                                          '-86ff-bf4de675094e.shp',
                                          {
                                              SPATIAL_GRANU.BLOCK: 'blockce10',
                                              SPATIAL_GRANU.TRACT: 'tractce10',
                                              SPATIAL_GRANU.COUNTY: 'countyfp10',
                                              SPATIAL_GRANU.STATE: 'statefp10'})

spatial_hierarchy2 = SpatialHierarchy("resource/chicago_shapes/shape_chicago_zipcodes/geo_export_a86acac7-4554"
                                        "-4a8c-b482-7e49844799cf.shp",
                                        {
                                            SPATIAL_GRANU.ZIPCODE: "zip"
                                        })

API.add_data_source(data_source_name='chicago_open_data', 
                    data_path='data/chicago_open_data_1m/', 
                    spatial_hierarchies=[spatial_hierarchy1, spatial_hierarchy2])
```

## Data Ingestion

Once a data source is added, the next step is to ingest data into a database. Nexus supports both DuckDB and Postgres.

To use the api `ingest_data`, you need to specify the connection to a database, the data sources and the desired spatial/temporal granularities.  

```python
from utils.spatial_hierarchy import SPATIAL_GRANU
from utils.time_point import TEMPORAL_GRANU

data_sources = ['chicago_open_data']
conn_str = 'data/test.db'
temporal_granu_l = []
spatial_granu_l = [SPATIAL_GRANU.ZIPCODE]
API.ingest_data(conn_str=conn_str, engine='duckdb', data_sources=data_sources,
                temporal_granu_l=temporal_granu_l, spatial_granu_l=spatial_granu_l)
```

## Use Nexus

Once the data has been ingested, you can explore the various functionalities offered by Nexus, including identifying correlations, controlling for variables, and extracting patterns from these correlations. For more detailed information, please refer to this [demo notebook](demo/nexus_api.ipynb).