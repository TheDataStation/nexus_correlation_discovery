# Nexus QuickStart

## Install

```bash
$ git clone git@github.com:TheDataStation/nexus_correlation_discovery.git
# install Nexus locally. This step will install all dependencies automatically.
$ pip install -e . 
```

## Quick Start

We have prepared some data for you to explore the capacity of Nexus easily. We included the following datasets. (the links to these data include metadata only. Will add links to the original data soon)

- [chicago open data](resource/chicago_1m_zipcode/chicago_open_data.json)
- [asthma data in chicago](resource/asthma/asthma_data.json)
- [chicago factors including population and median income](/home/cc/nexus_correlation_discovery/resource/chicago_factors/chicago_factors_data.json)

These datasets are ingested into a [duckdb database](data/quickstart.db)and converted to the zipcode granularity.

We also prepare a [notebook](demo/nexus_api.ipynb) for you to explore the various functionalities offered by Nexus, including identifying correlations, controlling for variables, and extracting patterns from these correlations. You can learn how to use Nexus API from this notebook

## Add your own data

You can follow these steps to incorporate your own data.

### Add a data source

In this stage, a data source is added, and Nexus will annotate spatial/temporal and numerical attributes for datasets within this source, storing this information accordingly.

To add a data source, you need to specify the name of the data source, the path to the data and a list of spatial hierarchies. To define a spatial hierarchy, you need to provide a shape file, and the mapping between spatial granularities to the shape file headers.



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

### Data Ingestion

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
