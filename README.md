# Nexus: Correlation Discovery over Collections of Spatio-Temporal Tabular Data

## Install

```bash
$ git clone git@github.com:TheDataStation/nexus_correlation_discovery.git

# optional: setup virtual environment
$ conda create -n nexus python=3.11.5 -y
$ conda activate nexus

# install Nexus locally. This step will install all dependencies automatically.
$ pip install -e . 
```

## Quickstart

We have prepared some data for you to explore Nexus easily. We include the following datasets.

- [chicago open data](https://uchicago.box.com/s/8v4fqtkvrq9uhj85g6y8048w4o6xp91g)
- [asthma data in chicago](data/asthma)
- [chicago factors including population and median income](data/chicago_factors)

These datasets are ingested into a [duckdb database](https://uchicago.box.com/s/v650de4zatbzk1yzvtuppfns78a2xhjc) and converted to the zipcode granularity.

To run the quickstart, you need to first download the above pre-ingested duckdb database and then put it under the `data` directory.

We prepare a [notebook](demo/nexus_api.ipynb) for you to explore the functionalities offered by Nexus, including identifying correlations, controlling for variables, and extracting patterns from these correlations. You can learn how to use Nexus API from this notebook.

## Add your own data

This [notebook](demo/data_ingestion.ipynb) introduces how to incorporate your own data.

### Add a data source

In this stage, a data source is added, and Nexus will annotate spatial/temporal and numerical attributes for datasets within this source, storing this information accordingly.

To add a data source, you need to specify the name of the data source, the path to the data and a list of spatial hierarchies. To define a spatial hierarchy, you need to provide a shape file, and the mapping between spatial granularities to the shape file headers. You can also add a data source without specifying a spatial hierarchy if you don't need to convert attributes with geographical coordinate granularity into other granularities.

The data source will be added to `config.yaml` by default. You could change the path to store data source configurations by doing

```bash
$ export CONFIG_FILE_PATH="path_to_your_configuration_file"
```

As directory containing all the metadata related to that data source will be created at `resource/{data source name}`.


```python
from nexus.nexus_api import API

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
from nexus.utils.spatial_hierarchy import SPATIAL_GRANU
from nexus.utils.time_point import TEMPORAL_GRANU

data_sources = ['chicago_open_data']
conn_str = 'data/test.db'
temporal_granu_l = []
spatial_granu_l = [SPATIAL_GRANU.ZIPCODE]
API.ingest_data(conn_str=conn_str, engine='duckdb', data_sources=data_sources,
                temporal_granu_l=temporal_granu_l, spatial_granu_l=spatial_granu_l)
```

## Datasets used in the paper

All datasets used in the Nexus paper can be downloaded [here](https://uchicago.box.com/s/v650de4zatbzk1yzvtuppfns78a2xhjc).


## Open Data Crawler

If you want to download datasets from open data portals, please refer to `nexus/data_prep/opendata_client.py` and `nexus/data_prep/table_downloader.py`. 

### Create app tokens and api keys on Socrata

Socrata is a platform that manages many open data portals. To use the above scripts, you need to first obtain an API key. Please follow this blog to get the key first https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys

### Get all dataset information under an open data portal

You can use `OpenDataClient` under `opendata_client.py` to get the catalog of datasets under a portal.

```python
domain = "data.cityofchicago.org"
client = OpenDataClient(domain, "https://data.cityofchicago.org/resource/", "Your App Token")
res = client.datasets(domain)
datasets_to_download = []
for obj in res:
    id = obj['resource']['id']
    name = obj['resource']['name']
    datasets_to_download.append([domain, id])
import json
with open('chicago_open_data.json', 'w') as f:
    json.dump(datasets_to_download, f, indent=4)
```

### Download datasets

`table_downloader.py` downloads the given datasets in parallel. It takes as input a list of (open data domain, dataset id) pairs.

```python
line_limit = 500000
dataset_dir = f"data/chicago_open_data/" # path to save the downloaded datasets
config = io_utils.load_config("data_prep")
root_dir, app_token = config["root_dir"], config["app_token"]
if not os.path.isdir(dataset_dir):
    os.makedirs(dataset_dir)
data_downloader = TableDownloader(
    output_dir=dataset_dir, app_token=app_token
)
meta_file = 'chicago_open_data.json' # the one we obtained from the previous step with domain name and dataset id.
data_downloader.download_all(line_limit, meta_file)
```

## Citation

```
@article{10.1145/3654957,
author = {Gong, Yue and Galhotra, Sainyam and Castro Fernandez, Raul},
title = {Nexus: Correlation Discovery over Collections of Spatio-Temporal Tabular Data},
year = {2024},
issue_date = {June 2024},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
volume = {2},
number = {3},
url = {https://doi.org/10.1145/3654957},
doi = {10.1145/3654957},
abstract = {Causal analysis is essential for gaining insights into complex real-world processes and making informed decisions. However, performing accurate causal analysis on observational data is generally infeasible, and therefore, domain experts start exploration with the identification of correlations. The increased availability of data from open government websites, organizations, and scientific studies presents an opportunity to harness observational datasets in assisting domain experts during this exploratory phase.In this work, we introduce Nexus, a system designed to align large repositories of spatio-temporal datasets and identify correlations, facilitating the exploration of causal relationships. Nexus addresses the challenges of aligning tabular datasets across space and time, handling missing data, and identifying correlations deemed "interesting". Empirical evaluation on Chicago Open Data and United Nations datasets demonstrates the effectiveness of Nexus in exposing interesting correlations, many of which have undergone extensive scrutiny by social scientists.},
journal = {Proc. ACM Manag. Data},
month = may,
articleno = {154},
numpages = {28},
keywords = {correlation analysis, data discovery, hypothesis generation, spatio-temporal data}
}
```