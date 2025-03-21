# Nexus: Correlation Discovery over Collections of Spatio-Temporal Tabular Data

## Install

```bash
$ pip install nexus-corr-discovery
```

## Quickstart

We have prepared a [quickstart guide](https://drive.google.com/file/d/1v9x-KkIqMBhzTB6_4OP57jELEFC1kP8P/view?usp=sharing) for you to learn the basics of Nexus!

## Add your own data

This [notebook](https://drive.google.com/file/d/1H_BhpxsF0d0UawrQUQsd0OolilsY447U/view?usp=sharing) introduces how to incorporate your own data.

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