# Discover and Navigate correlations in spatio-temporal datasets

## Prepare spatio-temporal datasets

### Collect metadata
```bash
$ python data_prep/st_tbl_collector.py
```

Run `st_tbl_collector.py` to get all metadata in an open data portal.

Metadata will be stored as a json file in a given location.

In the main function of st_tbl_collector, we collect metadata about all tables with spatio-temporal attributes in chicago open data portal. The metadata is stored in `data/st_table_chicago_open_data.json`

### Download datasets
```bash
$ python data_prep/table_downloader.py
```

Run `table_downloader.py` to download tables. If you want to download all tables from an open data portal, you can give the metadata 
path to the downloader, and the downloader will collect all table ids and download all of them.

Note there is a line_limit parameter in the downloader. If you set it to 1000, we will download the first 1000 rows of each table.
When you set it to 0, we download all rows in a table, which could make your downloading process pretty slow (there are tables with tens of millions of rows).

## Ingest data and Build Discovery Index

```bash
python test/test_ingest_to_db.py
```

## Profile datasets
```bash

```