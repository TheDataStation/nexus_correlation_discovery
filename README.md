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

### Create postgres database

#### On Apple M1
Follow [this documentation](https://gist.github.com/phortuin/2fe698b6c741fd84357cec84219c6667) to set up postgresql on MacOS with apple M1.

#### On Ubuntu 20.04
Install postgres
```bash
$ sudo apt update
$ sudo apt install postgresql postgresql-contrib
# start postgres service
$ sudo systemctl start postgresql.service
```

Go to `/etc/postgresql/12/main/pg_hba.conf`, change all methods to `trust`

Create a new role
```bash
# switch to postgres account
$ sudo -i -u postgres
# access the PostgreSQL prompt
$ psql
# begin to create a new user
postgres-> CREATE ROLE myuser WITH LOGIN;
postgres-> ALTER ROLE myuser CREATEDB;
postgres-> \q
```

Create a new database with the new user
```bash
$ psql postgres -U <username>
postgres-> CREATE DATABASE <database_name>;
postgres-> GRANT ALL PRIVILEGES ON DATABASE <database_name> TO <username>;
```

```bash
python test/test_ingest_to_db.py
```