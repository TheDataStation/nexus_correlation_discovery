from nexus.data_prep.table_downloader import TableDownloader
from config import ROOT_DIR, APP_TOKEN
from os import path

dataset_dir = "data/chicago_open_data_1m/"
output_dir = path.join(ROOT_DIR, dataset_dir)

data_downloader = TableDownloader(output_dir=output_dir, app_token=APP_TOKEN)

file_info = {
    "domain": "data.cityofchicago.org",
    "file_name": "wrvz-psew",
    "line_limit": 1000000,
    "app_token": APP_TOKEN,
}

data_downloader.download_file(file_info)
