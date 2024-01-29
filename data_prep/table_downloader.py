import requests
import time
from data_prep.opendata_client import OpenDataClient
import utils.io_utils as io_utils
from os import path

# from log import get_logger
from multiprocessing import Pool as ProcessPool
from tqdm import tqdm
import os

"""
Download tables detected by st_table_detector from an open data portal 
"""


class TableDownloader:
    def __init__(self, output_dir: str, format: str = "csv", app_token: str = ""):
        self.output_dir = output_dir
        self.format = format
        self.app_token = app_token
        self.table_info = []

    def load_table_info(self, tbl_info_path: str):
        tables = io_utils.load_json(tbl_info_path)
        for table in tables:
            self.table_info.append((table["domain"], table["tbl_id"]))

    @staticmethod
    def download(url: str, local_filename: str, app_token):
        """
        Utility function that downloads a chunked response from the specified url to a local path.
        This method is suitable for larger downloads.
        """
        headers = {"X-App-Token": app_token}
        response = requests.get(url, stream=True)
        with open(local_filename, "wb") as outfile:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    outfile.write(chunk)

    def download_file(self, file_info):
        domain = file_info["domain"]
        file_name = file_info["file_name"]
        line_limit = file_info["line_limit"]
        app_token = file_info["app_token"]
        # self.logger.info("begin downloading {}".format(file_name))
        # print("begin downloading {}".format(file_name))
        if line_limit == 0:
            uri = "{}://{}/resource/{}.{}?$$app_token={}".format(
                "https", domain, file_name, self.format, self.app_token
            )
        else:
            uri = "{}://{}/resource/{}.{}?$$app_token={}&$limit={}".format(
                "https", domain, file_name, self.format, self.app_token, line_limit
            )
        s_time = time.time()
        print("begin download")
        self.download(uri, path.join(self.output_dir, file_name + ".csv"), app_token)
        # download_time = time.time() - s_time
        # self.logger.info("finish downloading {} in {} seconds".format(file_name, download_time))
        # print("finish downloading {} in {} seconds".format(file_name, download_time))
        return file_name
        # return download_time

    def download_all(self, line_limit: int, tbl_info_path: str=None, tbl_info=None):
        if tbl_info_path is not None:
            self.load_table_info(tbl_info_path)
        elif tbl_info is not None:
            self.table_info = tbl_info
        # total_time = 0
        num_wokers = os.cpu_count()
        work_pool = ProcessPool(num_wokers)
        file_info_lst = []
        for tbl in self.table_info:
            file_info = {
                "domain": tbl[0],
                "file_name": tbl[1],
                "line_limit": line_limit,
                "app_token": self.app_token,
            }
            file_info_lst.append(file_info)

        N = len(file_info_lst)
        for _ in tqdm(
            work_pool.imap_unordered(self.download_file, file_info_lst),
            total=N,
            desc="downloading tables",
        ):
            continue

        # self.logger.info("total downloading time: {} s".format(total_time))
        # print("total downloading time: {} s".format(total_time))


if __name__ == "__main__":
    # line_limit = 100000
    # dataset_dir = "data/chicago_open_data_all_tbls/"
    # config = io_utils.load_config("data_prep")
    # root_dir, app_token = config["root_dir"], config["app_token"]
    # if not os.path.isdir(dataset_dir):
    #     os.makedirs(dataset_dir)
    # data_downloader = TableDownloader(
    #     output_dir=dataset_dir, app_token=app_token
    # )
    # domain = "data.cityofchicago.org"
    # client = OpenDataClient(domain, f"https://{domain}/resource/", app_token)
    # metadata = client.datasets(domain)
    # tbl_info = []
    # id_name_map = {}
    # for obj in metadata:
    #     resource = obj["resource"]
    #     tbl_name = resource["name"]
    #     tbl_id = resource["id"]
    #     id_name_map[tbl_id] = tbl_name
    #     tbl_info.append((domain, tbl_id))
    # io_utils.dump_json("data/chicago_open_data_id_name_map.json", id_name_map)
    # data_downloader.download_all(line_limit, tbl_info=tbl_info)
    line_limit = 500000
    # meta_file = "data/cdc_open_data.json"
    domains = {
                "ny": "data.ny.gov", 
                # "texas": "data.texas.gov", 
                # "sf": "data.sfgov.org",
                # "wa": 'data.wa.gov',
                # "ct": 'data.ct.gov', 
                # "pa": 'data.pa.gov', 
                # "maryland": 'opendata.maryland.gov',
                # "la": 'data.lacity.org'
    }

    for key, domain in domains.items():
        print(f"downloading domain: {domain}")
        meta_file = f"resource/{key}_open_data/{key}_open_data.json"
        dataset_dir = f"data/{key}_open_data/"
        config = io_utils.load_config("data_prep")
        root_dir, app_token = config["root_dir"], config["app_token"]
        if not os.path.isdir(dataset_dir):
            os.makedirs(dataset_dir)
        data_downloader = TableDownloader(
            output_dir=dataset_dir, app_token=app_token
        )
        data_downloader.download_all(line_limit, meta_file)
   
    # meta_file = "resource/nyc_open_data/nyc_open_data.json"
    # dataset_dir = "data/nyc_open_data/"

    # config = io_utils.load_config("data_prep")
    # root_dir, app_token = config["root_dir"], config["app_token"]
    # if not os.path.isdir(dataset_dir):
    #     os.makedirs(dataset_dir)
    # data_downloader = TableDownloader(
    #     output_dir=dataset_dir, app_token=app_token
    # )

    # data_downloader.download_all(meta_file, line_limit)
