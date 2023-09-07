from sodapy import Socrata
import utils.io_utils as io_utils
from os import path
from typing import List
from data_prep.prep_utils import is_num_column_valid

"""
Tables that contain spatial or temporal attributes
"""


class STTable:
    def __init__(self, domain: str, tbl_name: str, tbl_id: str):
        self.domain = domain
        self.tbl_name = tbl_name
        self.tbl_id = tbl_id
        self.t_attrs = []
        self.s_attrs = []
        self.num_columns = []

    def add_t_attr(self, t_attr: str):
        self.t_attrs.append(t_attr)

    def add_s_attr(self, s_attr: str):
        self.s_attrs.append(s_attr)

    def add_num_attr(self, num_attr: str):
        self.num_columns.append(num_attr)

    def is_valid(self):
        start_date, end_date = False, False
        s_i, j_i = 0, 0
        for i, t_attr in enumerate(self.t_attrs):
            if "start_" in t_attr:
                start_date = True
                s_i = t_attr
            if "end_" in t_attr:
                end_date = True
                j_i = t_attr
        if start_date and end_date:
            self.t_attrs.remove(s_i)
            self.t_attrs.remove(j_i)
        return len(self.t_attrs) != 0 or len(self.s_attrs) != 0

def is_t_attr_valid(t_attr: str):
    if "updated" in t_attr or "data_as_of" in t_attr:
        return False
    else:
        return True

class STTableDetector:
    """Detect tables with spatial and temporal attributes information from a open data domain
    Args:
        domain: open data domain. e.g., data.cityofchicago.org
    """

    def __init__(self, domains: List[str], app_token):
        self.domains = domains
        self.app_token = app_token
        self.date_types = ["Calendar date", "Date"]  # column tags representing dates
        self.location_types = [
            "Location",
            "Geospatial",
            "Point",
        ]  # column tags representing locations

        self.temporal_cnt, self.spatial_cnt = 0, 0
        self.st_tables = []

    def detect(self):
        for domain in self.domains:
            print(domain)
            print("domain name: {}".format(domain))
            client = Socrata(domain, self.app_token)
            data = client.datasets(only=["dataset"])
            print("total number of datasets:", len(data))
            for obj in data:
                resource = obj["resource"]
                tbl_name = resource["name"]
                tbl_id = resource["id"]
                column_types = resource["columns_datatype"]
                column_names = resource["columns_field_name"]
                tbl_obj = STTable(domain, tbl_name, tbl_id)

                for i, column_type in enumerate(column_types):
                    if column_type in self.date_types:
                        column_name = column_names[i]
                        if is_t_attr_valid(column_name):
                            tbl_obj.add_t_attr(column_name)
                        self.temporal_cnt += 1
                    elif column_type in self.location_types:
                        column_name = column_names[i]
                        tbl_obj.add_s_attr(column_name)
                        self.spatial_cnt += 1
                    elif column_type == "Number":
                        column_name = column_names[i]
                        if not is_num_column_valid(column_name):
                            continue
                        if ":@" in column_name:
                            continue
                        tbl_obj.add_num_attr(column_name)

                if tbl_obj.is_valid():
                    if "cdc_case_earliest_dt" in tbl_obj.t_attrs:
                        tbl_obj.t_attrs = ["cdc_case_earliest_dt"]
                    self.st_tables.append(tbl_obj.__dict__)
                   
            client.close()
            print("detected {} st_tables in total".format(len(self.st_tables)))

    def serialize(self, output_path):
        # output_path: path of the json file to store the output table information
        io_utils.dump_json(output_path, self.st_tables)


if __name__ == "__main__":
    # output_path = path.join(ROOT_DIR, "data/cdc_open_data.json")
    config = io_utils.load_config("data_prep")
    root_dir, app_token = config["root_dir"], config["app_token"]
    # output_path = "data/cdc_open_data.json"
    output_path = None
    # domain = ["data.cdc.gov"]
    domain = ["data.cdc.gov"]
    # print(output_path)

    st_table_detector = STTableDetector(domain, app_token)
    st_table_detector.detect()
    st_table_detector.serialize(output_path)
