import nexus.utils.io_utils as io_utils
from typing import List
from nexus.utils.profile_utils import is_num_column_valid
from opendata_client import OpenDataClient
"""
Tables that contain spatial or temporal attributes
"""


class STTable:
    def __init__(self, domain: str, tbl_name: str, tbl_id: str, link: str):
        self.domain = domain
        self.tbl_name = tbl_name
        self.tbl_id = tbl_id
        self.link = link
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
        self.st_tables = {}

    def detect(self):
        for domain in self.domains:
            print(domain)
            print("domain name: {}".format(domain))
            #client = Socrata(domain, self.app_token)
            client = OpenDataClient(domain, f"https://{domain}/resource/", self.app_token)
            data = client.datasets(domain)
            print("total number of datasets:", len(data))
            for obj in data:
                resource = obj["resource"]
                link = obj["link"]
                tbl_name = resource["name"]
                tbl_id = resource["id"]
                column_types = resource["columns_datatype"]
                column_names = resource["columns_field_name"]
                tbl_obj = STTable(domain, tbl_name, tbl_id, link)

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
                    self.st_tables[tbl_id] = tbl_obj.__dict__
            return len(self.st_tables)
            # client.close()
            print("detected {} st_tables in total".format(len(self.st_tables)))

    def serialize(self, output_path):
        # output_path: path of the json file to store the output table information
        io_utils.dump_json(output_path, self.st_tables)


if __name__ == "__main__":
    config = io_utils.load_config("data_prep")
    root_dir, app_token = config["root_dir"], config["app_token"]
    output_path = "resource/chicago_1m_zipcode/chicago_open_data_linked.json"
    domain = "data.cityofchicago.org"
    st_table_detector = STTableDetector([domain], app_token)
    st_tbls_cnt = st_table_detector.detect()
    st_table_detector.serialize(output_path)

    # output_path = None
# domains = ["data.cityofchicago.org", "data.cityofnewyork.us", "data.lacity.org",
    #            "www.dallasopendata.com", "data.ny.gov", "data.wa.gov", "data.nj.gov",
    #            "data.sfgov.org", "data.texas.gov", "opendata.maryland.gov",
    #            "data.pa.gov", "data.cambridgema.gov", "data.ct.gov"]
    # domains = {
    #             "ny": "data.ny.gov", 
    #             "texas": "data.texas.gov", 
    #             "sf": "data.sfgov.org",
    #             "wa": 'data.wa.gov',
    #             "ct": 'data.ct.gov', 
    #             "pa": 'data.pa.gov', 
    #             "maryland": 'opendata.maryland.gov',
    #             "la": 'data.lacity.org'
    #             }
   
    # # print(output_path)
    # domain_cnt = []
    # for key, domain in domains.items():
    #     output_path = f'resource/{key}_open_data/{key}_open_data.json'
    #     st_table_detector = STTableDetector([domain], app_token)
    #     st_tbls_cnt = st_table_detector.detect()
    #     domain_cnt.append((domain, st_tbls_cnt))
    #     st_table_detector.serialize(output_path)
    # domain_cnt.sort(key=lambda x: x[1], reverse=True)
    # print(domain_cnt)
    # st_table_detector.serialize(output_path)
