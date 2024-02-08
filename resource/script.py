from utils import io_utils

def add_domain(path, domain, out_path):
    # add domain attribute to all metadata
    tbl_attrs = io_utils.load_json(path)
    for tbl_id, info in tbl_attrs.items():
        info["domain"] = domain
        tbl_attrs[tbl_id] = info
    io_utils.dump_json(out_path, tbl_attrs)

def add_link(path, ref_path, out_path):
    tbl_attrs = io_utils.load_json(path)
    ref = io_utils.load_json(ref_path)
    for tbl_id, info in tbl_attrs.items():
        if tbl_id in ref:
            info["link"] = ref[tbl_id]["link"]
            tbl_attrs[tbl_id] = info
    io_utils.dump_json(out_path, tbl_attrs)

def check_ny_and_nyc():
    # check if there are repeated tables in ny and nyc
    ny_tbls = io_utils.load_json("resource/ny_open_data/ny_open_data.json")
    nyc_tbls = io_utils.load_json("resource/nyc_open_data/nyc_open_data.json")
    ny_tbl_ids = []
    nyc_tbl_ids = []
    for tbl in ny_tbls:
        ny_tbl_ids.append(tbl["tbl_id"])
    for tbl in nyc_tbls:
        nyc_tbl_ids.append(tbl["tbl_id"]) 
    print(set(ny_tbl_ids).intersection(set(nyc_tbl_ids)))


if __name__ == '__main__':
    add_link("resource/chicago_1m_zipcode/tbl_attrs_chicago_1m_bk.json", 'resource/chicago_1m_zipcode/chicago_open_data_linked.json', "resource/chicago_1m_zipcode/tbl_attrs_chicago_1m.json")
    # add_domain("resource/chicago_open_data/tbl_attrs_chicago_bk.json", 'data.cityofchicago.org', "resource/chicago_open_data/tbl_attrs_chicago_bk.json")
    # add_domain("resource/nyc_open_data/tbl_attrs_nyc_bk.json", 'data.cityofnewyork.us', "resource/nyc_open_data/tbl_attrs_nyc_bk.json")
    # add_domain("resource/chicago_1m/tbl_attrs_chicago_1m.json", 'data.cityofchicago.org', "resource/chicago_1m/tbl_attrs_chicago_1m_new.json")
    # check_ny_and_nyc()