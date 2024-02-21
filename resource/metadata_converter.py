from utils import io_utils

def add_domain(path, domain, out_path):
    # add domain attribute to all metadata
    tbl_attrs = io_utils.load_json(path)
    for tbl_id, info in tbl_attrs.items():
        info["domain"] = domain
        tbl_attrs[tbl_id] = info
    io_utils.dump_json(out_path, tbl_attrs)

def remove_datasets(path, out_path):
    # remove datasets that are not in the list
    tbl_attrs = io_utils.load_json(path)
    new_tbl_attrs = {}
    for id, info in tbl_attrs.items():
        if len(info["t_attrs"]) == 0 and len(info["s_attrs"]) == 0:
            continue
        new_tbl_attrs[id] = info
    io_utils.dump_json(out_path, new_tbl_attrs)

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

def convert_old_metadata_to_new_format(in_p, out_p):
    old_metadata = io_utils.load_json(in_p)
    new_metadata = {}
    for info in old_metadata:
        t_attr_names = info["t_attrs"]
        s_attrs_names = info["s_attrs"]
        t_attrs, s_attrs = [], []
        for attr in t_attr_names:
            t_attrs.append({"name": attr, "granu": "DateTime"})
        for attr in s_attrs_names:
            s_attrs.append({"name": attr, "granu": "POINT"})
        info['t_attrs'] = t_attrs
        info['s_attrs'] = s_attrs
        new_metadata[info["tbl_id"]] = info
    io_utils.dump_json(out_p, new_metadata)

def convert_old_tbl_attrs_to_new_format(in_p, out_p):
    old_metadata = io_utils.load_json(in_p)
    new_metadata = {}
    for tbl_id, info in old_metadata.items():
        t_attr_names = info["t_attrs"]
        s_attrs_names = info["s_attrs"]
        t_attrs, s_attrs = [], []
        for attr in t_attr_names:
            t_attrs.append({"name": attr, "granu": "DateTime"})
        for attr in s_attrs_names:
            if tbl_id == 'asthma':
                s_attrs.append({"name": attr, "granu": "ZIPCODE"})
            else:
                s_attrs.append({"name": attr, "granu": "POINT"})
        info['t_attrs'] = t_attrs
        info['s_attrs'] = s_attrs
        new_metadata[tbl_id] = info
    io_utils.dump_json(out_p, new_metadata)

if __name__ == '__main__':
    # convert_old_tbl_attrs_to_new_format('resource/chicago_1m/tbl_attrs_chicago_1m_new.json', 'resource/chicago_1m/tbl_attrs_chicago_1m_latest.json')
    convert_old_tbl_attrs_to_new_format('resource/chicago_1m_zipcode/tbl_attrs_chicago_1m_bk3.json', 'resource/chicago_1m_zipcode/tbl_attrs_chicago_1m.json')
    # remove_datasets("resource/chicago_1m_zipcode/tbl_attrs_chicago_1m_bk2.json",  "resource/chicago_1m_zipcode/tbl_attrs_chicago_1m.json")
    # add_link("resource/chicago_1m_zipcode/tbl_attrs_chicago_1m_bk.json", 'resource/chicago_1m_zipcode/chicago_open_data_linked.json', "resource/chicago_1m_zipcode/tbl_attrs_chicago_1m.json")
    # add_domain("resource/chicago_open_data/tbl_attrs_chicago_bk.json", 'data.cityofchicago.org', "resource/chicago_open_data/tbl_attrs_chicago_bk.json")
    # add_domain("resource/nyc_open_data/tbl_attrs_nyc_bk.json", 'data.cityofnewyork.us', "resource/nyc_open_data/tbl_attrs_nyc_bk.json")
    # add_domain("resource/chicago_1m/tbl_attrs_chicago_1m.json", 'data.cityofchicago.org', "resource/chicago_1m/tbl_attrs_chicago_1m_new.json")
    # check_ny_and_nyc()