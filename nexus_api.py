from data_search.search_corr import CorrSearch, Correlation
from data_search.commons import FIND_JOIN_METHOD
import pandas as pd
from utils.time_point import T_GRANU
from utils.coordinate import S_GRANU
from utils.io_utils import load_corrs_to_df
from data_search.db_ops import join_two_agg_tables_api, read_agg_tbl
import psycopg2
import os
import json
import utils.io_utils as io_utils

class API:
    def __init__(self, conn_str, data_sources=['chicago_1m_zipcode'], impute_options=[], correction=''):
        self.conn_str = conn_str
        conn_copg2 = psycopg2.connect(self.conn_str)
        self.cur = conn_copg2.cursor()
        self.data_sources = data_sources
        self.correction = correction
        self.impute_options = impute_options

        self.catalog = {}

        for data_source in data_sources:
            config = io_utils.load_config(data_source)
            attr_path = config["attr_path"]
            self.catalog.update(io_utils.load_json(attr_path))
          
        self.disp_attrs = [
            "tbl_id1",
            "tbl_name1",
            "agg_tbl1",
            "agg_attr1",
            "tbl_id2",
            "tbl_name2",
            "agg_tbl2",
            "agg_attr2",
            "missing_ratio_o2",
            "r_val",
            "p_val",
            "samples"]
    
    def find_correlations_from(self, dataset, t_granu, s_granu, overlap_t, r_t, corr_type="pearson", control_vars=[]):
        corr_search = CorrSearch(
            self.conn_str,
            self.data_sources,
            FIND_JOIN_METHOD.JOIN_ALL,
            impute_methods=self.impute_options,
            explicit_outer_join=False,
            correct_method=self.correction,
            q_val=0.05,
        )
        corr_search.set_join_cost(t_granu, s_granu, overlap_t)
        corr_search.find_all_corr_for_a_tbl(dataset, [t_granu, s_granu], overlap_t, r_t, p_t=0.05, fill_zero=True, corr_type=corr_type)
        corrs = load_corrs_to_df(corr_search.data)
        corrs['agg_attr1'] = corrs['agg_attr1'].str[:-3]
        corrs['agg_attr2'] = corrs['agg_attr2'].str[:-3]
        print(f"total number of correlations: {len(corrs)}")
        return corrs[self.disp_attrs]

    def get_aligned_data(self, row):
        agg_name1=row['agg_tbl1']
        agg_attr1 = row['agg_attr1']
        agg_name2=row['agg_tbl2']
        agg_attr2 = row['agg_attr2']
        unagg_flag = False
        if agg_attr1[0:4] != 'avg_':
            agg_attr1 = 'avg_' + agg_attr1
            unagg_flag = True
            
        df = join_two_agg_tables_api(self.cur, agg_name1, agg_attr1, agg_name2, agg_attr2, outer=False)
        df[agg_attr1] = df[agg_attr1].astype(float)
        if unagg_flag:
            df = df.rename(columns={agg_attr1: agg_attr1[4:]})
        provenance = f"{agg_name1} JOIN {agg_name2}"
        return df, provenance

    def save(self, df, path, name, provenance=None):
        df.to_csv(os.path.join(path, name), index=False)
        if provenance:
            json.dump(provenance, open(f'{path}/{name}_prov.json', 'w'))
    
    def show_raw_dataset(self, id):
        link = self.catalog[id]['link']
        return link

    def show_agg_dataset(self, agg_tbl_name):
        df = read_agg_tbl(self.cur, agg_tbl_name)
        return df

if __name__ == '__main__':
    conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
    nexus_api = API(conn_str)
    dataset = 'asthma'
    t_granu, s_granu = None, S_GRANU.ZIPCODE
    overlap_t = 5
    r_t = 0.5
    df = nexus_api.find_correlations_from(dataset, t_granu, s_granu, overlap_t, r_t, corr_type="pearson")
    print(len(df))
    print(df.loc[0])
    aligned = nexus_api.get_aligned_data(df.loc[0])