from data_search.search_corr import CorrSearch, Correlation
from data_search.commons import FIND_JOIN_METHOD
import pandas as pd
from utils.time_point import T_GRANU
from utils.coordinate import S_GRANU
from utils.io_utils import load_corrs_to_df
from data_search.db_ops import join_two_agg_tables_api, read_agg_tbl, join_multi_vars
import psycopg2
import os
import json
import utils.io_utils as io_utils
from data_search.data_model import Var
from typing import List
from sklearn import linear_model

class API:
    def __init__(self, conn_str, data_sources=['chicago_1m_zipcode', 'chicago_factors'], impute_options=[], correction=''):
        self.conn_str = conn_str
        conn_copg2 = psycopg2.connect(self.conn_str)
        self.cur = conn_copg2.cursor()
        self.data_sources = data_sources
        self.correction = correction
        self.impute_options = impute_options

        self.catalog = {}
        self.data_path_map = {}
        for data_source in data_sources:
            config = io_utils.load_config(data_source)
            attr_path = config["attr_path"]
            self.catalog.update(io_utils.load_json(attr_path))
            self.data_path_map[data_source] = config["data_path"]
          
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
        corr_search.find_all_corr_for_a_tbl(dataset, [t_granu, s_granu], overlap_t, r_t, p_t=0.05, fill_zero=True, corr_type=corr_type, control_vars=control_vars)
        corrs = load_corrs_to_df(corr_search.data)
        corrs['agg_attr1'] = corrs['agg_attr1'].str[:-3]
        corrs['agg_attr2'] = corrs['agg_attr2'].str[:-3]
        print(f"total number of correlations: {len(corrs)}")
        return corrs[self.disp_attrs]

    def find_all_correlations(self, t_granu, s_granu, overlap_t, r_t, persist_path=None, corr_type="pearson", control_vars=[]):
        corr_search = CorrSearch(
            self.conn_str,
            self.data_sources,
            FIND_JOIN_METHOD.JOIN_ALL,
            impute_methods=self.impute_options,
            explicit_outer_join=False,
            correct_method='FDR',
            q_val=0.05,
        )
        corr_search.set_join_cost(t_granu, s_granu, overlap_t)
        corr_search.find_all_corr_for_all_tbls([t_granu, s_granu], overlap_t, r_t, p_t=0.05, corr_type=corr_type, fill_zero=True, dir_path=persist_path)
        corrs = load_corrs_to_df(corr_search.all_corrs)
        corrs['agg_attr1'] = corrs['agg_attr1'].str[:-3]
        corrs['agg_attr2'] = corrs['agg_attr2'].str[:-3]
        print(f"total number of correlations: {len(corrs)}")
        return corrs[self.disp_attrs]

    def regress(self, target_var: Var, covariates: List[Var], reg):
        df, _ = join_multi_vars(self.cur, [target_var]+covariates)
        x = df[[var.attr_name for var in covariates]]
        y = df[target_var.attr_name]
        model = reg.fit(x, y)
        r_sq = model.score(x, y)
        return model, r_sq, df

    def assemble(self, vars: List[Var], constraints=None):
        df = join_multi_vars(self.cur, vars, constraints=constraints)
        return df
    
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
    
    def show_catalog(self):
        data = []
        # create a dataframe from catalog
        for id, info in self.catalog.items():
            if "link" in info:
                data.append([id, info['name'], info['link']])
        df = pd.DataFrame(data, columns=['id', 'name', 'link'])

        return df
    
    def show_raw_dataset(self, id):
        # todo: map data source to data path
        data_path = "/home/cc/nexus_correlation_discovery/data/chicago_open_data_1m/"
        df = pd.read_csv(f"{data_path}/{id}.csv")
        link = self.catalog[id]['link']
        return df, link

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

    # test regress
    target_var = Var('asthma_Zip5_6', 'avg_enc_asthma')
    covariates = [Var('ijzp-q8t2_location_6', 'count'), Var('n26f-ihde_pickup_centroid_location_6', 'avg_tip')]
    reg_model = linear_model.LinearRegression() # OLS regression
    model, rsq, merged = nexus_api.regress(target_var, covariates, reg_model)
    print(model.coef_)
    print(rsq)