from data_search.search_corr import CorrSearch, Correlation
from data_search.commons import FIND_JOIN_METHOD
import pandas as pd
from utils.time_point import T_GRANU
from utils.coordinate import S_GRANU
from data_search.data_model import Var
from demo.demo_ui import show_df
from nexus_api import API

conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
nexus_api = API(conn_str)

def test_find_correlations_from():
    dataset = 'asthma'
    t_granu, s_granu = None, S_GRANU.ZIPCODE
    overlap_t = 5
    r_t = 0.5
    df = nexus_api.find_correlations_from(dataset, t_granu, s_granu, overlap_t, r_t, corr_type="pearson")
    print(len(df))

def test_show_catalog():
    catalog = nexus_api.show_catalog()
    print(catalog)

def test_find_all_correlations():
    t_granu, s_granu = None, S_GRANU.ZIPCODE
    overlap_t = 5
    r_t = 0.5
    df = nexus_api.find_all_correlations(t_granu, s_granu, overlap_t, r_t, corr_type="pearson")
    print(len(df))

def test_control_for_variables():
    dataset = 'asthma'
    t_granu, s_granu = None, S_GRANU.ZIPCODE
    overlap_t = 5
    r_t = 0.5
    # control_vars = [Var('chicago_zipcode_population_zipcode_6', 'avg_population')]
    control_vars = [Var('chicago_income_by_zipcode_zipcode_6', 'avg_income_household_median')]
    df = nexus_api.find_correlations_from(dataset, t_granu, s_granu, overlap_t, r_t, corr_type="pearson", control_vars=control_vars)
    print(len(df))

def test_load_corrs():
    df = nexus_api.load_corrs_from_dir('evaluation/correlations2/chicago_1m_T_GRANU.MONTH_S_GRANU.TRACT/')
    print(len(df))

if __name__ == '__main__':
    # test_control_for_variables()
    test_load_corrs()