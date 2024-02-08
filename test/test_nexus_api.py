from data_search.search_corr import CorrSearch, Correlation
from data_search.commons import FIND_JOIN_METHOD
import pandas as pd
from utils.time_point import T_GRANU
from utils.coordinate import S_GRANU
from utils.io_utils import load_corrs_to_df
from data_search.db_ops import join_two_agg_tables_api
import psycopg2
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
    # df_formatted = show_df(df, name='asthma_corrs', use_qgrid=False)

def test_show_catalog():
    catalog = nexus_api.show_catalog()
    print(catalog)

if __name__ == '__main__':
    test_find_correlations_from()
    test_show_catalog()