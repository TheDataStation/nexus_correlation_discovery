from utils.coordinate import SPATIAL_GRANU
from utils.data_model import Var
from nexus_api import API

# conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
# nexus_api = API(conn_str)

def test_find_correlations_from():
    conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
    nexus_api = API(conn_str)
    dataset = 'asthma'
    t_granu, s_granu = None, SPATIAL_GRANU.ZIPCODE
    overlap_t = 5
    r_t = 0.5
    df = nexus_api.find_correlations_from(dataset, t_granu, s_granu, overlap_t, r_t, correlation_type="pearson")
    print(len(df))

def test_show_catalog():
    catalog = nexus_api.show_catalog()
    print(catalog)

def test_find_all_correlations():
    conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
    nexus_api = API(conn_str, data_sources=['chicago_1m_zipcode', 'chicago_factors'])
    t_granu, s_granu = None, SPATIAL_GRANU.ZIPCODE
    overlap_t = 10
    r_t = 0.6
    control_vars = [Var('chicago_income_by_zipcode_zipcode_6', 'avg_income_household_median')]
    persist_path = f'tmp/chicago_open_data_zipcode_control_for_income/'
    df = nexus_api.find_all_correlations(t_granu, s_granu, overlap_t, r_t, persist_path=persist_path, correlation_type="pearson", control_variables=control_vars)
    print(len(df))

    control_vars = [Var('chicago_zipcode_population_zipcode_6', 'avg_population')]
    persist_path = f'tmp/chicago_open_data_zipcode_control_for_population/'
    df = nexus_api.find_all_correlations(t_granu, s_granu, overlap_t, r_t, persist_path=persist_path, correlation_type="pearson", control_variables=control_vars)
    print(len(df))

    # t_granu, s_granu = None, S_GRANU.TRACT
    # overlap_t = 10
    # r_t = 0.6
    # conn_str = "postgresql://yuegong@localhost/chicago_1m_new"
    # nexus_api = API(conn_str, data_sources=['chicago_1m', 'chicago_factors'])
    # # control_vars = [Var('chicago_census_tract_population_census_tract_3', 'avg_population')]
    # control_vars = []
    # persist_path = f'tmp/chicago_open_data_tract/'
    # df = nexus_api.find_all_correlations(t_granu, s_granu, overlap_t, r_t, persist_path=persist_path, corr_type="pearson", control_vars=control_vars)
    # print(len(df))

def test_control_for_variables():
    dataset = 'asthma'
    t_granu, s_granu = None, SPATIAL_GRANU.ZIPCODE
    overlap_t = 5
    r_t = 0.5
    # control_vars = [Var('chicago_zipcode_population_zipcode_6', 'avg_population')]
    control_vars = [Var('chicago_income_by_zipcode_zipcode_6', 'avg_income_household_median')]
    df = nexus_api.find_correlations_from(dataset, t_granu, s_granu, overlap_t, r_t, corr_type="pearson", control_variables=control_vars)
    print(len(df))

def test_load_corrs():
    df = nexus_api.load_corrs_from_dir('evaluation/correlations2/chicago_1m_T_GRANU.MONTH_S_GRANU.TRACT/')
    print(len(df))

if __name__ == '__main__':
    # test_control_for_variables()
    # test_load_corrs()
    test_find_all_correlations()
    # test_find_correlations_from()