from nexus.utils.spatial_hierarchy import SPATIAL_GRANU, SpatialHierarchy
from nexus.utils.time_point import TEMPORAL_GRANU
from nexus.utils.data_model import Variable
from nexus.nexus_api import API
from nexus.data_ingestion.connection import ConnectionFactory
import pandas as pd


def test_add_data_sources():
    spatial_hierarchy1 = SpatialHierarchy('resource/chicago_shapes/shape_chicago_blocks/geo_export_8e927c91-3aad-4b67'
                                          '-86ff-bf4de675094e.shp',
                                          {
                                              SPATIAL_GRANU.BLOCK: 'blockce10',
                                              SPATIAL_GRANU.TRACT: 'tractce10',
                                              SPATIAL_GRANU.COUNTY: 'countyfp10',
                                              SPATIAL_GRANU.STATE: 'statefp10'})
    spatial_hierarchy2 = SpatialHierarchy("resource/chicago_shapes/shape_chicago_zipcodes/geo_export_a86acac7-4554"
                                          "-4a8c-b482-7e49844799cf.shp",
                                          {
                                              SPATIAL_GRANU.ZIPCODE: "zip"
                                          })
    API.add_data_source('chicago_test', 'data/chicago_open_data_1m/', [spatial_hierarchy1, spatial_hierarchy2])


def test_ingest_data_source_with_multiple_spatial_hierarchies():
    data_sources = ['chicago_test']
    conn_str = 'data/test.db'
    temporal_granu_l = []
    spatial_granu_l = [SPATIAL_GRANU.ZIPCODE]
    API.ingest_data(conn_str=conn_str, engine='duckdb', data_sources=data_sources,
                    temporal_granu_l=temporal_granu_l, spatial_granu_l=spatial_granu_l,
                    persist=True)


def test_find_correlations_from():
    conn_str = 'data/quickstart.db'
    nexus_api = API(conn_str, data_sources=['chicago_zipcode', 'asthma', 'chicago_factors'])
    dataset = 'asthma'
    # asthma data only has spatial attribute, thus the temporal granularity is set to ALL.
    temporal_granularity, spatial_granularity = TEMPORAL_GRANU.ALL, SPATIAL_GRANU.ZIPCODE
    overlap_threshold = 5
    correlation_threshold = 0.5
    # you can change correlation_type to 'spearman' or 'kendall'
    df = nexus_api.find_correlations_from(dataset, temporal_granularity, spatial_granularity,
                                          overlap_threshold, correlation_threshold,
                                          correlation_type="pearson")
    print(len(df))


def test_find_correlations_with_control():
    conn_str = 'data/quickstart.db'
    nexus_api = API(conn_str)
    dataset = 'asthma'
    temporal_granularity, spatial_granularity = TEMPORAL_GRANU.ALL, SPATIAL_GRANU.ZIPCODE
    overlap_threshold = 5
    correlation_threshold = 0.5
    control_variables = [Variable('chicago_income_by_zipcode_zipcode_6', 'avg_income_household_median')]
    df_control = nexus_api.find_correlations_from(dataset, temporal_granularity, spatial_granularity,
                                                  overlap_threshold, correlation_threshold,
                                                  correlation_type="pearson", control_variables=control_variables)
    print(len(df_control))


def test_show_catalog():
    catalog = nexus_api.show_catalog()
    print(catalog)

def test_pandas_dropna():
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6, pd.NA, 8, 9, 10], 'b': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    df["c"] = df['a'].dropna()
    print(df)

def test_find_all_correlations_postgres_all(find_join_method):
    conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
    nexus_api = API(conn_str, engine='postgres', data_sources=['chicago_zipcode', 'chicago_factors'])
    t_granu, s_granu = None, SPATIAL_GRANU.ZIPCODE
    overlap_t = 10
    r_t = 0.6
    # control_vars = [Variable('chicago_income_by_zipcode_zipcode_6', 'avg_income_household_median')]
    # persist_path = f'tmp/chicago_open_data_zipcode_control_for_income/'
    # df = nexus_api.find_all_correlations(t_granu, s_granu, overlap_t, r_t, persist_path=persist_path, correlation_type="pearson", control_variables=control_vars)
    # print(len(df))

    # control_vars = [Var('chicago_zipcode_population_zipcode_6', 'avg_population')]
    # persist_path = f'tmp/chicago_open_data_zipcode_control_for_population/'
    # df = nexus_api.find_all_correlations(t_granu, s_granu, overlap_t, r_t, persist_path=persist_path, correlation_type="pearson", control_variables=control_vars)
    # print(len(df))

    # control_vars = [Variable('chicago_income_by_zipcode_zipcode_6', 'avg_income_household_median'), 
    #                 Variable('chicago_zipcode_population_zipcode_6', 'avg_population')]
    control_vars = []
    # persist_path = f'tmp/chicago_open_data_zipcode_control_for_income_population/'
    persist_path = None
    df = nexus_api.find_all_correlations(t_granu, s_granu,
                                         overlap_t, r_t,
                                         persist_path=persist_path,
                                         correlation_type="pearson", control_variables=control_vars,
                                         find_join_method=find_join_method)
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


def test_find_all_correlations_duckdb_all(find_join_method):
    conn_str = 'data/quickstart.db'
    nexus_api = API(conn_str)
    nexus_api.data_sources = ['chicago_zipcode', 'chicago_factors']
    temporal_granularity, spatial_granularity = None, SPATIAL_GRANU.ZIPCODE
    overlap_threshold = 10
    correlation_threshold = 0.6
    persist_path = 'tmp/test/'
    df = nexus_api.find_all_correlations(temporal_granularity, spatial_granularity,
                                         overlap_threshold, correlation_threshold,
                                         persist_path=persist_path, correlation_type="pearson",
                                         find_join_method=find_join_method)
    print(len(df))


def test_control_for_variables():
    dataset = 'asthma'
    t_granu, s_granu = None, SPATIAL_GRANU.ZIPCODE
    overlap_t = 5
    r_t = 0.5
    # control_vars = [Var('chicago_zipcode_population_zipcode_6', 'avg_population')]
    control_vars = [Var('chicago_income_by_zipcode_zipcode_6', 'avg_income_household_median')]
    df = nexus_api.find_correlations_from(dataset, t_granu, s_granu, overlap_t, r_t, corr_type="pearson",
                                          control_variables=control_vars)
    print(len(df))


def test_load_corrs():
    df = nexus_api.load_corrs_from_dir('evaluation/correlations2/chicago_1m_T_GRANU.MONTH_S_GRANU.TRACT/')
    print(len(df))


def test_duckdb_migration_correctness():
    import duckdb
    import pandas as pd
    duckdb_conn = duckdb.connect('data/quickstart.db')
    duckdb_inverted_index = duckdb_conn.execute(
        "SELECT val, array_length(spatio_temporal_keys) as length FROM 'space_6_inv'").df()
    postgres_conn = ConnectionFactory.create_connection("postgresql://yuegong@localhost/chicago_1m_zipcode", 'postgres')
    postgres_conn.cur.execute("SELECT val, cardinality(st_schema_list) as length FROM space_6_inv")
    postgres_inverted_index = pd.DataFrame(postgres_conn.cur.fetchall(),
                                           columns=[desc[0] for desc in postgres_conn.cur.description])
    # iterate each row in duckdb_inverted_index
    maps1 = {}
    for _, row in duckdb_inverted_index.iterrows():
        maps1[row['val']] = row['length']
    maps2 = {}
    for _, row in postgres_inverted_index.iterrows():
        maps2[row['val']] = row['length']
    print(maps1 == maps2)
    # print(duckdb_inverted_index.equals(postgres_inverted_index))

def test_control_vars_for_correlations():
    import pandas as pd
    from collections import defaultdict
    import os
    os.environ["CONFIG_FILE_PATH"] = "config_test.yaml" 
    os.chdir(f"/Users/yuegong/nexus_correlation_discovery")
    # load correlations
    datasource_name = 'data_commons_no_unionable'
    all_correlations = pd.read_csv(f'{datasource_name}_correlations.csv')
    # rank variables by the number of correlations they are associated with
    count_map = defaultdict(list)
    # iterate each row in all correlations
    for index, row in all_correlations.iterrows():
        # get the two variables
        var1 = (row['table_id1'], row['agg_table1'], row['agg_attr1'])
        var2 = (row['table_id2'], row['agg_table2'], row['agg_attr2'])
        # increment the count for each variable
        count_map[var1].append(index)
        count_map[var2].append(index)
    # sort the variables by the length of the list of correlations they are associated with
    sorted_vars = sorted(count_map, key=lambda x: len(count_map[x]), reverse=True)
    # control for each variables
    from nexus.utils.data_model import Variable
    from nexus.data_search.search_corr import Correlation
    threshold = 10
    variable = sorted_vars[0]
    control_var = Variable(variable[1], variable[2], var_name=variable[2])
    # select the corresponding list of indices from a data frame
    cur_corrs = all_correlations.loc[count_map[variable]]
    correlations = []
    for index, row in cur_corrs.iterrows():
        correlations.append(Correlation.from_csv(row))
    from nexus.nexus_api import API
    datasource_name = 'data_commons_no_unionable'
    data_sources = [datasource_name]
    conn_str = f'data/{datasource_name}.db'
    nexus_api = API(conn_str, data_sources=[datasource_name])
    res = nexus_api.control_variables_for_correlaions([control_var], correlations)
    comparison = defaultdict(list)
    all_vars = set()
    for index, row in cur_corrs.iterrows():
        var1 = (row['agg_table1'], row['agg_attr1'])
        var2 = (row['agg_table2'], row['agg_attr2'])
        if var1 != control_var:
            all_vars.add(var1)
        if var2 != control_var:
            all_vars.add(var2)

    for index, row in all_correlations.iterrows():
        var1 = (row['agg_table1'], row['agg_attr1'])
        var2 = (row['agg_table2'], row['agg_attr2'])
        if var1 in all_vars and var2 in all_vars:
            key = tuple(sorted([var1, var2]))
            comparison[key].append(row['correlation coefficient'])
    
    for index, row in res.iterrows():
        var1 = (row['agg_table1'], row['agg_attr1'])
        var2 = (row['agg_table2'], row['agg_attr2'])
        key = tuple(sorted([var1, var2]))
        if key in comparison:
            comparison[key].append(row['correlation coefficient'])
            comparison[key].append(row['p value'])
    decrease_cnt = 0
    no_value = 0
    not_significant = 0
    for k, v in comparison.items():
        if len(v) == 1:
            no_value += 1
        if len(v) > 1:
            if v[2] > 0.05:
                not_significant += 1
                continue
            before_control, after_control = v[0], v[1]
            if abs(before_control) > after_control:
                decrease_cnt += 1
    print("+1", control_var, len(comparison), no_value, not_significant, decrease_cnt)

if __name__ == '__main__':
    # test_control_for_variables()
    # test_load_corrs()
    # test_find_correlations_with_control()
    # test_find_correlations_from()
    # find_join_method = FIND_JOIN_METHOD.JOIN_ALL
    # test_find_all_correlations_duckdb_all(find_join_method)
    # test_find_all_correlations_postgres_all(find_join_method)
    # test_add_data_sources()
    # test_ingest_data_source_with_multiple_spatial_hierarchies()
    test_control_vars_for_correlations()
