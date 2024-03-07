from data_ingestion.connection import ConnectionFactory
from data_search.search_corr import CorrSearch
from data_search.commons import FIND_JOIN_METHOD
import pandas as pd
from utils.time_point import TEMPORAL_GRANU
from utils.coordinate import SPATIAL_GRANU
from utils.io_utils import load_corrs_to_df, load_corrs_from_dir, dump_json
import os
import json
import utils.io_utils as io_utils
from utils.data_model import Variable
from typing import List
from sklearn import linear_model
from corr_analysis.factor_analysis.factor_analysis import factor_analysis, build_factor_clusters
import time
from data_ingestion.data_profiler import Profiler


class API:
    def __init__(self, connection_string, engine='duckdb',
                 data_sources=['chicago_zipcode', 'asthma', 'chicago_factors'], impute_options=[], correction=''):
        self.engine_type = engine
        self.db_engine = ConnectionFactory.create_connection(connection_string, engine)

        self.conn_str = connection_string
        # conn_copg2 = psycopg2.connect(self.conn_str)
        # self.cur = conn_copg2.cursor()
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

        self.display_attrs = [
            "table_id1",
            "table_name1",
            "agg_table1",
            "agg_attr1",
            "original_attr1_missing_ratio",
            "table_id2",
            "table_name2",
            "agg_table2",
            "agg_attr2",
            "original_attr2_missing_ratio",
            "correlation coefficient",
            "p value",
            "number of samples",
            "spatio-temporal key type",
        ]

    def find_correlations_from(self, dataset: str, temporal_granularity: TEMPORAL_GRANU,
                               spatial_granularity: SPATIAL_GRANU,
                               overlap_threshold: int, correlation_threshold: float, correlation_type="pearson",
                               control_variables=[]):
        corr_search = CorrSearch(
            self.conn_str,
            self.engine_type,
            self.data_sources,
            FIND_JOIN_METHOD.JOIN_ALL,
            impute_methods=self.impute_options,
            explicit_outer_join=False,
            correct_method=self.correction,
            q_val=0.05,
        )
        corr_search.set_join_cost(temporal_granularity, spatial_granularity, overlap_threshold)
        corr_search.find_all_corr_for_a_tbl(dataset, temporal_granularity, spatial_granularity, overlap_threshold,
                                            correlation_threshold, p_t=0.05, fill_zero=True,
                                            corr_type=correlation_type, control_variables=control_variables)
        correlations = load_corrs_to_df(corr_search.data)
        print(f"total number of correlations: {len(correlations)}")
        return correlations[self.display_attrs]

    def find_all_correlations(self, temporal_granularity, spatial_granularity, overlap_threshold,
                              correlation_threshold, persist_path=None, correlation_type="pearson",
                              control_variables=[]):
        corr_search = CorrSearch(
            self.conn_str,
            self.data_sources,
            FIND_JOIN_METHOD.COST_MODEL,
            impute_methods=self.impute_options,
            explicit_outer_join=False,
            correct_method='FDR',
            q_val=0.05,
        )
        corr_search.set_join_cost(temporal_granularity, spatial_granularity, overlap_threshold)
        start = time.time()
        corr_search.find_all_corr_for_all_tbls([temporal_granularity, spatial_granularity], overlap_threshold,
                                               correlation_threshold, p_t=0.05, corr_type=correlation_type,
                                               fill_zero=True, dir_path=persist_path, control_vars=control_variables)
        total_time = time.time() - start
        corr_search.perf_profile["total_time"] = total_time
        corr_search.perf_profile["cost_model_overhead"] = corr_search.overhead
        dump_json(
            f"tmp/perf_profile_{'_'.join(self.data_sources)}_{overlap_threshold}_{correlation_threshold}_{temporal_granularity}_{spatial_granularity}_{'_'.join([var.to_str() for var in control_variables])}.json",
            corr_search.perf_profile,
        )
        correlations = load_corrs_to_df(corr_search.all_corrs)
        print(f"total number of correlations: {len(correlations)}")
        return correlations[self.display_attrs]

    def regress(self, target_variable: Variable, co_variables: List[Variable], reg):
        df, _ = self.db_engine.join_multi_vars([target_variable] + co_variables)
        x = df[[var.attr_name for var in co_variables]]
        y = df[target_variable.attr_name]
        model = reg.fit(x, y)
        r_sq = model.score(x, y)
        return model, r_sq, df

    def join_and_project(self, variables: List[Variable], constraints={}):
        df = self.db_engine.join_multi_vars(variables, constraints=constraints)
        return df

    def get_joined_data_from_row(self, row):
        agg_name1 = row['agg_table1']
        agg_attr1 = row['agg_attr1']
        agg_name2 = row['agg_table2']
        agg_attr2 = row['agg_attr2']
        unagg_flag = False
        if agg_attr1[0:4] != 'avg_':
            agg_attr1 = 'avg_' + agg_attr1
            unagg_flag = True
        df, provenance = self.db_engine.join_two_tables_on_spatio_temporal_keys(
            agg_name1, [Variable(var_name=agg_attr1)],
            agg_name2, [Variable(var_name=agg_attr2)], use_outer=False)

        df[agg_attr1] = df[agg_attr1].astype(float)
        if unagg_flag:
            df = df.rename(columns={agg_attr1: agg_attr1[4:]})
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
        df = self.db_engine.read_agg_tbl(agg_tbl_name)
        return df

    def get_total_number_of_vars(self, t_granu, s_granu):
        total_num = 0
        st_schema_list = Profiler.load_all_spatio_temporal_keys(self.catalog, t_granu, s_granu)
        for tbl, st_schema in st_schema_list:
            agg_tbl = st_schema.get_agg_tbl_name(tbl)
            df = self.db_engine.read_agg_tbl(agg_tbl)
            # drop columns where all values are the same
            nunique = df.nunique()
            cols_to_drop = nunique[nunique == 1].index
            df = df.drop(cols_to_drop, axis=1)
            num_valid_columns = len(df.columns) - 1
            if num_valid_columns == 1 or tbl == '85ca-t3if':
                total_num += num_valid_columns
            else:
                total_num += num_valid_columns - 1
        return total_num

    @staticmethod
    def load_corrs_from_dir(corr_path):
        return load_corrs_from_dir(corr_path)

    @staticmethod
    def factor_analysis(corrs, corrs_map, n_factors=3, save_path=None):
        return factor_analysis(corrs, corrs_map, n_factors, save_path=save_path)

    @staticmethod
    def build_factor_clusters(fa, corrs, corr_map, n_factors, threshold=0.5):
        return build_factor_clusters(fa, corrs, corr_map, n_factors, threshold)


if __name__ == '__main__':
    conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
    nexus_api = API(conn_str)
    dataset = 'asthma'
    temporal_granu, spatial_granu = None, SPATIAL_GRANU.ZIPCODE
    overlap_t = 5
    r_t = 0.5

    # test regress
    target_var = Variable('asthma_Zip5_6', 'avg_enc_asthma')
    variables = [Variable('ijzp-q8t2_location_6', 'count'), Variable('n26f-ihde_pickup_centroid_location_6', 'avg_tip')]
    reg_model = linear_model.LinearRegression()  # OLS regression
    model, rsq, merged = nexus_api.regress(target_var, variables, reg_model)
    print(model.coef_)
    print(rsq)
