import pandas as pd
import utils.coordinate as coordinate
from utils.time_point import set_temporal_granu, parse_datetime, TEMPORAL_GRANU
from utils.profile_utils import is_num_column_valid
import geopandas as gpd
import os
import utils.io_utils as io_utils
from utils.coordinate import resolve_spatial_hierarchy, set_spatial_granu, SPATIAL_GRANU
import numpy as np
from sqlalchemy.types import *
import time
from tqdm import tqdm
from utils.data_model import (
    SpatioTemporalKey, Attr, Table,
)
import data_ingestion.db_ops as db_ops
from typing import List
from utils.correlation_sketch_utils import murmur3_32, grm, FixedSizeMaxHeap
import traceback
from data_ingestion.data_profiler import Profiler
from data_ingestion.connection import ConnectionFactory
"""
DBIngestor ingests dataframes to a database (current implementation uses postgres)
Here are the procedure to ingest a spatial-temporal table
1. Read the table as a dataframe
2. Expand the dataframe by resolving different resolutions for temporal and spatial attributes
3. Aggregate the dataframe to different spatio-temporal scales
4. Ingest the aggregated dataframes to the database
5. Create indices on the aggregated tables
"""


class DBIngestor:
    def __init__(self, conn_string: str, engine='postgres', mode='no_cross') -> None:
        self.engine_type = engine
        self.db_engine = ConnectionFactory.create_connection(conn_string, engine)
        self.mode = mode
        self.sketch = False

    @staticmethod
    def get_numerical_columns(all_columns, tbl: Table):
        numerical_columns = list(set(all_columns) & set(tbl.num_columns))
        # numerical_columns = tbl.num_columns
        valid_num_columns = []
        # exclude columns that contain stop words and timestamp columns
        for col in numerical_columns:
            if is_num_column_valid(col) and col not in tbl.temporal_attrs:
                valid_num_columns.append(col)
        return valid_num_columns

    @staticmethod
    def select_valid_attrs(attrs: List[Attr], max_limit=0):
        valid_attrs = []
        for attr in attrs:
            if len(attr.name) > 48:
                continue
            if any(
                keyword in attr.name
                for keyword in ["update", "modified", "_end", "end_", "status", "_notified"]
            ):
                continue
            valid_attrs.append(attr)
        # limit the max number of spatio/temporal join keys a table can have
        valid_attrs.sort(key=lambda x: len(x.name))
        return valid_attrs[:max_limit]

    def ingest_data_source(self, data_source: str,
                           temporal_granu_l: List[TEMPORAL_GRANU], spatial_granu_l: List[SPATIAL_GRANU],
                           temporal_range=None, spatial_range=None,
                           clean=False, persist=False, max_limit=2, retry_list=None):
        # successfully ingested table information
        ingested_tables = {}
        # failed tables
        failed_tables = []
        # idx tables that are created successfully
        inverted_index_tables = []

        data_source_config = io_utils.load_config(data_source)

        geo_chain = data_source_config["geo_chain"]
        geo_keys = data_source_config["geo_keys"]
        coordinate.resolve_geo_chain(geo_chain, geo_keys)

        catalog_path = data_source_config["meta_path"]
        data_catalog = io_utils.load_json(catalog_path)
        if retry_list is not None:
            previous_failed_tbls = io_utils.load_json(data_source_config['failed_tbl_path'])
        if clean:
            self.delete_all_aggregated_tbls_and_inv_indices(temporal_granu_l, spatial_granu_l)

        if self.engine_type == 'postgres':
            inverted_index_tables = self.create_inverted_index_tables(temporal_granu_l, spatial_granu_l)

        for _, obj in tqdm(data_catalog.items()):
            t_attrs = [Attr(attr["name"], attr["granu"]) for attr in obj["t_attrs"]]
            s_attrs = [Attr(attr["name"], attr["granu"]) for attr in obj["s_attrs"]]
            t_attrs, s_attrs = self.select_valid_attrs(t_attrs, max_limit), self.select_valid_attrs(s_attrs, max_limit)
            if len(t_attrs) == 0 and len(s_attrs) == 0:
                continue
           
            tbl = Table(
                domain=obj["domain"],
                tbl_id=obj["tbl_id"],
                tbl_name=obj["tbl_name"],
                temporal_attrs=t_attrs,
                spatial_attrs=s_attrs,
                num_columns=obj["num_columns"],
                link=obj["link"] if "link" in obj else "",
            )
           
            print(tbl.tbl_id)
            if retry_list is not None:
                if tbl.tbl_id not in retry_list:
                    continue
                try:
                    tbl_info = self.ingest_tbl(tbl, temporal_granu_l, spatial_granu_l,
                                    spatial_range, temporal_range,
                                    data_source_config, inverted_index_tables)
                    self.create_cnt_tbl(tbl, temporal_granu_l, spatial_granu_l)
                    previous_failed_tbls.remove(tbl.tbl_id)
                except Exception as e:
                    traceback.print_exc()
            else:
                try:
                    tbl_info = self.ingest_tbl(tbl, temporal_granu_l, spatial_granu_l,
                                    spatial_range, temporal_range,
                                    data_source_config, inverted_index_tables)
                    self.create_cnt_tbl(tbl, temporal_granu_l, spatial_granu_l)
                except Exception as e:
                    failed_tables.append(tbl.tbl_id)
                    traceback.print_exc()
            ingested_tables[tbl.tbl_id] = tbl_info

        if persist:
            if retry_list is not None:
                previous_ingested_tables = io_utils.load_json(data_source_config['attr_path'])
                previous_ingested_tables.update(ingested_tables)
                io_utils.dump_json(data_source_config['attr_path'], previous_ingested_tables)
                io_utils.dump_json(data_source_config['failed_tbl_path'], previous_failed_tbls)
            else:
                io_utils.dump_json(
                    data_source_config["attr_path"],
                    ingested_tables,
                )
                io_utils.dump_json(
                    data_source_config["failed_tbl_path"],
                    failed_tables
                )
                # io_utils.dump_json(data_source_config["idx_tbl_path"], list(self.idx_tables))

        # create dataset profiles
        profiler = Profiler(db_engine=self.db_engine, data_source=data_source, mode=self.mode)
        print("begin collecting agg stats")
        profiler.collect_agg_tbl_col_stats(temporal_granu_l, spatial_granu_l)
        print("begin profiling original data")
        profiler.profile_original_data()

    def create_cnt_tbl(self, tbl: Table,
                       temporal_granu_l: List[TEMPORAL_GRANU], spatial_granu_l: List[SPATIAL_GRANU]):
        spatio_temporal_keys = tbl.get_spatio_temporal_keys(temporal_granu_l, spatial_granu_l, mode=self.mode)
        for spatio_temporal_key in spatio_temporal_keys:
            self.db_engine.create_cnt_tbl_for_agg_tbl(tbl.tbl_id, spatio_temporal_key)

    def create_cnt_tbls_for_inv_index_tbls(self, inverted_indices):
        if self.engine_type == 'postgres':
            self.db_engine.create_cnt_tbl_for_inverted_indices(inverted_indices)
        else:
            raise Exception("Not implemented")

    def ingest_tbl(self, tbl: Table,
                   temporal_granu_l: List[TEMPORAL_GRANU],
                   spatial_granu_l: List[SPATIAL_GRANU],
                   temporal_range=None, spatial_range=None,
                   data_source_config=None, created_inverted_index=[]):
        if len(tbl.temporal_attrs) == 0 and len(tbl.spatial_attrs) == 0:
            print("not a valid spatio-temporal table")
            return
        if len(coordinate.supported_chain) == 0:
            geo_chain = data_source_config["geo_chain"]
            geo_keys = data_source_config["geo_keys"]
            coordinate.resolve_geo_chain(geo_chain, geo_keys)
        tbl_path = os.path.join(data_source_config['data_path'], f"{tbl.tbl_id}.csv")
        print("reading csv")
        df = io_utils.read_csv(tbl_path)

        # get numerical columns
        all_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        numerical_columns = self.get_numerical_columns(all_columns, tbl)
        numerical_columns = [x for x in numerical_columns if len(x) <= 56]
        tbl.num_columns = numerical_columns

        t_attr_names = [attr.name for attr in tbl.temporal_attrs]
        s_attr_names = [attr.name for attr in tbl.spatial_attrs]
        df = df[t_attr_names + s_attr_names + numerical_columns]

        print("begin expanding dataframe")
        # expand dataframe
        start = time.time()
        df, df_schema, t_attrs_success, s_attrs_success = self.expand_df(
            df, tbl.temporal_attrs, tbl.spatial_attrs,
            temporal_granu_l, spatial_granu_l, temporal_range, spatial_range,
            data_source_config
        )
        tbl.temporal_attrs = t_attrs_success
        tbl.spatial_attrs = s_attrs_success
        print("expanding table used {} s".format(time.time() - start))

        # if dataframe is None, return
        if df is None:
            print("df is none")
            return

        print("begin ingesting")
        start = time.time()
        self.db_engine.create_tbl(tbl.tbl_id, df, mode="replace")
        print("ingesting table used {} s".format(time.time() - start))

        print("begin creating agg_tbl")
        start = time.time()
        self.spatio_temporal_aggregations(tbl, temporal_granu_l, spatial_granu_l, created_inverted_index)
        print("creating agg_tbl used {}".format(time.time() - start))

        print("begin deleting the original table")
        # delete original table
        start = time.time()
        self.db_engine.delete_tbl(tbl.tbl_id)
        print("deleting original table used {}".format(time.time() - start))

        # return the information of this table
        return {
            "domain": tbl.domain,
            "name": tbl.tbl_name,
            "t_attrs": [t_attr.__dict__ for t_attr in t_attrs_success],
            "s_attrs": [s_attr.__dict__ for s_attr in s_attrs_success],
            "num_columns": numerical_columns,
            "link": tbl.link
        }

    def spatio_temporal_aggregations(self, tbl: Table,
                                     temporal_granu_l: List[TEMPORAL_GRANU], spatial_granu_l: List[SPATIAL_GRANU],
                                     create_inverted_index_tables):
        spatio_temporal_keys = tbl.get_spatio_temporal_keys(temporal_granu_l, spatial_granu_l, mode=self.mode)
        variables = tbl.get_variables()

        for spatio_temporal_key in spatio_temporal_keys:
            start = time.time()
            # transform data and also create an index on the key val column
            agg_tbl_name = self.db_engine.create_aggregate_tbl(tbl.tbl_id, spatio_temporal_key, variables)
            print(f"finish aggregating {agg_tbl_name} in {time.time()-start} s")
            # ingest spatio-temporal values to an index table
            if self.engine_type == 'postgres':
                start = time.time()
                self.insert_spatio_temporal_key_to_inv_idx(tbl.tbl_id, spatio_temporal_key, create_inverted_index_tables)
                print(f"finish ingesting to idx table in {time.time()-start} s")
            if self.sketch:
                # create correlation sketch for an aggregation table.
                self.create_correlation_sketch(agg_tbl_name)
    
    def create_correlation_sketch(self, agg_tbl: str, k: int):
        # read the key column from agg_tbl
        keys = db_ops.read_key(self.cur, agg_tbl)
        sketch = FixedSizeMaxHeap(k) # consists of k min values
        for key in keys:
            # hash each key using murmur3
            hash_val = murmur3_32(key)
            # use another function to map hash_val to 0-1
            hu = grm(hash_val)
            sketch.push((hu, key))
        min_keys = [item[1] for item in sketch.get_data()]
        # project these values from the original table
        db_ops.create_correlation_sketch_tbl(self.cur, agg_tbl, k, min_keys)

    def create_inverted_index_tables(self, temporal_granu_l: List[TEMPORAL_GRANU], spatial_granu_l: List[SPATIAL_GRANU]):
        inverted_index_names = []
        for temporal_granu in temporal_granu_l:
            inv_idx = f"time_{temporal_granu.value}_inv"
            self.db_engine.create_inv_index_tbl(inv_idx)
            inverted_index_names.append(inv_idx)
        for spatial_granu in spatial_granu_l:
            inv_idx = f"space_{spatial_granu.value}_inv"
            self.db_engine.create_inv_index_tbl(inv_idx)
            inverted_index_names.append(inv_idx)
        for temporal_granu in temporal_granu_l:
            for spatial_granu in spatial_granu_l:
                inv_idx = f"time_{temporal_granu.value}_space_{spatial_granu.value}_inv"
                self.db_engine.create_inv_index_tbl(inv_idx)
                inverted_index_names.append(inv_idx)
        return inverted_index_names

    def insert_spatio_temporal_key_to_inv_idx(self, tbl_id: str, spatio_temporal_key: SpatioTemporalKey,
                                              created_inverted_index_tables):
        # decide which index table to ingest the agg_tbl values
        inv_idx = spatio_temporal_key.get_idx_tbl_name() + "_inv"
        if inv_idx not in created_inverted_index_tables:
            self.db_engine.create_inv_index_tbl(inv_idx)
        self.db_engine.insert_spatio_temporal_key_to_inv_idx(inv_idx, tbl_id, spatio_temporal_key)

    def delete_all_aggregated_tbls_and_inv_indices(self, temporal_granu_l: List[TEMPORAL_GRANU],
                                                   spatial_granu_l: List[SPATIAL_GRANU]):
        # delete aggregated tables and inverted indices that are already in the database
        for t_granu in temporal_granu_l:
            self.db_engine.delete_tbl("time_{}".format(t_granu.value))
            self.db_engine.delete_tbl("time_{}_inv".format(t_granu.value))

        for s_granu in spatial_granu_l:
            self.db_engine.delete_tbl("space_{}".format(s_granu.value))
            self.db_engine.delete_tbl("space_{}_inv".format(s_granu.value))

        for t_granu in temporal_granu_l:
            for s_granu in spatial_granu_l:
                self.db_engine.delete_tbl("time_{}_space_{}".format(t_granu.value, s_granu.value))
                self.db_engine.delete_tbl("time_{}_space_{}_inv".format(t_granu.value, s_granu.value))

    def expand_df(self, df, t_attrs: List[Attr], s_attrs: List[Attr],
                  temporal_granu_l: List[TEMPORAL_GRANU], spatial_granu_l: List[SPATIAL_GRANU],
                  temporal_range=None, spatial_range=None, data_source_config=None):
        t_attrs_success = []
        s_attrs_success = []
        df_schema = {}
        for t_attr in t_attrs:
            if len(temporal_granu_l) == 0:
                break
            # parse datetime column to datetime class
            df[t_attr.name] = pd.to_datetime(df[t_attr.name], utc=False, errors="coerce").replace(
                {np.NaN: None}
            )
            if temporal_range:
                # if temporal_range is specified, we only ingest data within a certain range
              
                is_datetime64_utc = df[t_attr.name].dtype == "datetime64[ns, UTC]"
                try:
                    if is_datetime64_utc:
                        df = df.loc[(df[t_attr.name] >= pd.to_datetime(temporal_range[0], utc=True)) & (df[t_attr.name] < pd.to_datetime(temporal_range[1], utc=True))]
                    else:   
                        df = df.loc[(df[t_attr.name] >= temporal_range[0]) & (df[t_attr.name] < temporal_range[1])]
                except TypeError:
                    df = df.loc[(df[t_attr.name] >= pd.to_datetime(temporal_range[0], utc=True)) & (df[t_attr.name] < pd.to_datetime(temporal_range[1], utc=True))]
             
                if len(df) == 0:
                    return None, None, [], []
            df_dts = df[t_attr.name].apply(parse_datetime).dropna()
            if len(df_dts):
                for t_granu in temporal_granu_l:
                    new_attr = "{}_{}".format(t_attr.name, t_granu.value)
                    df[new_attr] = df_dts.apply(set_temporal_granu, args=(t_granu,))
                    df_schema[new_attr] = Integer()
                t_attrs_success.append(t_attr)
        for s_attr in s_attrs:
            if len(spatial_granu_l) == 0:
                break
            if s_attr.granu == 'POINT':
                # parse (long, lat) pairs to point
                df_points = df[s_attr.name].apply(coordinate.parse_coordinate)

                # create a geopandas dataframe using points
                gdf = (
                    gpd.GeoDataFrame(geometry=df_points)
                    .dropna()
                    .set_crs(epsg=4326, inplace=True)
                )

                df_resolved = resolve_spatial_hierarchy(data_source_config['shape_path'], gdf)

                # df_resolved can be none meaning there is no point falling into the shape file
                if df_resolved is None:
                    continue
                for s_granu in spatial_granu_l:
                    new_attr = "{}_{}".format(s_attr.name, s_granu.value)
                    df[new_attr] = df_resolved.apply(set_spatial_granu, args=(s_granu,))
            else:
                for s_granu in spatial_granu_l:
                    if s_granu.name == s_attr.granu:
                        new_attr = "{}_{}".format(s_attr.name, s_granu.value)
                        df[new_attr] = df[s_attr.name].astype(int).astype(str)
            s_attrs_success.append(s_attr)
        return df, df_schema, t_attrs_success, s_attrs_success


if __name__ == "__main__":
    # ingest asthma dataset
    data_sources = ['chicago_factors']
    # conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
    # conn_str = "postgresql://yuegong@localhost/chicago_1m_new"
    conn_str = "postgresql://yuegong@localhost/test"
    temporal_granu_l = []
    spatial_granu_l = [SPATIAL_GRANU.TRACT, SPATIAL_GRANU.ZIPCODE]
    ingestor = DBIngestor(conn_str, engine='postgres')
    # ingest tables
    for data_source in data_sources:
        print(data_source)
        start_time = time.time()
        ingestor.ingest_data_source(data_source, temporal_granu_l, spatial_granu_l,
                                    clean=False, persist=True, max_limit=1)
        print(f"ingesting data finished in {time.time() - start_time} s")

    # create count tables for inverted index tables
    inverted_index_tables = ["space_3_inv", "space_6_inv"]
    ingestor.create_cnt_tbls_for_inv_index_tbls(inverted_index_tables)

    # data_sources = ['asthma']
    # temporal_granu_l, spatial_granu_l = [], [SPATIAL_GRANU.ZIPCODE]
    # conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
    # # create profiles
    # for data_source in data_sources:
    #     profiler = Profiler(data_source, temporal_granu_l, spatial_granu_l, conn_str)
    #     profiler.set_mode('no_cross')
    #     print("begin collecting agg stats")
    #     profiler.collect_agg_tbl_col_stats()
    #     print("begin profiling original data")
    #     profiler.profile_original_data()
    
    # start_time = time.time()
    # t_scales = [T_GRANU.DAY, T_GRANU.MONTH]
    # s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]
    # data_source = "chicago_10k"
    # config = io_utils.load_config(data_source)
    # conn_string = config["db_path"]
    # ingestor = DBIngestorAgg(conn_string, data_source, t_scales, s_scales)
    # # tbl = Table(
    # #     domain="",
    # #     tbl_id="qqqh-hgyw",
    # #     tbl_name="",
    # #     t_attrs=[
    # #         "lse_report_reviewed_on_date",
    # #         # "scheduled_inspection_date",
    # #         # "rescheduled_inspection_date",
    # #     ],
    # #     s_attrs=[],
    # #     num_columns=[],
    # # )
    # # ingestor.ingest_tbl(tbl)
    # ingestor.ingest_data_source(clean=True, persist=True)