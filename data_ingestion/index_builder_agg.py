import pandas as pd
import utils.coordinate as coordinate
from utils.time_point import set_temporal_granu, parse_datetime
from utils.profile_utils import is_num_column_valid
import geopandas as gpd
import os
import utils.io_utils as io_utils
from sqlalchemy import create_engine
from utils.coordinate import resolve_spatial_hierarchy, set_spatial_granu, SPATIAL_GRANU
import psycopg2
from data_search.search_db import DBSearch
import numpy as np
from sqlalchemy.types import *
from data_ingestion.profile_datasets import Profiler
import time
from tqdm import tqdm
from utils.data_model import (
    Attr,
    Variable,
    AggFunc,
    SpatioTemporalKey, Attr, Table,
)
import data_ingestion.db_ops as db_ops
from data_ingestion.db_ops import IndexType
from typing import List
from utils.correlation_sketch_utils import murmur3_32, grm, FixedSizeMaxHeap
import traceback
from data_ingestion.postgres_connector import PostgresConnector
from data_ingestion.duckdb_connector import DuckDBConnector

"""
DBIngestor ingests dataframes to a database (current implementation uses postgres)
Here are the procedure to ingest a spatial-temporal table
1. Read the table as a dataframe
2. Expand the dataframe by resolving different resolutions for temporal and spatial attributes
3. Aggregate the dataframe to different spatio-temporal scales
4. Ingest the aggregated dataframes to the database
5. Create indices on the aggregated tables
"""


class DBIngestorAgg:
    def __init__(self, conn_string: str, source, t_scales, s_scales, mode='no_cross', engine='postgres') -> None:
        if engine == 'postgres':
            db = create_engine(conn_string)
            self.conn = db.connect()
            conn_copg2 = psycopg2.connect(conn_string)
            conn_copg2.autocommit = True
            self.cur = conn_copg2.cursor()
            self.db_engine = PostgresConnector(conn_string)
            self.db_search = DBSearch(conn_string)
        elif engine == 'duckdb':
            self.db_engine = DuckDBConnector(conn_string)

       
        # tables that have been created
        self.created_tbls = set()
        # successfully ingested table information
        self.tbls = {}
        self.failed_tbls = []
        # idx tables that are created successfully
        self.idx_tables = set()

        self.t_scales = t_scales
        self.s_scales = s_scales

        self.config = io_utils.load_config(source)
        self.data_path = self.config["data_path"]
        self.shape_path = self.config["shape_path"]
        self.t_range = None
        self.mode = mode
        self.sketch = False

        geo_chain = self.config["geo_chain"]
        geo_keys = self.config["geo_keys"]
        coordinate.resolve_geo_chain(geo_chain, geo_keys)

    def set_time_range(self, t_range):
        self.t_range = t_range

    @staticmethod
    def get_numerical_columns(all_columns, tbl: Table):
        numerical_columns = list(set(all_columns) & set(tbl.num_columns))
        # numerical_columns = tbl.num_columns
        valid_num_columns = []
        # exclude columns that contain stop words and timestamp columns
        for col in numerical_columns:
            if is_num_column_valid(col) and col not in tbl.t_attrs:
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

    def ingest_data_source(self, clean=False, persist=False, max_limit=2, retry_list=None):
        meta_path = self.config["meta_path"]
        meta_data = io_utils.load_json(meta_path)
        if retry_list is not None:
            previous_failed_tbls = io_utils.load_json(self.config['failed_tbl_path'])
        if clean:
            self.clean_aggregated_idx_tbls()

        for _, obj in tqdm(meta_data.items()):
            t_attrs = [Attr(attr["name"], attr["granu"]) for attr in obj["t_attrs"]]
            s_attrs = [Attr(attr["name"], attr["granu"]) for attr in obj["s_attrs"]]
            t_attrs, s_attrs = self.select_valid_attrs(t_attrs, max_limit), self.select_valid_attrs(s_attrs, max_limit)
            # print(t_attrs, s_attrs)
            if len(t_attrs) == 0 and len(s_attrs) == 0:
                continue
           
            tbl = Table(
                domain=obj["domain"],
                tbl_id=obj["tbl_id"],
                tbl_name=obj["tbl_name"],
                t_attrs=t_attrs,
                s_attrs=s_attrs,
                num_columns=obj["num_columns"],
                link=obj["link"] if "link" in obj else "",
            )
           
            print(tbl.tbl_id)
            if retry_list is not None:
                if tbl.tbl_id not in retry_list:
                    continue
                try:
                    self.ingest_tbl(tbl)
                    # previous_failed_tbls.remove(tbl.tbl_id)
                except Exception as e:
                    print("failed")
                    traceback.print_exc()
                # if tbl.tbl_id == '3gj5-t7ah':
                #     self.ingest_tbl(tbl)
            else:
                try:
                    self.ingest_tbl(tbl)
                except Exception as e:
                    print("failed")
                    self.failed_tbls.append(tbl.tbl_id)
                    traceback.print_exc()
        # create a cnt table for each individual table
        # for tbl_id, info in self.tbls.items():
        #     try:
        #         self.create_cnt_tbl(
        #             tbl_id,
        #             info["t_attrs"],
        #             info["s_attrs"],
        #             self.t_scales,
        #             self.s_scales,
        #         )
        #     except Exception as e:
        #         print("failed creating cnt tbl")
        #         self.failed_tbls.append(f"{tbl_id}_cnt")
        #         traceback.print_exc() 

        # for idx_tbl in tqdm(self.idx_tables):
        #     db_ops.create_indices_on_tbl(
        #         self.cur, f"{idx_tbl}_inv", idx_tbl, ["val"], mode=IndexType.HASH
        #     )
        # # create a cnt table for every invereted index
        # self.create_inv_cnt_tbls(self.idx_tables)

        if persist:
            if retry_list is not None:
                ingested_tbls = io_utils.load_json(self.config['attr_path'])
                ingested_tbls.update(self.tbls)
                io_utils.dump_json(self.config['attr_path'], ingested_tbls)
                io_utils.dump_json(self.config['failed_tbl_path'], previous_failed_tbls)
            else:
                io_utils.dump_json(
                    self.config["attr_path"],
                    self.tbls,
                )
                io_utils.dump_json(
                    self.config["failed_tbl_path"],
                    self.failed_tbls
                )
                io_utils.dump_json(self.config["idx_tbl_path"], list(self.idx_tables))

    @staticmethod
    def create_cnt_tbls_for_inv_index_tbls(conn_str, idx_tbls):
        conn_copg2 = psycopg2.connect(conn_str)
        conn_copg2.autocommit = True
        cur = conn_copg2.cursor()
        # for idx_tbl in tqdm(idx_tbls):
        #     print(idx_tbl)
        #     db_ops.create_indices_on_tbl(
        #         cur, f"{idx_tbl}_inv", idx_tbl, ["val"], mode=IndexType.HASH
        #     )
        # create a cnt table for every invereted index
        db_ops.create_inv_idx_cnt_tbl(cur, idx_tbls)

    def create_inv_cnt_tbls(self, idx_tbls):
        db_ops.create_inv_idx_cnt_tbl(self.cur, idx_tbls)

    def create_cnt_tbl(self, tbl, t_attrs, s_attrs, t_scales, s_scales):
        st_schema_list = []
        for t in t_attrs:
            for t_scale in t_scales:
                st_schema_list.append((tbl, SpatioTemporalKey(temporal_attr=Attr(t["name"], t_scale))))

        for s in s_attrs:
            for s_scale in s_scales:
                if s["granu"] != 'POINT' and s["granu"] != s_scale.name:
                    continue
                st_schema_list.append((tbl, SpatioTemporalKey(spatial_attr=Attr(s["name"], s_scale))))

        for t in t_attrs:
            for s in s_attrs:
                if self.mode == 'cross':
                    for t_scale in t_scales:
                        for s_scale in s_scales:
                            if s["granu"] != 'POINT' and s["granu"] != s_scale.name:
                                continue
                            st_schema_list.append(
                                (tbl, SpatioTemporalKey(Attr(t["name"], t_scale), Attr(s["name"], s_scale)))
                            )
                elif self.mode == 'no_cross':
                    for i in range(len(t_scales)):
                        if s.granu != 'POINT' and s["granu"] != s_scales[i].name:
                            continue
                        st_schema_list.append(
                            (tbl, SpatioTemporalKey(Attr(t["name"], t_scales[i]), Attr(s["name"], s_scales[i])))
                        )
        for schema in st_schema_list:
            db_ops.create_cnt_tbl_for_agg_tbl(self.cur, schema[0], schema[1])

    def ingest_tbl(self, tbl: Table):
        tbl_path = os.path.join(self.data_path, f"{tbl.tbl_id}.csv")
        print("reading csv")
        df = io_utils.read_csv(tbl_path)

        # get numerical columns
        all_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        numerical_columns = self.get_numerical_columns(all_columns, tbl)
        numerical_columns = [x for x in numerical_columns if len(x) <= 56]
        
        t_attr_names = [attr.name for attr in tbl.t_attrs]
        s_attr_names = [attr.name for attr in tbl.s_attrs]
        df = df[t_attr_names + s_attr_names + numerical_columns]

        print("begin expanding dataframe")
        # expand dataframe
        start = time.time()
        df, df_schema, t_attrs_success, s_attrs_success = self.expand_df(
            df, tbl.t_attrs, tbl.s_attrs, self.t_range
        )

        print("expanding table used {} s".format(time.time() - start))
        # if dataframe is None, return
        if df is None:
            print("df is none")
            return
        self.tbls[tbl.tbl_id] = {
            "domain": tbl.domain,
            "name": tbl.tbl_name,
            "t_attrs": [t_attr.__dict__ for t_attr in t_attrs_success],
            "s_attrs": [s_attr.__dict__ for s_attr in s_attrs_success],
            "num_columns": numerical_columns,
            "link": tbl.link
        }
        print("begin ingesting")
        start = time.time()
        # ingest dataframe to database
        # df = df.replace({np.NaN: None})  # substitue NaT to None to avoid psycopg error
        self.ingest_df_to_db(df, tbl.tbl_id, mode="replace")
        print("ingesting table used {} s".format(time.time() - start))

        return 
        print("begin creating agg_tbl")
        start = time.time()
        # create aggregated tables
        self.create_agg_tbl(
            tbl.tbl_id,
            t_attrs_success,
            s_attrs_success,
            self.t_scales,
            self.s_scales,
            numerical_columns,
        )
        print("creating agg_tbl used {}".format(time.time() - start))

        print("begin deleting the original table")
        # delete original table
        start = time.time()
        db_ops.del_tbl(self.cur, tbl.tbl_id)
        print("deleting original table used {}".format(time.time() - start))

    def create_agg_tbl(self, tbl, t_attrs, s_attrs, t_scales, s_scales, num_columns):
        st_schema_list = []
        for t in t_attrs:
            for scale in t_scales:
                st_schema_list.append(SpatioTemporalKey(temporal_attr=Attr(t.name, scale)))

        for s in s_attrs:
            for scale in s_scales:
                if s.granu != 'POINT' and s.granu != scale.name:
                    continue
                st_schema_list.append(SpatioTemporalKey(spatial_attr=Attr(s.name, scale)))

        for t in t_attrs:
            for s in s_attrs:
                if self.mode == 'no_cross':
                    for i in range(len(t_scales)):
                        if s.granu != 'POINT' and s.granu != s_scales[i].name:
                            continue
                        st_schema_list.append(
                            SpatioTemporalKey(Attr(t.name, t_scales[i]), Attr(s.name, s_scales[i]))
                        )
                elif self.mode == 'cross':
                    for t_scale in t_scales:
                        for s_scale in s_scales:
                            if s.granu != 'POINT' and s.granu != s_scale.name:
                                continue
                            st_schema_list.append(
                                SpatioTemporalKey(Attr(t.name, t_scale), Attr(s.name, s_scale))
                            )

        for st_schema in st_schema_list:
            variables = []
            for agg_col in num_columns:
                if len(agg_col) > 56:
                    continue
                variables.append(Variable(tbl, agg_col, AggFunc.AVG, "avg_{}".format(agg_col)))
            variables.append(Variable(tbl, "*", AggFunc.COUNT, "count"))

            start = time.time()
            # transform data and also create an index on the key val column
            agg_tbl_name = db_ops.create_agg_tbl(self.cur, tbl, st_schema, variables)
            print(agg_tbl_name)
            print(f"finish aggregating table in {time.time()-start} s")
            # ingest spatio-temporal values to an index table
            start = time.time()
            self.ingest_agg_tbl_to_idx_tbl(tbl, agg_tbl_name, st_schema)
            print(f"finish ingesting to idx table in {time.time()-start} s")
            if self.sketch == True:
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

    def create_idx_on_agg_tbls(
        self,
        tbl: Table,
        t_attrs,
        s_attrs,
        t_scales,
        s_scales,
    ):
        st_schema_list = []
        for t in t_attrs:
            for scale in t_scales:
                st_schema_list.append(SpatioTemporalKey(temporal_attr=Attr(t, scale)))

        for s in s_attrs:
            for scale in s_scales:
                st_schema_list.append(SpatioTemporalKey(spatial_attr=Attr(s, scale)))

        for t in t_attrs:
            for s in s_attrs:
                for t_scale in t_scales:
                    for s_scale in s_scales:
                        st_schema_list.append(
                            SpatioTemporalKey(Attr(t, t_scale), Attr(s, s_scale))
                        )

        for st_schema in st_schema_list:
            col_names = st_schema.get_col_names_with_granu()
            agg_tbl_name = st_schema.get_agg_tbl_name(tbl)
            if len(col_names) == 1:
                db_ops.create_indices_on_tbl(
                    self.cur,
                    agg_tbl_name + "_idx",
                    agg_tbl_name,
                    col_names,
                    IndexType.HASH,
                )
            elif len(col_names) == 2:
                db_ops.create_indices_on_tbl(
                    self.cur,
                    agg_tbl_name + "_idx",
                    agg_tbl_name,
                    col_names,
                )

    def ingest_agg_tbl_to_idx_tbl(self, tbl, agg_tbl, st_schema: SpatioTemporalKey):
        # decide which index table to ingest the agg_tbl values
        idx_tbl_name = st_schema.get_idx_tbl_name() + "_inv"
        print(idx_tbl_name)
        # if this idx table has not been created yet, create it first
        if idx_tbl_name not in self.idx_tables:
            db_ops.create_idx_tbl(self.cur, idx_tbl_name)
        db_ops.insert_to_idx_tbl(self.cur, idx_tbl_name, st_schema.get_id(tbl), agg_tbl)
        self.idx_tables.add(idx_tbl_name)

    def clean_aggregated_idx_tbls(self):
        # delete aggregated indices that are already in the database
        for t_granu in self.t_scales:
            db_ops.del_tbl(self.cur, "time_{}".format(t_granu.value))
            db_ops.del_tbl(self.cur, "time_{}_inv".format(t_granu.value))

        for s_granu in self.s_scales:
            db_ops.del_tbl(self.cur, "space_{}".format(s_granu.value))
            db_ops.del_tbl(self.cur, "space_{}_inv".format(s_granu.value))

        for t_granu in self.t_scales:
            for s_granu in self.s_scales:
                db_ops.del_tbl(
                    self.cur, "time_{}_space_{}".format(t_granu.value, s_granu.value)
                )
                db_ops.del_tbl(
                    self.cur,
                    "time_{}_space_{}_inv".format(t_granu.value, s_granu.value),
                )

    def expand_df(self, df, t_attrs: List[Attr], s_attrs: List[Attr], temporal_range=False, spatial_range=False):
        t_attrs_success = []
        s_attrs_success = []
        df_schema = {}
        for t_attr in t_attrs:
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
            # df_dts = np.vectorize(parse_datetime)(df[t_attr])
            if len(df_dts):
                for t_granu in self.t_scales:
                    new_attr = "{}_{}".format(t_attr.name, t_granu.value)
                    df[new_attr] = df_dts.apply(set_temporal_granu, args=(t_granu,))

                    # df[new_attr] = np.vectorize(set_temporal_granu, otypes=[object])(
                    #     df_dts, t_granu.value
                    # )

                    df_schema[new_attr] = Integer()
                t_attrs_success.append(t_attr)
        for s_attr in s_attrs:
            if s_attr.granu == 'Point':
                # parse (long, lat) pairs to point
                df_points = df[s_attr.name].apply(coordinate.parse_coordinate)

                # create a geopandas dataframe using points
                gdf = (
                    gpd.GeoDataFrame(geometry=df_points)
                    .dropna()
                    .set_crs(epsg=4326, inplace=True)
                )

                df_resolved = resolve_spatial_hierarchy(self.shape_path, gdf)

                # df_resolved can be none meaning there is no point falling into the shape file
                if df_resolved is None:
                    continue
                for s_granu in self.s_scales:
                    new_attr = "{}_{}".format(s_attr.name, s_granu.value)
                    df[new_attr] = df_resolved.apply(set_spatial_granu, args=(s_granu,))
                    df_schema[new_attr] = Integer()
            else:
                for s_granu in self.s_scales:
                    if s_granu.name == s_attr.granu:
                        new_attr = "{}_{}".format(s_attr.name, s_granu.value)
                        df[new_attr] = df[s_attr.name]
            s_attrs_success.append(s_attr)
        return df, df_schema, t_attrs_success, s_attrs_success

    def create_tbl(self, tbl_name, df):
        df_columns = df.iloc[:0]

        df_columns.to_sql(
            tbl_name,
            con=self.conn,
            if_exists="replace",
            index=False,
        )

        self.created_tbls.add(tbl_name)

    def ingest_df_to_db(self, df, tbl_name, mode="replace"):
        self.db_engine.create_tbl(tbl_name, df, mode)
        # if mode == "replace":
        #     # drop old tables before ingesting the new dataframe
        #     self.create_tbl(tbl_name, df)
        #     db_ops.copy_from_dataFile_StringIO(self.cur, df, tbl_name)

        # elif mode == "append":
        #     if tbl_name not in self.created_tbls:
        #         self.create_tbl(tbl_name, df)
        #     db_ops.copy_from_dataFile_StringIO(self.cur, df, tbl_name)


if __name__ == "__main__":
    # ingest asthma dataset
    # data_sources = ['chicago_factors']
    # # conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
    # conn_str = "postgresql://yuegong@localhost/chicago_1m_new"
    # t_scales = []
    # s_scales = [S_GRANU.TRACT, S_GRANU.ZIPCODE]

    # # ingest tables
    # for data_source in data_sources:
    #     print(data_source)
    #     start_time = time.time()
    #     ingestor = DBIngestorAgg(conn_str, data_source, t_scales, s_scales)
    #     ingestor.ingest_data_source(clean=False, persist=True, max_limit=1)
    #     print(f"ingesting data finished in {time.time() - start_time} s")

    # # create count tables for inverted index tables
    # idx_tbls = ["space_3_inv", "space_6_inv"]
    # DBIngestorAgg.create_cnt_tbls_for_inv_index_tbls(conn_str, idx_tbls) 

    # # create count tables for individual tables
    # for data_source in data_sources:
    #     ingestor = DBIngestorAgg(conn_str, data_source, t_scales, s_scales)
    #     config = io_utils.load_config(data_source)
    #     print(config["attr_path"])
    #     tbl_attrs = io_utils.load_json(config["attr_path"])
    #     for tbl_id, info in tbl_attrs.items():
    #         print(data_source, tbl_id)
    #         ingestor.create_cnt_tbl(
    #             tbl_id,
    #             info["t_attrs"],
    #             info["s_attrs"],
    #             t_scales,
    #             s_scales,
    #         )

    data_sources = ['asthma']
    t_scales, s_scales = [], [SPATIAL_GRANU.ZIPCODE]
    conn_str = "postgresql://yuegong@localhost/chicago_1m_zipcode"
    # create profiles
    for data_source in data_sources:
        profiler = Profiler(data_source, t_scales, s_scales, conn_str)
        profiler.set_mode('no_cross')
        print("begin collecting agg stats")
        profiler.collect_agg_tbl_col_stats()
        print("begin profiling original data")
        profiler.profile_original_data()
    
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
