import pandas as pd
import utils.coordinate as coordinate
from utils.time_point import set_temporal_granu, parse_datetime, T_GRANU
from utils.profile_utils import is_num_column_valid
import geopandas as gpd
import os
import utils.io_utils as io_utils
from sqlalchemy import create_engine
from utils.coordinate import resolve_spatial_hierarchy, set_spatial_granu, S_GRANU
import psycopg2
from data_search.search_db import DBSearch
import numpy as np
from sqlalchemy.types import *
from data_ingestion.table import Table
import time
from tqdm import tqdm
from data_search.data_model import (
    Unit,
    Variable,
    AggFunc,
    UnitType,
    ST_Schema,
    SchemaType,
)
import data_ingestion.db_ops as db_ops
from data_ingestion.db_ops import IndexType
from typing import List
from utils.correlation_sketch_utils import murmur3_32, grm, FixedSizeMaxHeap

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
    def __init__(self, conn_string: str, source, t_scales, s_scales) -> None:
        db = create_engine(conn_string)
        self.conn = db.connect()
        conn_copg2 = psycopg2.connect(conn_string)
        conn_copg2.autocommit = True
        self.cur = conn_copg2.cursor()
        self.db_search = DBSearch(conn_string)
        # tables that have been created
        self.created_tbls = set()
        # successfully ingested table information
        self.tbls = {}
        # idx tables that are created successfully
        self.idx_tables = set()

        self.t_scales = t_scales
        self.s_scales = s_scales

        self.config = io_utils.load_config(source)
        self.data_path = self.config["data_path"]
        self.shape_path = self.config["shape_path"]

    @staticmethod
    def get_numerical_columns(all_columns, tbl: Table):
        numerical_columns = list(set(all_columns) & set(tbl.num_columns))
        valid_num_columns = []
        # exclude columns that contain stop words and timestamp columns
        for col in numerical_columns:
            if is_num_column_valid(col) and col not in tbl.t_attrs:
                valid_num_columns.append(col)
        return valid_num_columns

    @staticmethod
    def select_valid_attrs(attrs, max_limit=0):
        valid_attrs = []
        for attr in attrs:
            if any(
                keyword in attr
                for keyword in ["update", "modified", "_end", "end_", "status", "_notified"]
            ):
                continue
            valid_attrs.append(attr)
        # limit the max number of spatio/temporal join keys a table can have
        return valid_attrs[:max_limit]

    def ingest_data_source(self, clean=False, persist=False, first=False):
        meta_path = self.config["meta_path"]
        geo_chain = self.config["geo_chain"]
        geo_keys = self.config["geo_keys"]
        coordinate.resolve_geo_chain(geo_chain, geo_keys)
        meta_data = io_utils.load_json(meta_path)
        if clean:
            self.clean_aggregated_idx_tbls()

        max_limit = 2
        for obj in tqdm(meta_data):
            t_attrs, s_attrs = self.select_valid_attrs(
                obj["t_attrs"], max_limit
            ), self.select_valid_attrs(obj["s_attrs"], max_limit)
            if len(t_attrs) == 0 and len(s_attrs) == 0:
                continue
            if first:
                # when first is specified, only take the first t_attr and s_attr
                if t_attrs:
                    t_attrs = [t_attrs[0]]
                if s_attrs:
                    s_attrs = [s_attrs[0]]
            tbl = Table(
                domain=obj["domain"],
                tbl_id=obj["tbl_id"],
                tbl_name=obj["tbl_name"],
                t_attrs=t_attrs,
                s_attrs=s_attrs,
                num_columns=obj["num_columns"],
            )
            print(tbl.t_attrs, tbl.s_attrs)
            print(tbl.tbl_id)
            self.ingest_tbl(tbl)

        for idx_tbl in tqdm(self.idx_tables):
            db_ops.create_indices_on_tbl(
                self.cur, f"{idx_tbl}_inv", idx_tbl, ["val"], mode=IndexType.HASH
            )
        # create a cnt table for every invereted index
        self.create_inv_cnt_tbls(self.idx_tables)
        # create a cnt table for each individual table
        for tbl_id, info in self.tbls.items():
            self.create_cnt_tbl(
                tbl_id,
                info["t_attrs"],
                info["s_attrs"],
                self.t_scales,
                self.s_scales,
            )

        if persist:
            io_utils.dump_json(
                self.config["attr_path"],
                self.tbls,
            )
            io_utils.dump_json(self.config["idx_tbl_path"], list(self.idx_tables))

    def create_inv_cnt_tbls(self, idx_tbls):
        db_ops.create_inv_idx_cnt_tbl(self.cur, idx_tbls)

    def create_cnt_tbl(self, tbl, t_attrs, s_attrs, t_scales, s_scales):
        st_schema_list = []
        for t in t_attrs:
            for t_scale in t_scales:
                st_schema_list.append((tbl, ST_Schema(t_unit=Unit(t, t_scale))))

        for s in s_attrs:
            for s_scale in s_scales:
                st_schema_list.append((tbl, ST_Schema(s_unit=Unit(s, s_scale))))

        for t in t_attrs:
            for s in s_attrs:
                for t_scale in t_scales:
                    for s_scale in s_scales:
                        st_schema_list.append(
                            (tbl, ST_Schema(Unit(t, t_scale), Unit(s, s_scale)))
                        )
        for schema in st_schema_list:
            db_ops.create_cnt_tbl_for_agg_tbl(self.cur, schema[0], schema[1])

    def ingest_tbl(self, tbl: Table):
        tbl_path = os.path.join(self.data_path, f"{tbl.tbl_id}.csv")
        df = io_utils.read_csv(tbl_path)

        # get numerical columns
        all_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        numerical_columns = self.get_numerical_columns(all_columns, tbl)

        df = df[tbl.t_attrs + tbl.s_attrs + numerical_columns]

        print("begin expanding dataframe")
        # expand dataframe
        start = time.time()
        df, df_schema, t_attrs_success, s_attrs_success = self.expand_df(
            df, tbl.t_attrs, tbl.s_attrs
        )

        print("expanding table used {} s".format(time.time() - start))
        # if dataframe is None, return
        if df is None:
            print("df is none")
            return
        self.tbls[tbl.tbl_id] = {
            "name": tbl.tbl_name,
            "t_attrs": t_attrs_success,
            "s_attrs": s_attrs_success,
            "num_columns": numerical_columns,
        }
        print("begin ingesting")
        start = time.time()
        # ingest dataframe to database
        # df = df.replace({np.NaN: None})  # substitue NaT to None to avoid psycopg error
        self.ingest_df_to_db(df, tbl.tbl_id, mode="replace")
        print("ingesting table used {} s".format(time.time() - start))

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
                st_schema_list.append(ST_Schema(t_unit=Unit(t, scale)))

        for s in s_attrs:
            for scale in s_scales:
                st_schema_list.append(ST_Schema(s_unit=Unit(s, scale)))

        for t in t_attrs:
            for s in s_attrs:
                # for i in range(len(t_scales)):
                #     st_schema_list.append(
                #         ST_Schema(Unit(t, t_scales[i]), Unit(s, s_scales[i]))
                #     )
                for t_scale in t_scales:
                    for s_scale in s_scales:
                        st_schema_list.append(
                            ST_Schema(Unit(t, t_scale), Unit(s, s_scale))
                        )

        for st_schema in st_schema_list:
            vars = []
            for agg_col in num_columns:
                vars.append(Variable(agg_col, AggFunc.AVG, "avg_{}".format(agg_col)))
            vars.append(Variable("*", AggFunc.COUNT, "count"))

            start = time.time()
            # transform data and also create an index on the key val column
            agg_tbl_name = db_ops.create_agg_tbl(self.cur, tbl, st_schema, vars)
            print(f"finish aggregating table in {time.time()-start} s")
            # ingest spatio-temporal values to an index table
            start = time.time()
            self.ingest_agg_tbl_to_idx_tbl(tbl, agg_tbl_name, st_schema)
            print(f"finish ingesting to idx table in {time.time()-start} s")
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
                st_schema_list.append(ST_Schema(t_unit=Unit(t, scale)))

        for s in s_attrs:
            for scale in s_scales:
                st_schema_list.append(ST_Schema(s_unit=Unit(s, scale)))

        for t in t_attrs:
            for s in s_attrs:
                for t_scale in t_scales:
                    for s_scale in s_scales:
                        st_schema_list.append(
                            ST_Schema(Unit(t, t_scale), Unit(s, s_scale))
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

    def ingest_agg_tbl_to_idx_tbl(self, tbl, agg_tbl, st_schema: ST_Schema):
        # decide which index table to ingest the agg_tbl values
        idx_tbl_name = st_schema.get_idx_tbl_name() + "_inv"
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

    def expand_df(self, df, t_attrs, s_attrs):
        t_attrs_success = []
        s_attrs_success = []
        df_schema = {}
        for t_attr in t_attrs:
            # parse datetime column to datetime class
            df[t_attr] = pd.to_datetime(df[t_attr], utc=False, errors="coerce").replace(
                {np.NaN: None}
            )
            df_dts = df[t_attr].apply(parse_datetime).dropna()
            # df_dts = np.vectorize(parse_datetime)(df[t_attr])
            if len(df_dts):
                for t_granu in self.t_scales:
                    new_attr = "{}_{}".format(t_attr, t_granu.value)
                    df[new_attr] = df_dts.apply(set_temporal_granu, args=(t_granu,))

                    # df[new_attr] = np.vectorize(set_temporal_granu, otypes=[object])(
                    #     df_dts, t_granu.value
                    # )

                    df_schema[new_attr] = Integer()
                t_attrs_success.append(t_attr)
        for s_attr in s_attrs:
            # parse (long, lat) pairs to point
            df_points = df[s_attr].apply(coordinate.parse_coordinate)

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
                new_attr = "{}_{}".format(s_attr, s_granu.value)
                df[new_attr] = df_resolved.apply(set_spatial_granu, args=(s_granu,))
                df_schema[new_attr] = Integer()
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

    def ingest_df_to_db(self, df, tbl_name, mode="replace", df_schema=None):
        if mode == "replace":
            # drop old tables before ingesting the new dataframe
            self.create_tbl(tbl_name, df)
            db_ops.copy_from_dataFile_StringIO(self.cur, df, tbl_name)

        elif mode == "append":
            if tbl_name not in self.created_tbls:
                self.create_tbl(tbl_name, df)
            db_ops.copy_from_dataFile_StringIO(self.cur, df, tbl_name)


if __name__ == "__main__":
    start_time = time.time()
    t_scales = [T_GRANU.DAY, T_GRANU.MONTH]
    s_scales = [S_GRANU.BLOCK, S_GRANU.TRACT]
    data_source = "chicago_10k"
    config = io_utils.load_config(data_source)
    conn_string = config["db_path"]
    ingestor = DBIngestorAgg(conn_string, data_source, t_scales, s_scales)
    # tbl = Table(
    #     domain="",
    #     tbl_id="qqqh-hgyw",
    #     tbl_name="",
    #     t_attrs=[
    #         "lse_report_reviewed_on_date",
    #         # "scheduled_inspection_date",
    #         # "rescheduled_inspection_date",
    #     ],
    #     s_attrs=[],
    #     num_columns=[],
    # )
    # ingestor.ingest_tbl(tbl)
    ingestor.ingest_data_source(clean=True, persist=True)
