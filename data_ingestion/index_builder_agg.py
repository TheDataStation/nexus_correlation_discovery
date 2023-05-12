import pandas as pd
from config import DATA_PATH
import utils.coordinate as coordinate
from utils.time_point import set_temporal_granu, parse_datetime, T_GRANU
from utils.profile_utils import is_num_column_valid
import geopandas as gpd
from psycopg2 import sql
import pandas as pd
from collections import defaultdict
import utils.io_utils as io_utils
from sqlalchemy import create_engine
from utils.coordinate import resolve_spatial_hierarchy, set_spatial_granu, S_GRANU
import psycopg2
from data_search.search_db import DBSearch
import numpy as np
from sqlalchemy.types import *
from data_ingestion.table import Table
import time
from data_search.data_model import Unit, Variable, AggFunc
import data_ingestion.db_ops as db_ops

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
    def __init__(self, conn_string: str, t_scales, s_scales) -> None:
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

    def get_numerical_columns(self, all_columns, tbl: Table):
        numerical_columns = list(set(all_columns) & set(tbl.num_columns))
        valid_num_columns = []
        # exclude columns that contain stop words and timestamp columns
        for col in numerical_columns:
            if is_num_column_valid(col) and col not in tbl.t_attrs:
                valid_num_columns.append(col)
        return valid_num_columns

    def ingest_tbl(self, tbl: Table):
        df = io_utils.read_csv(DATA_PATH + tbl.tbl_id + ".csv")

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
        self.ingest_df_to_db(df, tbl.tbl_id, mode="replace")
        print("ingesting table used {} s".format(time.time() - start))

        # create aggregated tables
        self.create_agg_tbl(
            tbl.tbl_id,
            t_attrs_success,
            s_attrs_success,
            self.t_scales,
            self.s_scales,
            numerical_columns,
        )

        # delete original table
        db_ops.del_tbl(self.cur, tbl.tbl_id)

    def create_agg_tbl(self, tbl, t_attrs, s_attrs, t_scales, s_scales, num_columns):
        st_schema = []
        for t in t_attrs:
            for scale in t_scales:
                st_schema.append([Unit(t, scale)])

        for s in s_attrs:
            for scale in s_scales:
                st_schema.append([Unit(s, scale)])

        for t in t_attrs:
            for s in s_attrs:
                for t_scale in t_scales:
                    for s_scale in s_scales:
                        st_schema.append([Unit(t, t_scale), Unit(s, s_scale)])

        for units in st_schema:
            vars = []
            for agg_col in num_columns:
                vars.append(Variable(agg_col, AggFunc.AVG, "avg_{}".format(agg_col)))
            vars.append(Variable("*", AggFunc.COUNT, "count"))
            db_ops.create_agg_tbl(self.cur, tbl, units, vars)

    def expand_df(self, df, t_attrs, s_attrs):
        t_attrs_success = []
        s_attrs_success = []
        df_schema = {}
        for t_attr in t_attrs:
            # parse datetime column to datetime class
            df[t_attr] = pd.to_datetime(
                df[t_attr], infer_datetime_format=True, utc=True, errors="coerce"
            ).dropna()
            df_dts = df[t_attr].apply(parse_datetime, args=([self.t_scales])).dropna()
            # df_dts = np.vectorize(parse_datetime)(df[t_attr])

            if len(df_dts):
                for t_granu in self.t_scales:
                    new_attr = "{}_{}".format(t_attr, t_granu.value)
                    df[new_attr] = df_dts.apply(
                        set_temporal_granu, args=(t_granu.value,)
                    ).astype(int)

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

            df_resolved = resolve_spatial_hierarchy(gdf, self.s_scales)

            # df_resolved can be none meaning there is no point falling into the shape file
            if df_resolved is None:
                continue
            for s_granu in self.s_scales:
                new_attr = "{}_{}".format(s_attr, s_granu.value)
                df[new_attr] = df_resolved.apply(
                    set_spatial_granu, args=(s_granu.value,)
                ).astype(int)

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
