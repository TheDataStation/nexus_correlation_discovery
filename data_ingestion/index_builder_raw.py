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
from io import StringIO
import sys

"""
DBIngestor ingests dataframes to a database (current implementation uses postgres)
Here are the procedure to ingest a spatial-temporal table
1. Read the table as a dataframe
2. Expand the dataframe by resolving different resolutions for temporal and spatial attributes
3. Ingest the expanded dataframe to the database
4. Create hash indices on top of all spatial-temporal attributes
"""


class DBIngestor:
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

    # Define a function that handles and parses psycopg2 exceptions
    def show_psycopg2_exception(self, err):
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()
        # get the line number when exception occured
        line_n = traceback.tb_lineno
        # print the connect() error
        print("\npsycopg2 ERROR:", err, "on line number:", line_n)
        print("psycopg2 traceback:", traceback, "-- type:", err_type)
        # psycopg2 extensions.Diagnostics object attribute
        print("\nextensions.Diagnostics:", err.diag)
        # print the pgcode and pgerror exceptions
        print("pgerror:", err.pgerror)
        print("pgcode:", err.pgcode, "\n")

    def copy_from_dataFile_StringIO(self, df, tbl_name):
        copy_sql = """
        COPY "{}"
        FROM STDIN
        DELIMITER ',' 
        CSV HEADER
        """.format(
            tbl_name
        )
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        self.cur.copy_expert(copy_sql, buffer)

        # save dataframe to an in memory buffer
        # buffer = StringIO()
        # df.to_csv(buffer, header=False, index=False)
        # buffer.seek(0)

        # try:
        #     self.cur.copy_from(buffer, tbl_name, sep=",", null="", columns=df.columns)

        # except (Exception, psycopg2.DatabaseError) as err:
        #     # pass exception to function
        #     self.show_psycopg2_exception(err)
        #     self.cur.close()

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
        # create agg tbl
        # self.create_agg_tbl(df, tbl_id, t_attrs_success, s_attrs_success, t_attrs)
        # create aggregated index tables
        print("begin creating indices")
        start = time.time()
        self.create_aggregated_idx_tbls(
            df, tbl.tbl_id, t_attrs_success, s_attrs_success
        )
        print("creating index table used {} s".format(time.time() - start))
        # ingest indices table
        # self.create_idx_tbls(df, tbl_id, t_attrs_success, s_attrs_success)
        # create hash indices
        # self.create_indices_on_tbl(tbl_id, t_attrs_success, s_attrs_success)

    def clean_aggregated_idx_tbls(self):
        # delete aggregated indices that are already in the database
        for t_granu in T_GRANU:
            self.del_tbl("time_{}".format(t_granu.value))
        for s_granu in S_GRANU:
            self.del_tbl("space_{}".format(s_granu.value))

        for t_granu in T_GRANU:
            for s_granu in S_GRANU:
                self.del_tbl("time_space_{}_{}".format(t_granu.value, s_granu.value))

    def create_aggregated_idx_tbls(self, df, tbl_id, t_attrs_success, s_attrs_success):
        # instead of creating idx tables for different resolutions for each table,
        # we create an index table for each resolution where we host info about all tables
        idx_name_to_df_data = defaultdict(list)

        for t_attr in t_attrs_success:
            for t_granu in self.t_scales:
                t_attr_granu = "{}_{}".format(t_attr, t_granu.value)
                t_attr_values = (
                    df[t_attr_granu].dropna().drop_duplicates().values.tolist()
                )
                idx_name_to_df_data["time_{}".format(t_granu.value)].extend(
                    [[tbl_id, t_attr, v] for v in t_attr_values]
                )

        for s_attr in s_attrs_success:
            for s_granu in self.s_scales:
                s_attr_granu = "{}_{}".format(s_attr, s_granu.value)
                s_attr_values = (
                    df[s_attr_granu].dropna().drop_duplicates().values.tolist()
                )
                idx_name_to_df_data["space_{}".format(s_granu.value)].extend(
                    [[tbl_id, s_attr, v] for v in s_attr_values]
                )

        for t_attr in t_attrs_success:
            for s_attr in s_attrs_success:
                for t_granu in self.t_scales:
                    for s_granu in self.s_scales:
                        t_attr_granu = "{}_{}".format(t_attr, t_granu.value)
                        s_attr_granu = "{}_{}".format(s_attr, s_granu.value)
                        ts_values = (
                            df[[t_attr_granu, s_attr_granu]]
                            .dropna()
                            .drop_duplicates()
                            .values.tolist()
                        )
                        idx_name_to_df_data[
                            "time_space_{}_{}".format(t_granu.value, s_granu.value)
                        ].extend(
                            [[tbl_id, t_attr, s_attr, v[0], v[1]] for v in ts_values]
                        )
        start = time.time()
        for idx_name, df_data in idx_name_to_df_data.items():
            if idx_name.startswith("time_space"):
                df = pd.DataFrame(
                    df_data, columns=["tbl_id", "t_attr", "s_attr", "t_val", "s_val"]
                )
                df["t_val"] = df["t_val"].astype(int)
                df["s_val"] = df["s_val"].astype(int)
            elif idx_name.startswith("time"):
                df = pd.DataFrame(df_data, columns=["tbl_id", "t_attr", "t_val"])
                df["t_val"] = df["t_val"].astype(int)
            elif idx_name.startswith("space"):
                df = pd.DataFrame(df_data, columns=["tbl_id", "s_attr", "s_val"])
                df["s_val"] = df["s_val"].astype(int)

            self.ingest_df_to_db(df, idx_name, mode="append")
            self.idx_tables.add(idx_name)
        print("ingesting index tables took {} s".format(time.time() - start))

    def expand_df(self, df, t_attrs, s_attrs):
        t_attrs_success = []
        s_attrs_success = []
        df_schema = {}
        for t_attr in t_attrs:
            # parse datetime column to datetime class
            df[t_attr] = pd.to_datetime(
                df[t_attr], infer_datetime_format=True, utc=True, errors="coerce"
            ).dropna()
            df_dts = df[t_attr].apply(parse_datetime).dropna()
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

    def del_tbl(self, tbl_name):
        sql_str = """DROP TABLE IF EXISTS {tbl}"""
        self.cur.execute(sql.SQL(sql_str).format(tbl=sql.Identifier(tbl_name)))

    def create_tbl(self, tbl_name, df):
        df_columns = df.iloc[:0]
        # print(df_columns)

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
            self.copy_from_dataFile_StringIO(df, tbl_name)
            # df.to_sql(
            #     tbl_name,
            #     con=self.conn,
            #     if_exists="replace",
            #     index=False,
            # )
        elif mode == "append":
            if tbl_name not in self.created_tbls:
                self.create_tbl(tbl_name, df)
            self.copy_from_dataFile_StringIO(df, tbl_name)
            # df.to_sql(
            #     tbl_name,
            #     con=self.conn,
            #     if_exists="append",
            #     index=False,
            # )

    def create_index_on_agg_idx_table(self):
        sql_str1 = """
            CREATE INDEX {idx_name} ON {tbl} using hash({col});
        """
        for t_granu in self.t_scales:
            idx_table_name = "time_{}".format(t_granu.value)
            if idx_table_name not in self.idx_tables:
                continue
            query = sql.SQL(sql_str1).format(
                idx_name=sql.Identifier("{}_t".format(idx_table_name)),
                tbl=sql.Identifier(idx_table_name),
                col=sql.Identifier("t_val"),
            )
            self.cur.execute(query)
            query = sql.SQL(sql_str1).format(
                idx_name=sql.Identifier("{}_tbl_id".format(idx_table_name)),
                tbl=sql.Identifier(idx_table_name),
                col=sql.Identifier("tbl_id"),
            )
            self.cur.execute(query)

        for s_granu in self.s_scales:
            idx_table_name = "space_{}".format(s_granu.value)
            if idx_table_name not in self.idx_tables:
                continue
            query = sql.SQL(sql_str1).format(
                idx_name=sql.Identifier("{}_s".format(idx_table_name)),
                tbl=sql.Identifier(idx_table_name),
                col=sql.Identifier("s_val"),
            )
            self.cur.execute(query)
            query = sql.SQL(sql_str1).format(
                idx_name=sql.Identifier("{}_tbl_id".format(idx_table_name)),
                tbl=sql.Identifier(idx_table_name),
                col=sql.Identifier("tbl_id"),
            )
            self.cur.execute(query)

        sql_str2 = """
            CREATE INDEX {idx_name} ON {tbl} ({col1}, {col2});
        """
        for t_granu in self.t_scales:
            for s_granu in self.s_scales:
                idx_table_name = "time_space_{}_{}".format(t_granu.value, s_granu.value)
                if idx_table_name not in self.idx_tables:
                    continue
                query = sql.SQL(sql_str2).format(
                    idx_name=sql.Identifier("{}_ts".format(idx_table_name)),
                    tbl=sql.Identifier(idx_table_name),
                    col1=sql.Identifier("t_val"),
                    col2=sql.Identifier("s_val"),
                )
                self.cur.execute(query)
                query = sql.SQL(sql_str1).format(
                    idx_name=sql.Identifier("{}_tbl_id".format(idx_table_name)),
                    tbl=sql.Identifier(idx_table_name),
                    col=sql.Identifier("tbl_id"),
                )
                self.cur.execute(query)
