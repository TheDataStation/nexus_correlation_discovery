import pandas as pd
from config import DATA_PATH
import utils.coordinate as coordinate
from utils.time_point import set_temporal_granu, parse_datetime, T_GRANU
import geopandas as gpd
from psycopg2 import sql
import pandas as pd
from collections import defaultdict
import utils.io_utils as io_utils
from sqlalchemy import create_engine
from utils.coordinate import resolve_spatial_hierarchy, set_spatial_granu, S_GRANU
import psycopg2
from data_search.data_model import Unit, Variable, AggFunc
from data_search.search_db import DBSearch
import numpy as np
from sqlalchemy.types import *
from typing import List
from dataclasses import dataclass

"""
DBIngestor ingests dataframes to a database (current implementation uses postgres)
Here are the procedure to ingest a spatial-temporal table
1. Read the table as a dataframe
2. Expand the dataframe by resolving different resolutions for temporal and spatial attributes
3. Ingest the expanded dataframe to the database
4. Create hash indices on top of all spatial-temporal attributes
"""


@dataclass
class Table:
    domain: str
    tbl_id: str
    tbl_name: str
    t_attrs: List[str]
    s_attrs: List[str]
    num_columns: List[str]


class DBIngestor:
    def __init__(self, conn_string: str) -> None:
        db = create_engine(conn_string)
        self.conn = db.connect()
        conn_copg2 = psycopg2.connect(conn_string)
        conn_copg2.autocommit = True
        self.cur = conn_copg2.cursor()
        self.db_search = DBSearch(conn_string)
        # successfully ingested table information
        self.tbls = {}

    def is_num_column_valid(self, col_name):
        stop_words_contain = [
            "id",
            "longitude",
            "latitude",
            "ward",
            "date",
            "zipcode",
            "zip_code",
            "_zip",
            "street_number",
            "street_address",
            "district",
            "coordinate",
            "community_area",
            "_no",
            "_year",
            "_day",
            "_month",
            "_hour",
            "_number",
            "_code",
            "census_tract",
            "address",
            "x_coord",
            "y_coord",
        ]
        stop_words_equal = [
            "permit_",
            "beat",
            "zip",
            "year",
            "week_number",
            "ssa",
            "license_",
            "day_of_week",
            "police_sector",
            "police_beat",
            "license",
            "month",
            "hour",
            "day",
            "lat",
            "long",
            "mmwr_week",
            "zip4",
            "phone",
        ]
        for stop_word in stop_words_contain:
            if stop_word in col_name:
                return False
        for stop_word in stop_words_equal:
            if stop_word == col_name:
                return False
        return True

    def get_numerical_columns(self, all_columns, tbl: Table):
        numerical_columns = list(set(all_columns) & set(tbl.num_columns))
        valid_num_columns = []
        # exclude columns that contain stop words and timestamp columns
        for col in numerical_columns:
            if self.is_num_column_valid(col) and col not in tbl.t_attrs:
                valid_num_columns.append(col)
        return valid_num_columns

    def ingest_tbl(self, tbl: Table):
        df = io_utils.read_csv(DATA_PATH + tbl.tbl_id + ".csv")

        # get numerical columns
        all_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        numerical_columns = self.get_numerical_columns(all_columns, tbl)

        # expand dataframe
        df, df_schema, t_attrs_success, s_attrs_success = self.expand_df(
            df, tbl.t_attrs, tbl.s_attrs
        )
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

        # ingest dataframe to database
        self.ingest_df_to_db(df, tbl.tbl_id, mode="replace")

        # create agg tbl
        # self.create_agg_tbl(df, tbl_id, t_attrs_success, s_attrs_success, t_attrs)
        # create aggregated index tables
        self.create_aggregated_idx_tbls(
            df, tbl.tbl_id, t_attrs_success, s_attrs_success
        )
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
            for t_granu in T_GRANU:
                t_attr_granu = "{}_{}".format(t_attr, t_granu.value)
                t_attr_values = (
                    df[t_attr_granu].dropna().drop_duplicates().values.tolist()
                )
                idx_name_to_df_data["time_{}".format(t_granu.value)].extend(
                    [[tbl_id, t_attr, v] for v in t_attr_values]
                )

        for s_attr in s_attrs_success:
            for s_granu in S_GRANU:
                s_attr_granu = "{}_{}".format(s_attr, s_granu.value)
                s_attr_values = (
                    df[s_attr_granu].dropna().drop_duplicates().values.tolist()
                )
                idx_name_to_df_data["space_{}".format(s_granu.value)].extend(
                    [[tbl_id, s_attr, v] for v in s_attr_values]
                )

        for t_attr in t_attrs_success:
            for s_attr in s_attrs_success:
                for t_granu in T_GRANU:
                    for s_granu in S_GRANU:
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

        for idx_name, df_data in idx_name_to_df_data.items():
            if idx_name.startswith("time_space"):
                df = pd.DataFrame(
                    df_data, columns=["tbl_id", "t_attr", "s_attr", "t_val", "s_val"]
                )
            elif idx_name.startswith("time"):
                df = pd.DataFrame(df_data, columns=["tbl_id", "t_attr", "t_val"])
            elif idx_name.startswith("space"):
                df = pd.DataFrame(df_data, columns=["tbl_id", "s_attr", "s_val"])

            self.ingest_df_to_db(df, idx_name, mode="append")

    def create_idx_tbls(self, df, tbl_id, t_attrs_success, s_attrs_success):
        # maintain the mapping between index tbl name to df
        idx_name_to_df = {}
        for t_attr in t_attrs_success:
            for t_granu in T_GRANU:
                t_attr_granu = "{}_{}".format(t_attr, t_granu.value)
                df_idx = df[t_attr_granu].dropna().drop_duplicates()
                self.del_tbl("{}_{}_{}".format(tbl_id, t_attr, t_granu.name))
                idx_name_to_df[
                    "{}_{}_{}".format(tbl_id, t_attr, t_granu.value)
                ] = df_idx

        for s_attr in s_attrs_success:
            for s_granu in S_GRANU:
                s_attr_granu = "{}_{}".format(s_attr, s_granu.value)
                df_idx = df[s_attr_granu].dropna().drop_duplicates()
                self.del_tbl("{}_{}_{}".format(tbl_id, s_attr, s_granu.name))
                idx_name_to_df[
                    "{}_{}_{}".format(tbl_id, s_attr, s_granu.value)
                ] = df_idx

        for t_attr in t_attrs_success:
            for s_attr in s_attrs_success:
                for t_granu in T_GRANU:
                    for s_granu in S_GRANU:
                        t_attr_granu = "{}_{}".format(t_attr, t_granu.value)
                        s_attr_granu = "{}_{}".format(s_attr, s_granu.value)

                        df_idx = (
                            df[[t_attr_granu, s_attr_granu]].dropna().drop_duplicates()
                        )
                        idx_name_to_df[
                            "{}_{}_{}_{}_{}".format(
                                tbl_id, t_attr, t_granu.value, s_attr, s_granu.value
                            )
                        ] = df_idx

        for idx_name, df in idx_name_to_df.items():
            self.ingest_df_to_db(df, idx_name)

    def get_agg_df(self, tbl_id, num_columns, units):
        # create variables, only consider avg and count now
        vars = [
            Variable(attr, AggFunc.AVG, "{}_{}".format(attr, "avg"))
            for attr in num_columns
        ]
        vars.append(Variable("*", AggFunc.COUNT, "count"))
        df_agg = self.db_search.transform(tbl_id, units, vars)
        return df_agg

    def create_agg_tbl(self, df, tbl_id, t_attrs_success, s_attrs_success, t_attrs):
        idx_name_to_df = {}
        num_columns = self.get_numerical_columns(tbl_id, t_attrs)
        for t_attr in t_attrs_success:
            for t_granu in T_GRANU:
                units = [Unit(t_attr, t_granu)]
                df_agg = self.get_agg_df(tbl_id, num_columns, units)
                idx_name_to_df[
                    "{}_{}_{}".format(tbl_id, t_attr, t_granu.value)
                ] = df_agg

        for s_attr in s_attrs_success:
            for s_granu in S_GRANU:
                units = [Unit(s_attr, s_granu)]
                df_agg = self.get_agg_df(tbl_id, num_columns, units)
                idx_name_to_df[
                    "{}_{}_{}".format(tbl_id, s_attr, s_granu.value)
                ] = df_agg

        for t_attr in t_attrs_success:
            for s_attr in s_attrs_success:
                for t_granu in T_GRANU:
                    for s_granu in S_GRANU:
                        units = [Unit(t_attr, t_granu), Unit(s_attr, s_granu)]
                        df_agg = self.get_agg_df(tbl_id, num_columns, units)
                        idx_name_to_df[
                            "{}_{}_{}_{}_{}".format(
                                tbl_id, t_attr, t_granu.value, s_attr, s_granu.value
                            )
                        ] = df_agg

        for idx_name, df in idx_name_to_df.items():
            self.ingest_df_to_db(df, idx_name)

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
            if len(df_dts):
                for t_granu in T_GRANU:
                    new_attr = "{}_{}".format(t_attr, t_granu.value)
                    df[new_attr] = df_dts.apply(
                        set_temporal_granu, args=(t_granu.value,)
                    ).astype(int)
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

            df_resolved = resolve_spatial_hierarchy(gdf)

            # df_resolved can be none meaning there is no point falling into the shape file
            if df_resolved is None:
                continue
            for s_granu in S_GRANU:
                new_attr = "{}_{}".format(s_attr, s_granu.value)
                df[new_attr] = df_resolved.apply(
                    set_spatial_granu, args=(s_granu.value,)
                ).astype(int)
                df_schema[new_attr] = Integer()
            s_attrs_success.append(s_attr)

        # numeric_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        # final_proj_list = list(set(attrs_to_project) | set(numeric_columns))
        # print(df.dtypes)
        return df, df_schema, t_attrs_success, s_attrs_success

    def del_tbl(self, tbl_name):
        sql_str = """DROP TABLE IF EXISTS {tbl}"""
        self.cur.execute(sql.SQL(sql_str).format(tbl=sql.Identifier(tbl_name)))

    def ingest_df_to_db(self, df, tbl_name, mode="replace", df_schema=None):
        if mode == "replace":
            # drop old tables before ingesting the new dataframe
            self.del_tbl(tbl_name)
            df.to_sql(
                tbl_name,
                con=self.conn,
                if_exists="replace",
                index=False,
                dtype=df_schema,
            )
        elif mode == "append":
            df.to_sql(tbl_name, con=self.conn, if_exists="append", index=False)

    def create_indices_on_tbl(self, tbl_id, t_attrs, s_attrs):
        self.create_index_on_unary_attr(tbl_id, t_attrs, T_GRANU, "t")
        self.create_index_on_unary_attr(tbl_id, s_attrs, S_GRANU, "s")
        self.create_index_on_binary_attrs(tbl_id, t_attrs, s_attrs)

    def create_index_on_unary_attr(self, tbl_id, attrs, resolutions, type):
        sql_str = """
            CREATE INDEX {idx_name} on {tbl} using hash ({field});
        """

        for i, attr in enumerate(attrs):
            for granu in resolutions:
                attr_name = "{}_{}".format(attr, granu.value)
                # execute creating index
                self.cur.execute(
                    sql.SQL(sql_str).format(
                        idx_name=sql.Identifier(
                            "{}_{}{}_{}_idx".format(tbl_id, type, i, granu.value)
                        ),
                        tbl=sql.Identifier(tbl_id),
                        field=sql.Identifier(attr_name),
                    )
                )

                idx_tbl_name = "{}_{}_{}".format(tbl_id, attr, granu.value)
                self.cur.execute(
                    sql.SQL(
                        "CREATE unique INDEX {idx_name} on {tbl} ({field});"
                    ).format(
                        idx_name=sql.Identifier(
                            "{}_{}{}_{}_idx2".format(tbl_id, type, i, granu.value)
                        ),
                        tbl=sql.Identifier(idx_tbl_name),
                        field=sql.Identifier(attr_name),
                    )
                )

    def create_index_on_agg_idx_table(self):
        sql_str1 = """
            CREATE INDEX {idx_name} ON {tbl} using hash({col});
        """
        for t_granu in T_GRANU:
            idx_table_name = "time_{}".format(t_granu.value)
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

        for s_granu in S_GRANU:
            idx_table_name = "space_{}".format(s_granu.value)
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
        for t_granu in T_GRANU:
            for s_granu in S_GRANU:
                idx_table_name = "time_space_{}_{}".format(t_granu.value, s_granu.value)
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

    def create_index_on_binary_attrs(self, tbl_id, t_attrs, s_attrs):
        sql_str = """
            CREATE INDEX {idx_name} ON {tbl} ({field1}, {field2});
        """
        for i, t_attr in enumerate(t_attrs):
            for j, s_attr in enumerate(s_attrs):
                for t_granu in T_GRANU:
                    for s_granu in S_GRANU:
                        t_attr_granu = "{}_{}".format(t_attr, t_granu.value)
                        s_attr_granu = "{}_{}".format(s_attr, s_granu.value)

                        query = sql.SQL(sql_str).format(
                            idx_name=sql.Identifier(
                                "{}_{}_{}_idx".format(
                                    tbl_id,
                                    "{}_{}".format(i, t_granu.value),
                                    "{}_{}".format(j, s_granu.value),
                                )
                            ),
                            tbl=sql.Identifier(tbl_id),
                            field1=sql.Identifier(t_attr_granu),
                            field2=sql.Identifier(s_attr_granu),
                        )
                        # print(self.cur.mogrify(query))
                        self.cur.execute(query)

                        idx_tbl_name = "{}_{}_{}_{}_{}".format(
                            tbl_id, t_attr, t_granu.value, s_attr, s_granu.value
                        )

                        query = sql.SQL(
                            "CREATE unique {idx_name} ON {tbl} ({field1}, {field2});"
                        ).format(
                            idx_name=sql.Identifier(
                                "{}_{}_{}_{}_{}_idx2".format(
                                    tbl_id, i, t_granu.value, j, s_granu.value
                                )
                            ),
                            tbl=sql.Identifier(idx_tbl_name),
                            field1=sql.Identifier(t_attr_granu),
                            field2=sql.Identifier(s_attr_granu),
                        )
                        # print(self.cur.mogrify(query))
                        self.cur.execute(query)
