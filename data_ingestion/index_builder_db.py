import pandas as pd
from config import DATA_PATH, META_PATH
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

"""
DBIngestor ingests dataframes to a database (current implementation uses postgres)
Here are the procedure to ingest a spatial-temporal table
1. Read the table as a dataframe
2. Expand the dataframe by resolving different resolutions for temporal and spatial attributes
3. Ingest the expanded dataframe to the database
4. Create hash indices on top of all spatial-temporal attributes
"""


class DBIngestor:
    def __init__(self, conn_string: str) -> None:
        db = create_engine(conn_string)
        self.conn = db.connect()
        conn_copg2 = psycopg2.connect(conn_string)
        conn_copg2.autocommit = True
        self.cur = conn_copg2.cursor()
        self.db_search = DBSearch(conn_string)
        self.load_tbl_lookup()
        # successfully ingested table information
        self.tbls = {}
        # self.clean_aggregated_idx_tbls()

    def load_tbl_lookup(self):
        self.meta_data = io_utils.load_json(META_PATH)
        self.tbl_lookup = {}
        for obj in self.meta_data:
            tbl_id, tbl_name = (
                obj["tbl_id"],
                obj["tbl_name"],
            )
            self.tbl_lookup[tbl_id] = tbl_name
        return self.tbl_lookup

    def is_num_column_valid(self, col_name):
        stop_words = [
            "id",
            "longitude",
            "latitude",
            "ward",
            "date",
            "zipcode",
            "district",
            "coordinate",
        ]
        for stop_word in stop_words:
            if stop_word in col_name:
                return False
        return True

    def get_numerical_columns(self, df, t_attrs):
        numerical_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        valid_num_columns = []
        # exclude columns that contain stop words and timestamp columns
        for col in numerical_columns:
            if self.is_num_column_valid(col) and col not in t_attrs:
                valid_num_columns.append(col)
        return valid_num_columns

    def ingest_tbl(self, tbl_id, t_attrs, s_attrs):
        df = io_utils.read_csv(DATA_PATH + tbl_id + ".csv")

        # get numerical columns
        numerical_columns = self.get_numerical_columns(df, t_attrs)
        # expand dataframe
        df, t_attrs_success, s_attrs_success = self.expand_df(df, t_attrs, s_attrs)
        # if dataframe is None, return
        if df is None:
            return
        self.tbls[tbl_id] = {
            "name": self.tbl_lookup[tbl_id],
            "t_attrs": t_attrs_success,
            "s_attrs": s_attrs_success,
            "num_columns": numerical_columns,
        }

        # ingest dataframe to database
        # self.ingest_df_to_db(df, tbl_id)

        # create agg tbl
        # self.create_agg_tbl(df, tbl_id, t_attrs_success, s_attrs_success, t_attrs)
        # create aggregated index tables
        # self.create_aggregated_idx_tbls(df, tbl_id, t_attrs_success, s_attrs_success)
        # ingest indices table
        # self.create_idx_tbls(df, tbl_id, t_attrs_success, s_attrs_success)
        # # create hash indices
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
                        # idx_name_to_df[
                        #     "time_space_{}_{}".format(t_granu.value, s_granu.value)
                        # ] = pd.DataFrame(
                        #     [[tbl_id, t_attr, s_attr, v[0], v[1]] for v in ts_values],
                        #     columns=["tbl_id", "t_attr", "s_attr", "t_val", "s_val"],
                        # )

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
                        self.del_tbl(
                            "{}_{}_{}_{}_{}".format(
                                tbl_id, t_attr, t_granu.name, s_attr, s_granu.name
                            )
                        )
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
        num_columns = get_numerical_columns(tbl_id, t_attrs)
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
                    )
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
                )
            s_attrs_success.append(s_attr)

        # numeric_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        # final_proj_list = list(set(attrs_to_project) | set(numeric_columns))

        return df, t_attrs_success, s_attrs_success

    def del_tbl(self, tbl_name):
        sql_str = """DROP TABLE IF EXISTS {tbl}"""
        self.cur.execute(sql.SQL(sql_str).format(tbl=sql.Identifier(tbl_name)))

    def ingest_df_to_db(self, df, tbl_name, mode="replace"):
        if mode == "replace":
            # drop old tables before ingesting the new dataframe
            self.del_tbl(tbl_name)
            df.to_sql(tbl_name, con=self.conn, if_exists="replace", index=False)
        elif mode == "append":
            df.to_sql(tbl_name, con=self.conn, if_exists="append", index=False)

    def create_indices_on_tbl(self, tbl_id, t_attrs, s_attrs):
        self.create_index_on_unary_attr(tbl_id, t_attrs, T_GRANU)
        self.create_index_on_unary_attr(tbl_id, s_attrs, S_GRANU)
        self.create_index_on_binary_attrs(tbl_id, t_attrs, s_attrs)

    def create_index_on_unary_attr(self, tbl_id, attrs, resolutions):
        sql_str = """
            CREATE INDEX {idx_name} on {tbl} ({field});
        """
        for attr in attrs:
            for granu in resolutions:
                attr_name = "{}_{}".format(attr, granu.name)
                # execute creating index
                self.cur.execute(
                    sql.SQL(sql_str).format(
                        idx_name=sql.Identifier("{}_{}_idx".format(tbl_id, attr_name)),
                        tbl=sql.Identifier(tbl_id),
                        field=sql.Identifier(attr_name),
                    )
                )

    def create_index_on_binary_attrs(self, tbl_id, t_attrs, s_attrs):
        sql_str = """
            CREATE INDEX {idx_name} ON {tbl} ({field1}, {field2});
        """
        for t_attr in t_attrs:
            for s_attr in s_attrs:
                for t_granu in T_GRANU:
                    for s_granu in S_GRANU:
                        t_attr_granu = "{}_{}".format(t_attr, t_granu.name)
                        s_attr_granu = "{}_{}".format(s_attr, s_granu.name)

                        query = sql.SQL(sql_str).format(
                            idx_name=sql.Identifier(
                                "{}_{}_{}_idx".format(
                                    tbl_id,
                                    "{}_{}".format(t_attr, t_granu.value),
                                    "{}_{}".format(s_attr, s_granu.value),
                                )
                            ),
                            tbl=sql.Identifier(tbl_id),
                            field1=sql.Identifier(t_attr_granu),
                            field2=sql.Identifier(s_attr_granu),
                        )
                        # print(self.cur.mogrify(query))
                        self.cur.execute(query)
