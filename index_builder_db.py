import pandas as pd
from config import DATA_PATH
import coordinate
from time_point import set_temporal_granu, parse_datetime, T_GRANU
import geopandas as gpd
from psycopg2 import sql
import pandas as pd
import numpy as np
import io_utils
from sqlalchemy import create_engine
from coordinate import resolve_spatial_hierarchy, set_spatial_granu, S_GRANU
import psycopg2


"""
DBIngestor ingests dataframes to a database (current implementation use postgres)
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

    def ingest_tbl(self, tbl_id, t_attrs, s_attrs):
        df = io_utils.read_csv(DATA_PATH + tbl_id + ".csv")
        # expand dataframe
        self.expand_df(df, t_attrs, s_attrs)
        # ingest dataframe to database
        self.ingest_df_to_db(df, tbl_id)
        # create hash indices
        self.create_indices_on_tbl(tbl_id, t_attrs, s_attrs)

    def expand_df(self, df, t_attrs, s_attrs):
        attrs_to_project = []
        for t_attr in t_attrs:
            attrs_to_project.append(t_attr)
            # parse datetime column to datetime class
            df[t_attr] = pd.to_datetime(
                df[t_attr], infer_datetime_format=True, utc=True, errors="coerce"
            ).dropna()
            df_dts = df[t_attr].apply(parse_datetime).dropna()
            for t_granu in T_GRANU:
                new_attr = "{}_{}".format(t_attr, t_granu.name)
                attrs_to_project.append(new_attr)
                df[new_attr] = df_dts.apply(set_temporal_granu, args=(t_granu.value,))

        for s_attr in s_attrs:
            attrs_to_project.append(s_attr)
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
                new_attr = "{}_{}".format(s_attr, s_granu.name)
                attrs_to_project.append(new_attr)
                df[new_attr] = df_resolved.apply(
                    set_spatial_granu, args=(s_granu.value,)
                )

        # numeric_columns = list(df.select_dtypes(include=[np.number]).columns.values)
        # final_proj_list = list(set(attrs_to_project) | set(numeric_columns))
        return df

    def ingest_df_to_db(self, df, tbl_id):
        # drop old tables before ingesting the new dataframe
        sql_str = """DROP TABLE IF EXISTS {tbl}"""
        self.cur.execute(sql.SQL(sql_str).format(tbl=sql.Identifier(tbl_id)))
        df.to_sql(tbl_id, con=self.conn, if_exists="replace", index=False)

    def create_indices_on_tbl(self, tbl_id, t_attrs, s_attrs):
        self.create_index_on_unary_attr(tbl_id, t_attrs, T_GRANU)
        self.create_index_on_unary_attr(tbl_id, s_attrs, S_GRANU)
        self.create_index_on_binary_attrs(tbl_id, t_attrs, s_attrs)

    def create_index_on_unary_attr(self, tbl_id, attrs, resolutions):
        sql_str = """
            CREATE INDEX {idx_name} on {tbl} USING HASH ({field});;
        """
        for attr in attrs:
            for granu in resolutions:
                attr_name = "{}_{}".format(attr, granu.name)
                # execute creating index
                self.cur.execute(
                    sql.SQL(sql_str).format(
                        idx_name=sql.Identifier("{}_{}_idx".format(tbl_id, attr_name)),
                        tbl=sql.Identifier(tbl_id),
                        field=sql.Identifier(attr),
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
