import os
from config import META_PATH
import utils.io_utils as io_utils
import time
from utils.coordinate import S_GRANU, pt_to_str
from utils.time_point import T_GRANU, dt_to_str
import pandas as pd
from typing import List
from psycopg2 import sql
import itertools
import psycopg2


class DBSearch:
    def __init__(self, conn_string) -> None:
        self.tbl_schemas = {}
        self.tbl_names = {}
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        self.cur = conn.cursor()
        self.load_meta_data()

    def load_meta_data(self):
        self.tbl_lookup = {}
        meta_data = io_utils.load_json(META_PATH)
        for obj in meta_data:
            tbl_id, tbl_name, t_attrs, s_attrs = (
                obj["tbl_id"],
                obj["tbl_name"],
                obj["t_attrs"],
                obj["s_attrs"],
            )
            ts_attrs = [t_attrs, s_attrs]
            ts_schemas = [p for p in itertools.product(*ts_attrs)]
            self.tbl_schemas[tbl_id] = ts_schemas
            self.tbl_names[tbl_id] = tbl_name

    def search(self, tbl_id: str, attrs: List[str], granu_list):
        tbl_list = self.get_table_list()
        result = []
        for tbl_id2 in tbl_list:
            if tbl_id2 == tbl_id:
                continue
            ts_schemas = self.tbl_schemas[tbl_id2]
            for ts_schema in ts_schemas:
                overlap = self.get_intersection_between_two_ts_schema(
                    tbl_id, attrs, tbl_id2, ts_schema, granu_list
                )
                if overlap > 0:
                    result.append(
                        [tbl_id2, self.tbl_names[tbl_id2], ts_schema, overlap]
                    )

        df = pd.DataFrame(
            data=result, columns=["tbl_id", "tbl_name", "attrs", "overlap"]
        )

        df = df.sort_values(by="overlap", ascending=False)
        return df

    def get_table_list(self):
        select_tbl = """
            SELECT table_name  FROM information_schema.tables WHERE table_schema='public'
            AND table_type='BASE TABLE';
        """

        self.cur.execute(select_tbl)
        tbl_list = [r[0] for r in self.cur.fetchall()]
        return tbl_list

    def get_intersection_between_two_ts_schema(
        self, tbl1: str, attrs1: List[str], tbl2: str, attrs2: List[str], granu_list
    ):
        col_names1, col_names2 = self.get_col_names(attrs1, attrs2, granu_list)

        query = sql.SQL(
            "select {fields1} from {tbl1} INTERSECT SELECT {fields2} from {tbl2}"
        ).format(
            fields1=sql.SQL(",").join([sql.Identifier(col) for col in col_names1]),
            tbl1=sql.Identifier(tbl1),
            fields2=sql.SQL(",").join([sql.Identifier(col) for col in col_names2]),
            tbl2=sql.Identifier(tbl2),
        )
        # print(cur.mogrify(query))
        self.cur.execute(query)
        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        ).dropna()
        return len(df)

    def get_col_name(self, attr, granu):
        if attr is None:
            return None
        return "{}_{}".format(attr, granu.name)

    def get_col_names(self, attrs1, attrs2, granu_list):
        col_names1, col_names2 = [], []
        for i, attr in enumerate(attrs1):
            col_names1.append(self.get_col_name(attr, granu_list[i]))

        for i, attr in enumerate(attrs2):
            col_names2.append(self.get_col_name(attr, granu_list[i]))

        return col_names1, col_names2

    def aggregate_join_two_tables(
        self, tbl1: str, attrs1: List[str], tbl2: str, attrs2: List[str], granu_list
    ):
        col_names1, col_names2 = self.get_col_names(attrs1, attrs2, granu_list)
        agg_join_sql = """
        SELECT {a1_fields1}, a1.cnt1, a2.cnt2 FROM
        (SELECT {fields1}, COUNT(*) as cnt1 FROM {tbl1} GROUP BY {fields1}) a1
        JOIN
        (SELECT {fields2}, COUNT(*) as cnt2 FROM {tbl2} GROUP BY {fields2}) a2
        ON 
        concat_ws(', ', {a1_fields1}) = concat_ws(', ', {a2_fields2})
        """
        query = sql.SQL(agg_join_sql).format(
            fields1=sql.SQL(",").join([sql.Identifier(col) for col in col_names1]),
            a1_fields1=sql.SQL(",").join(
                [sql.Identifier("a1", col) for col in col_names1]
            ),
            tbl1=sql.Identifier(tbl1),
            fields2=sql.SQL(",").join([sql.Identifier(col) for col in col_names2]),
            a2_fields2=sql.SQL(",").join(
                [sql.Identifier("a2", col) for col in col_names2]
            ),
            tbl2=sql.Identifier(tbl2),
        )

        self.cur.execute(query)

        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        ).dropna()

        return df

    def format_result(self, df, attr_len):
        # translate rows
        for i in range(attr_len):
            df.iloc[:, i] = df.iloc[:, i].apply(self.display_value)
        return df

    def display_value(self, v):
        tokens = v.split("-")
        tokens.reverse()
        return "-".join(tokens)
