from config import META_PATH, ATTR_PATH
import utils.io_utils as io_utils
import pandas as pd
from typing import List
from psycopg2 import sql
import itertools
import psycopg2
from collections import defaultdict
from utils.coordinate import S_GRANU
from utils.time_point import T_GRANU
from data_search.data_model import Unit, Variable


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
        tbl_attr_data = io_utils.load_json(ATTR_PATH)

        for tbl_id, info in tbl_attr_data.items():
            tbl_name, t_attrs, s_attrs = (
                info["name"],
                info["t_attrs"],
                info["s_attrs"],
            )

            self.tbl_names[tbl_id] = tbl_name
            ts_schemas_time = []
            for t in t_attrs:
                ts_schemas_time.append([t])

            ts_schemas_space = []
            for s in s_attrs:
                ts_schemas_space.append([s])

            ts_attrs = [t_attrs, s_attrs]
            ts_schemas_ts = [p for p in itertools.product(*ts_attrs)]

            self.tbl_schemas[tbl_id] = {
                "t": ts_schemas_time,
                "s": ts_schemas_space,
                "ts": ts_schemas_ts,
            }

    def find_augmentable_tables(self, tbl: str, units: List[Unit], threshold):
        tbl_list = self.get_table_list()
        t_granu, s_granu = None, None
        for unit in units:
            if unit.granu in T_GRANU:
                t_granu = unit.granu
            elif unit.granu in S_GRANU:
                s_granu = unit.granu

        result = []
        for tbl2 in tbl_list:
            if tbl2 == tbl:
                # skip self
                continue
            align_schemas = self.tbl_schemas[tbl2]
            units2_list = []
            if len(units) == 2:
                units2_list = [
                    [Unit(attrs[0], t_granu), Unit(attrs[1], s_granu)]
                    for attrs in align_schemas["ts"]
                ]
            elif units[0].granu in T_GRANU:
                units2_list = [
                    [Unit(attrs[0], t_granu)] for attrs in align_schemas["t"]
                ]
            else:
                units2_list = [
                    [Unit(attrs[0], s_granu)] for attrs in align_schemas["s"]
                ]

            for units2 in units2_list:
                overlap = self.get_intersection(tbl, units, tbl2, units2)
                if overlap > threshold:
                    result.append(
                        [
                            tbl2,
                            self.tbl_names[tbl2],
                            units2,
                            overlap,
                        ]
                    )
        return result
        # df = pd.DataFrame(
        #     data=result, columns=["tbl_id", "tbl_name", "attrs", "overlap"]
        # )

        # df = df.sort_values(by="overlap", ascending=False)
        # return df

    def get_intersection(self, tbl1, units1, tbl2, units2):
        col_names1 = self.get_col_names_with_granu(units1)
        col_names2 = self.get_col_names_with_granu(units2)
        query = sql.SQL(
            "select {fields1} from {tbl1} INTERSECT SELECT {fields2} from {tbl2}"
        ).format(
            fields1=sql.SQL(",").join([sql.Identifier(col) for col in col_names1]),
            tbl1=sql.Identifier(tbl1),
            fields2=sql.SQL(",").join([sql.Identifier(col) for col in col_names2]),
            tbl2=sql.Identifier(tbl2),
        )
        # print(self.cur.mogrify(query))
        self.cur.execute(query)
        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        ).dropna()
        return len(df)

    def search(self, tbl_id: str, attrs: List[str], granu_list):
        tbl_list = self.get_table_list()
        result = []
        for tbl_id2 in tbl_list:
            if tbl_id2 == tbl_id:
                continue
            ts_schemas = self.tbl_schemas[tbl_id2]
            if len(attrs) == 2:
                ts_schemas = ts_schemas["ts"]
            elif granu_list[0] in T_GRANU:
                ts_schemas = ts_schemas["t"]
            else:
                ts_schemas = ts_schemas["s"]

            for ts_schema in ts_schemas:
                # print(tbl_id, attrs, tbl_id2, ts_schema, granu_list)
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
        # print(attrs1, attrs2, granu_list)
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

    def get_col_names_with_granu(self, units: List[Unit]):
        group_by_names = []
        for unit in units:
            group_by_names.append(self.get_col_name(unit.attr_name, unit.granu))
        return group_by_names

    def transform(self, tbl: str, units: List[Unit], vars: List[Variable]):
        sql_str = """
        SELECT {fields}, {agg_stmts} FROM {tbl} GROUP BY {fields}
        """

        col_names = self.get_col_names_with_granu(units)
        query = sql.SQL(sql_str).format(
            fields=sql.SQL(",").join([sql.Identifier(col) for col in col_names]),
            agg_stmts=sql.SQL(",").join(
                [
                    sql.SQL(var.agg_func.name + "({}) as {}").format(
                        sql.Identifier(var.attr_name),
                        sql.Identifier(var.var_name),
                    )
                    for var in vars
                ]
            ),
            tbl=sql.Identifier(tbl),
        )
        # print(self.cur.mogrify(query))
        self.cur.execute(query, [var.agg_func.name for var in vars])
        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        )
        return df

    def aggregate_join_two_tables_avg(
        self,
        tbl1: str,
        attrs1: List[str],
        agg_attr1: str,
        tbl2: str,
        attrs2: List[str],
        agg_attr2: str,
        granu_list,
    ):
        col_names1, col_names2 = self.get_col_names(attrs1, attrs2, granu_list)
        agg_join_sql = """
        SELECT {a1_fields1}, a1.agg1, a2.agg2 FROM
        (SELECT {fields1}, AVG({agg_attr1}) as agg1 FROM {tbl1} GROUP BY {fields1}) a1
        FULL OUTER JOIN
        (SELECT {fields2}, AVG({agg_attr2}) as agg2 FROM {tbl2} GROUP BY {fields2}) a2
        ON 
        concat_ws(', ', {a1_fields1}) = concat_ws(', ', {a2_fields2})
        """

        query = sql.SQL(agg_join_sql).format(
            fields1=sql.SQL(",").join([sql.Identifier(col) for col in col_names1]),
            a1_fields1=sql.SQL(",").join(
                [sql.Identifier("a1", col) for col in col_names1]
            ),
            tbl1=sql.Identifier(tbl1),
            agg_attr1=sql.Identifier(agg_attr1),
            fields2=sql.SQL(",").join([sql.Identifier(col) for col in col_names2]),
            a2_fields2=sql.SQL(",").join(
                [sql.Identifier("a2", col) for col in col_names2]
            ),
            tbl2=sql.Identifier(tbl2),
            agg_attr2=sql.Identifier(agg_attr2),
        )

        self.cur.execute(query)

        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        ).dropna(subset=col_names1)

        df[["agg1", "agg2"]] = df[["agg1", "agg2"]].astype(float)
        return df.fillna(0)

    def aggregate_join_two_tables_avg_inner(
        self,
        tbl1: str,
        attrs1: List[str],
        agg_attr1: str,
        tbl2: str,
        attrs2: List[str],
        agg_attr2: str,
        granu_list,
    ):
        col_names1, col_names2 = self.get_col_names(attrs1, attrs2, granu_list)
        agg_join_sql = """
        SELECT {a1_fields1}, a1.agg1, a2.agg2 FROM
        (SELECT {fields1}, AVG({agg_attr1}) as agg1 FROM {tbl1} GROUP BY {fields1}) a1
        JOIN
        (SELECT {fields2}, AVG({agg_attr2}) as agg2 FROM {tbl2} GROUP BY {fields2}) a2
        ON 
        concat_ws(', ', {a1_fields1}) = concat_ws(', ', {a2_fields2})
        """

        query = sql.SQL(agg_join_sql).format(
            fields1=sql.SQL(",").join([sql.Identifier(col) for col in col_names1]),
            a1_fields1=sql.SQL(",").join(
                [sql.Identifier("a1", col) for col in col_names1]
            ),
            tbl1=sql.Identifier(tbl1),
            agg_attr1=sql.Identifier(agg_attr1),
            fields2=sql.SQL(",").join([sql.Identifier(col) for col in col_names2]),
            a2_fields2=sql.SQL(",").join(
                [sql.Identifier("a2", col) for col in col_names2]
            ),
            tbl2=sql.Identifier(tbl2),
            agg_attr2=sql.Identifier(agg_attr2),
        )

        self.cur.execute(query)

        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        ).dropna(subset=col_names1)

        df[["agg1", "agg2"]] = df[["agg1", "agg2"]].astype(float)
        return df

    def agg_join_with_filter():
        pass

    def aggregate_join_two_tables2(
        self,
        tbl1: str,
        units1: List[Unit],
        vars1: List[Variable],
        tbl2: str,
        units2: List[Unit],
        vars2: List[Variable],
    ):
        col_names1 = self.get_col_names_with_granu(units1)
        col_names2 = self.get_col_names_with_granu(units2)

        agg_join_sql = """
        SELECT {a1_fields1}, {agg_vars} FROM
        (SELECT {fields1}, {agg_stmts1} FROM {tbl1} GROUP BY {fields1}) a1
        JOIN
        (SELECT {fields2}, {agg_stmts2} FROM {tbl2} GROUP BY {fields2}) a2
        ON 
        concat_ws(', ', {a1_fields1}) = concat_ws(', ', {a2_fields2})
        """

        query = sql.SQL(agg_join_sql).format(
            fields1=sql.SQL(",").join([sql.Identifier(col) for col in col_names1]),
            agg_stmts1=sql.SQL(",").join(
                [
                    sql.SQL(var.agg_func.name + "(*) as {}").format(
                        sql.Identifier(var.var_name),
                    )
                    if var.attr_name == "*"
                    else sql.SQL(var.agg_func.name + "({}) as {}").format(
                        sql.Identifier(var.attr_name),
                        sql.Identifier(var.var_name),
                    )
                    for var in vars1
                ]
            ),
            tbl1=sql.Identifier(tbl1),
            a1_fields1=sql.SQL(",").join(
                [sql.Identifier("a1", col) for col in col_names1]
            ),
            fields2=sql.SQL(",").join([sql.Identifier(col) for col in col_names2]),
            agg_stmts2=sql.SQL(",").join(
                [
                    sql.SQL(var.agg_func.name + "(*) as {}").format(
                        sql.Identifier(var.var_name),
                    )
                    if var.attr_name == "*"
                    else sql.SQL(var.agg_func.name + "({}) as {}").format(
                        sql.Identifier(var.attr_name),
                        sql.Identifier(var.var_name),
                    )
                    for var in vars2
                ]
            ),
            tbl2=sql.Identifier(tbl2),
            a2_fields2=sql.SQL(",").join(
                [sql.Identifier("a2", col) for col in col_names2]
            ),
            agg_vars=sql.SQL(",").join(
                [sql.Identifier("a1", var.var_name) for var in vars1]
                + [sql.Identifier("a2", var.var_name) for var in vars2]
            ),
        )

        # print(self.cur.mogrify(query))

        self.cur.execute(query)

        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        ).dropna(subset=col_names1)
        return df

    def aggregate_join_two_tables_inner(
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
        try:
            self.cur.execute(query)
        except psycopg2.ProgrammingError as e:
            return None

        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        ).dropna(subset=col_names1)

        return df

    def aggregate_join_two_tables(
        self, tbl1: str, attrs1: List[str], tbl2: str, attrs2: List[str], granu_list
    ):
        col_names1, col_names2 = self.get_col_names(attrs1, attrs2, granu_list)
        agg_join_sql = """
        SELECT {a1_fields1}, a1.cnt1, a2.cnt2 FROM
        (SELECT {fields1}, COUNT(*) as cnt1 FROM {tbl1} GROUP BY {fields1}) a1
        FULL OUTER JOIN
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
        ).dropna(subset=col_names1)

        return df.fillna(0)

    def format_result(self, df, attr_len):
        # translate rows
        for i in range(attr_len):
            df.iloc[:, i] = df.iloc[:, i].apply(self.display_value)
        return df

    def display_value(self, v):
        tokens = v.split("-")
        tokens.reverse()
        return "-".join(tokens)
