from config import ATTR_PATH
import utils.io_utils as io_utils
import pandas as pd
from typing import List
from psycopg2 import sql
import itertools
import psycopg2
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
        self.tbl_list = list(self.tbl_attrs.keys())

    def load_meta_data(self):
        self.tbl_lookup = {}
        self.tbl_attrs = io_utils.load_json(ATTR_PATH)

        for tbl_id, info in self.tbl_attrs.items():
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

    def find_augmentable_tables(self, tbl: str, units: List[Unit], threshold, mode):
        if mode == "agg_idx":
            return self.find_augmentable_tables_agg_idx(tbl, units, threshold)
        elif mode == "multi_idx":
            return self.find_augmentable_tables_multi_idx(tbl, units, threshold)

    def find_augmentable_tables_agg_idx(self, tbl: str, units: List[Unit], threshold):
        return self.get_intersection_agg_idx(tbl, units, threshold)

    def find_augmentable_tables_multi_idx(self, tbl: str, units: List[Unit], threshold):
        t_granu, s_granu = None, None
        for unit in units:
            if unit.granu in T_GRANU:
                t_granu = unit.granu
            elif unit.granu in S_GRANU:
                s_granu = unit.granu

        result = []
        for tbl2 in self.tbl_list:
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
                overlap = self.get_intersection_multi_idx(
                    tbl, units, tbl2, units2, threshold
                )
                if overlap == threshold:
                    result.append(
                        [
                            tbl2,
                            self.tbl_names[tbl2],
                            units2,
                            overlap,
                        ]
                    )
        return result

    def get_intersection_agg_idx(self, tbl, units, threshold):
        # query aggregated index tables to find joinable tables
        if len(units) == 1:
            if units[0].granu in T_GRANU:
                idx_tbl = "time_{}".format(units[0].granu.value)
            elif units[0].granu in S_GRANU:
                idx_tbl = "space_{}".format(units[0].granu.value)
        else:
            idx_tbl = "time_space_" + "_".join(
                [str(unit.granu.value) for unit in units]
            )
        query = sql.SQL(
            "select tbl_id, {attrs}, count(*) from {idx_tbl} \
                            where tbl_id != %s and ({values}) in \
                            (select {values} from {idx_tbl} where tbl_id = %s and {filter_stmts}) \
                            group by tbl_id, {attrs} \
                            having count(*) >= %s"
        ).format(
            attrs=sql.SQL(",").join(
                [sql.Identifier(unit.get_type()) for unit in units]
            ),
            values=sql.SQL(",").join(
                [sql.Identifier(unit.get_val()) for unit in units]
            ),
            idx_tbl=sql.Identifier(idx_tbl),
            filter_stmts1=sql.SQL(" AND").join(
                [
                    sql.SQL("{} != %s").format(sql.Identifier(unit.get_type()))
                    for unit in units
                ]
            ),
            filter_stmts=sql.SQL(" AND").join(
                [
                    sql.SQL("{} = %s").format(sql.Identifier(unit.get_type()))
                    for unit in units
                ]
            ),
        )
        # print(self.cur.mogrify(query))
        self.cur.execute(
            query,
            [tbl] + [tbl] + [unit.attr_name for unit in units] + [threshold],
        )
        query_res = self.cur.fetchall()
        result = []
        for row in query_res:
            tbl2_id = row[0]
            tbl2_name = self.tbl_names[tbl2_id]
            units2 = [Unit(attr, units[i].granu) for i, attr in enumerate(row[1:-1])]
            overlap = int(row[-1])
            result.append([tbl2_id, tbl2_name, units2, overlap])
        return result

    def get_intersection_multi_idx(self, tbl1, units1, tbl2, units2, threshold):
        # query index table of each table to find joinable tables
        col_names1 = self.get_col_names_with_granu(units1)
        col_names2 = self.get_col_names_with_granu(units2)
        # query = sql.SQL(
        #     "select {fields1} from {tbl1_idx} INTERSECT SELECT {fields2} from {tbl2_idx} LIMIT %s"
        # ).format(
        #     fields1=sql.SQL(",").join([sql.Identifier(col) for col in col_names1]),
        #     tbl1_idx=sql.Identifier(tbl1 + "_" + "_".join(col_names1)),
        #     fields2=sql.SQL(",").join([sql.Identifier(col) for col in col_names2]),
        #     tbl2_idx=sql.Identifier(tbl2 + "_" + "_".join(col_names2)),
        # )
        query = sql.SQL(
            "select {fields1} from {tbl1_idx} where ({fields1}) in (select {fields2} from {tbl2_idx}) LIMIT %s"
        ).format(
            fields1=sql.SQL(",").join([sql.Identifier(col) for col in col_names1]),
            tbl1_idx=sql.Identifier(tbl1 + "_" + "_".join(col_names1)),
            fields2=sql.SQL(",").join([sql.Identifier(col) for col in col_names2]),
            tbl2_idx=sql.Identifier(tbl2 + "_" + "_".join(col_names2)),
        )
        # print(self.cur.mogrify(query))
        self.cur.execute(query, [threshold])
        query_res = self.cur.fetchall()
        return len(query_res)

    def _get_intersection(self, tbl1, units1, tbl2, units2, threhold):
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
        return [unit.to_int_name() for unit in units]

    def transform(self, tbl: str, units: List[Unit], vars: List[Variable]):
        sql_str = """
        SELECT {fields}, {agg_stmts} FROM {tbl} GROUP BY {fields}
        """

        col_names = self.get_col_names_with_granu(units)
        query = sql.SQL(sql_str).format(
            fields=sql.SQL(",").join([sql.Identifier(col) for col in col_names]),
            agg_stmts=sql.SQL(",").join(
                [
                    sql.SQL(var.agg_func.name + "(*) as {}").format(
                        sql.Identifier(var.var_name),
                    )
                    if var.attr_name == "*"
                    else sql.SQL(var.agg_func.name + "({}) as {}").format(
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

    def aggregate_join_two_tables(
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

    def format_result(self, df, attr_len):
        # translate rows
        for i in range(attr_len):
            df.iloc[:, i] = df.iloc[:, i].apply(self.display_value)
        return df

    def display_value(self, v):
        tokens = v.split("-")
        tokens.reverse()
        return "-".join(tokens)
