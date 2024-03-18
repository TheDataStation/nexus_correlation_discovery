import pandas as pd
from typing import List
from psycopg2 import sql
import itertools
import psycopg2
from nexus.utils.coordinate import SPATIAL_GRANU
from nexus.utils.time_point import TEMPORAL_GRANU
from nexus.utils.data_model import Attr, Variable, SpatioTemporalKey


class DBSearch:
    def __init__(self, conn_string) -> None:
        self.tbl_schemas = {}
        self.tbl_names = {}
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        self.cur = conn.cursor()
        # self.load_meta_data()
        # self.tbl_list = list(self.tbl_attrs.keys())

    def load_meta_data(self):
        self.tbl_lookup = {}
        # self.tbl_attrs = io_utils.load_json(ATTR_PATH)

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

    def find_augmentable_st_schemas(
        self, tbl: str, st_schema: SpatioTemporalKey, threshold, mode
    ):
        if mode == "inv_idx":
            return estimate_joinable_candidates(self.cur, tbl, st_schema, threshold)
        elif mode == "agg_idx":
            return self.find_augmentable_tables_agg_idx(tbl, units, threshold)
        elif mode == "multi_idx":
            return self.find_augmentable_tables_multi_idx(tbl, units, threshold)

    def find_augmentable_tables_agg_idx(self, tbl: str, units: List[Attr], threshold):
        return self.get_intersection_agg_idx(tbl, units, threshold)

    def find_augmentable_tables_multi_idx(self, tbl: str, units: List[Attr], threshold):
        t_granu, s_granu = None, None
        for unit in units:
            if unit.granu in TEMPORAL_GRANU:
                t_granu = unit.granu
            elif unit.granu in SPATIAL_GRANU:
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
                    [Attr(attrs[0], t_granu), Attr(attrs[1], s_granu)]
                    for attrs in align_schemas["ts"]
                ]
            elif units[0].granu in TEMPORAL_GRANU:
                units2_list = [
                    [Attr(attrs[0], t_granu)] for attrs in align_schemas["t"]
                ]
            else:
                units2_list = [
                    [Attr(attrs[0], s_granu)] for attrs in align_schemas["s"]
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
            if units[0].granu in TEMPORAL_GRANU:
                idx_tbl = "time_{}".format(units[0].granu.value)
            elif units[0].granu in SPATIAL_GRANU:
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
        # print(
        #     self.cur.mogrify(
        #         query, [tbl] + [tbl] + [unit.attr_name for unit in units] + [threshold]
        #     )
        # )
        self.cur.execute(
            query,
            [tbl] + [tbl] + [unit.name for unit in units] + [threshold],
        )
        query_res = self.cur.fetchall()
        result = []
        for row in query_res:
            tbl2_id = row[0]
            tbl2_name = self.tbl_names[tbl2_id]
            units2 = [Attr(attr, units[i].granu) for i, attr in enumerate(row[1:-1])]
            overlap = int(row[-1])
            result.append([tbl2_id, tbl2_name, units2, overlap])
        return result

    def get_intersection_multi_idx(self, tbl1, units1, tbl2, units2, threshold):
        # query index table of each table to find joinable tables
        col_names1 = self.get_col_names_with_granu(units1)
        col_names2 = self.get_col_names_with_granu(units2)

        query = sql.SQL(
            "select {fields1} from {tbl1_idx} where ({fields1}) in (select {fields2} from {tbl2_idx}) LIMIT %s"
        ).format(
            fields1=sql.SQL(",").join([sql.Identifier(col) for col in col_names1]),
            tbl1_idx=sql.Identifier(tbl1 + "_" + "_".join(col_names1)),
            fields2=sql.SQL(",").join([sql.Identifier(col) for col in col_names2]),
            tbl2_idx=sql.Identifier(tbl2 + "_" + "_".join(col_names2)),
        )
        self.cur.execute(query, [threshold])
        query_res = self.cur.fetchall()
        return query_res
        # print(self.cur.mogrify(query))

    def __get_intersection(self, tbl1, units1, tbl2, units2, threhold):
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

    def get_col_names_with_granu(self, units: List[Attr]):
        return [unit.to_int_name() for unit in units]

    def transform(self, tbl: str, units: List[Attr], vars: List[Variable]):
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

    def align_two_two_tables(
        self,
        tbl1: str,
        units1: List[Attr],
        vars1: List[Variable],
        tbl2: str,
        units2: List[Attr],
        vars2: List[Variable],
    ):
        # calculate intersecting units and store them in a temp table
        # self.get_intersection_multi_idx(tbl1, units1, tbl2, units2)

        col_names1 = self.get_col_names_with_granu(units1)
        col_names2 = self.get_col_names_with_granu(units2)

        # agg_sql = """
        # SELECT {fields}, {agg_stmts} FROM {tbl}
        #     WHERE ({fields}) IN (select * FROM intersected_values)
        #     GROUP BY {fields}
        #     ORDER BY {fields}
        # """

        agg_join_sql = """
        WITH intersected as (select {fields1} from {tbl1_idx} intersect (select {fields2} from {tbl2_idx}))
        SELECT {a1_fields1}, {agg_vars} FROM
            (SELECT {fields}, {agg_stmts1} FROM {tbl1}
            JOIN intersected ON {join_cond1}
            GROUP BY {fields} ORDER BY {fields}) a1
            JOIN
            (SELECT {fields}, {agg_stmts2} FROM {tbl2}
            JOIN intersected ON {join_cond2}
            GROUP BY {fields} ORDER BY {fields}) a2
            ON {join_cond}
        """

        query = sql.SQL(agg_join_sql).format(
            fields=sql.SQL(",").join(
                [sql.Identifier("intersected", col) for col in col_names1]
            ),
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
            tbl1_idx=sql.Identifier(tbl1 + "_" + "_".join(col_names1)),
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
            tbl2_idx=sql.Identifier(tbl2 + "_" + "_".join(col_names2)),
            agg_vars=sql.SQL(",").join(
                [sql.Identifier("a1", var.var_name) for var in vars1]
                + [sql.Identifier("a2", var.var_name) for var in vars2]
            ),
            join_cond1=sql.SQL(" AND ").join(
                [
                    sql.SQL("{} = {}").format(
                        sql.Identifier(tbl1, col_names1[i]),
                        sql.Identifier("intersected", col_names1[i]),
                    )
                    for i in range(len(col_names1))
                ]
            ),
            join_cond2=sql.SQL(" AND ").join(
                [
                    sql.SQL("{} = {}").format(
                        sql.Identifier(tbl2, col_names2[i]),
                        sql.Identifier("intersected", col_names1[i]),
                    )
                    for i in range(len(col_names1))
                ]
            ),
            join_cond=sql.SQL(" AND ").join(
                [
                    sql.SQL("{} = {}").format(
                        sql.Identifier("a1", col_names1[i]),
                        sql.Identifier("a2", col_names1[i]),
                    )
                    for i in range(len(col_names1))
                ]
            ),
        )

        self.cur.execute(query)
        # print(self.cur.mogrify(query))
        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        )
        return df

    def create_tmp_agg_tbl(
        self,
        tbl: str,
        units: List[Attr],
        variables: List[Variable],
    ):
        sql_str = """
        CREATE TEMPORARY TABLE {tmp_tbl} AS
        SELECT {fields}, {agg_stmts} FROM {tbl} GROUP BY {fields}
        """

        col_names = self.get_col_names_with_granu(units)
        query = sql.SQL(sql_str).format(
            tmp_tbl=sql.Identifier(
                "_{}_{}".format(tbl, "_".join([col for col in col_names]))
            ),
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
                    for var in variables
                ]
            ),
            tbl=sql.Identifier(tbl),
        )

        self.cur.execute(query)

        analyze_sql = "ANALYZE {tmp_tbl};"
        query = sql.SQL(analyze_sql).format(
            tmp_tbl=sql.Identifier(
                "_{}_{}".format(tbl, "_".join([col for col in col_names]))
            )
        )
        self.cur.execute(query)

    def aggregate_join_two_tables_using_tmp(
        self,
        tbl1: str,
        units1: List[Attr],
        vars1: List[Variable],
        tbl2: str,
        units2: List[Attr],
        vars2: List[Variable],
    ):
        col_names1 = self.get_col_names_with_granu(units1)
        col_names2 = self.get_col_names_with_granu(units2)

        agg_join_sql = """
        SELECT {a1_fields1}, {agg_vars} FROM
        {tmp_tbl1} a1 JOIN {tmp_tbl2} a2
        ON {join_cond}
        """

        query = sql.SQL(agg_join_sql).format(
            a1_fields1=sql.SQL(",").join(
                [sql.Identifier("a1", col) for col in col_names1]
            ),
            agg_vars=sql.SQL(",").join(
                [
                    sql.SQL("{} AS {}").format(
                        sql.Identifier("a1", var.var_name[:-3]),
                        sql.Identifier(var.var_name),
                    )
                    for var in vars1
                ]
                + [
                    sql.SQL("{} AS {}").format(
                        sql.Identifier("a2", var.var_name[:-3]),
                        sql.Identifier(var.var_name),
                    )
                    for var in vars2
                ]
            ),
            tmp_tbl1=sql.Identifier(
                "_{}_{}".format(tbl1, "_".join([col for col in col_names1]))
            ),
            tmp_tbl2=sql.Identifier(
                "_{}_{}".format(tbl2, "_".join([col for col in col_names2]))
            ),
            join_cond=sql.SQL(" AND ").join(
                [
                    sql.SQL("{} = {}").format(
                        sql.Identifier("a1", col_names1[i]),
                        sql.Identifier("a2", col_names2[i]),
                    )
                    for i in range(len(col_names1))
                ]
            ),
        )

        self.cur.execute(query)

        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        ).dropna(
            subset=col_names1
        )  # drop empty keys
        return df

    def aggregate_join_two_tables(
        self,
        tbl1: str,
        units1: List[Attr],
        vars1: List[Variable],
        tbl2: str,
        units2: List[Attr],
        vars2: List[Variable],
    ):
        col_names1 = self.get_col_names_with_granu(units1)
        col_names2 = self.get_col_names_with_granu(units2)

        agg_join_sql = """
        SELECT {a1_fields1}, {agg_vars} FROM
        (SELECT {fields1}, {agg_stmts1} FROM {tbl1} GROUP BY {fields1}) a1
        JOIN
        (SELECT {fields2}, {agg_stmts2} FROM {tbl2} GROUP BY {fields2}) a2
        ON {join_cond}
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
            join_cond=sql.SQL(" AND ").join(
                [
                    sql.SQL("{} = {}").format(
                        sql.Identifier("a1", col_names1[i]),
                        sql.Identifier("a2", col_names2[i]),
                    )
                    for i in range(len(col_names1))
                ]
            ),
        )

        # print(self.cur.mogrify(query))

        self.cur.execute(query)

        df = pd.DataFrame(
            self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description]
        ).dropna(
            subset=col_names1
        )  # drop empty keys
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
