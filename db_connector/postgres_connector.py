import pandas as pd
import collections
from sqlalchemy import create_engine
import psycopg2
from io import StringIO
from utils.data_model import Table, SpatioTemporalKey, Variable
from typing import List, Dict
from psycopg2 import sql
from db_connector.database_connecter import DatabaseConnectorInterface, IndexType


class PostgresConnector(DatabaseConnectorInterface):
    def __init__(self, conn_str):
        db = create_engine(conn_str)
        self.conn = db.connect()
        conn_copg2 = psycopg2.connect(conn_str)
        conn_copg2.autocommit = True
        self.cur = conn_copg2.cursor()

    def _copy_from_dataFile_StringIO(self, df, tbl_name):
        copy_sql = f"""
                    COPY "{tbl_name}"
                    FROM STDIN
                    DELIMITER ',' 
                    CSV HEADER
                """

        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        self.cur.copy_expert(copy_sql, buffer)

    def create_tbl(self, tbl_id: str, df: pd.DataFrame, mode='replace'):
        # create the table if not exists
        schema = df.iloc[:0]
        schema.to_sql(
            tbl_id,
            con=self.conn,
            if_exists=mode,
            index=False,
        )
        # ingest data
        self._copy_from_dataFile_StringIO(df, tbl_id)

    def delete_tbl(self, tbl_id: str):
        sql_str = """DROP TABLE IF EXISTS {tbl}"""
        self.cur.execute(sql.SQL(sql_str).format(tbl=sql.Identifier(tbl_id)))

    def create_aggregate_tbl(self, tbl_id: str, spatio_temporal_key: SpatioTemporalKey, variables: List[Variable]):
        col_names = spatio_temporal_key.get_col_names_with_granu()
        agg_tbl_name = "{}_{}".format(tbl_id, "_".join([col for col in col_names]))

        self.delete_tbl(agg_tbl_name)

        sql_str = """
                CREATE TABLE {agg_tbl} AS
                SELECT CONCAT_WS(',', {fields}) as val, {agg_stmts} FROM {tbl} GROUP BY {fields}
                HAVING {not_null_stmts}
                """

        query = sql.SQL(sql_str).format(
            agg_tbl=sql.Identifier(agg_tbl_name),
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
            tbl=sql.Identifier(tbl_id),
            not_null_stmts=sql.SQL(" AND ").join(
                [
                    sql.SQL("{} is not NULL").format(sql.Identifier(field))
                    for field in col_names
                ]
            ),
        )

        self.cur.execute(query)
        if len(agg_tbl_name) >= 63:
            idx_name = agg_tbl_name[:59] + "_idx"
        else:
            idx_name = agg_tbl_name + "_idx"

        self.create_indices_on_tbl(
            idx_name, agg_tbl_name, ["val"], IndexType.HASH
        )

        return agg_tbl_name

    def create_indices_on_tbl(self, idx_name: str, tbl_id: str, col_names: List[str], mode=IndexType.B_TREE):
        if mode == IndexType.B_TREE:
            sql_str = """
                    CREATE INDEX {idx_name} ON {tbl} ({cols});
                """

            query = sql.SQL(sql_str).format(
                idx_name=sql.Identifier(idx_name),
                tbl=sql.Identifier(tbl_id),
                cols=sql.SQL(",").join([sql.Identifier(col) for col in col_names]),
            )
        elif mode == IndexType.HASH:
            # hash index can only be created on a single column in postgres
            col_name = col_names[0]

            sql_str = """
                    CREATE INDEX {idx_name} ON {tbl} using hash({col});
                """

            query = sql.SQL(sql_str).format(
                idx_name=sql.Identifier(idx_name),
                tbl=sql.Identifier(tbl_id),
                col=sql.Identifier(col_name),
            )
        # print(cur.mogrify(query))
        self.cur.execute(query)

    def create_inv_index_tbl(self, inv_index_tbl):
        sql_str = """
            CREATE TABLE IF NOT EXISTS {idx_tbl} (
                val text UNIQUE,
                spatio_temporal_keys _text
            )
        """
        query = sql.SQL(sql_str).format(idx_tbl=sql.Identifier(inv_index_tbl))
        self.cur.execute(query)

    def insert_spatio_temporal_key_to_inv_idx(self, inv_idx: str, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        sql_str = """
            INSERT INTO {inv_idx} (val, spatio_temporal_keys)
            SELECT val, ARRAY[%s] as spatio_temporal_keys FROM {agg_tbl}
            ON CONFLICT (val) 
            DO
                UPDATE SET spatio_temporal_keys = (SELECT array_agg(distinct x)
                                             FROM unnest({original} || EXCLUDED.spatio_temporal_keys) as t(x));
        """
        query = sql.SQL(sql_str).format(
            inv_idx=sql.Identifier(inv_idx),
            agg_tbl=sql.Identifier(spatio_temporal_key.get_agg_tbl_name(tbl_id)),
            original=sql.Identifier(inv_idx, "spatio_temporal_keys"),
        )

        self.cur.execute(query, (spatio_temporal_key.get_id(tbl_id),))

    def create_cnt_tbl_for_inverted_indices(self, idx_names):
        for idx_name in idx_names:
            tbl_name = f"{idx_name}_cnt"
            self.delete_tbl(tbl_name)
            sql_str = """
                 CREATE TABLE {tbl_name} AS
                 SELECT val, array_length(spatio_temporal_keys, 1) as cnt from {idx_name}
            """
            query = sql.SQL(sql_str).format(
                tbl_name=sql.Identifier(tbl_name), idx_name=sql.Identifier(idx_name)
            )
            self.cur.execute(query)

            self.create_indices_on_tbl(tbl_name + "_i", tbl_name, ["val"], IndexType.HASH)

    def create_cnt_tbl_for_agg_tbl(self, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        idx_cnt_name = "{}_inv_cnt".format(spatio_temporal_key.get_idx_tbl_name())
        agg_tbl = spatio_temporal_key.get_agg_tbl_name(tbl_id)
        if len(agg_tbl) >= 63:
            cnt_tbl_name = agg_tbl[:59] + "_cnt"
        else:
            cnt_tbl_name = f"{agg_tbl}_cnt"
        self.delete_tbl(cnt_tbl_name)
        sql_str = """
                CREATE TABLE {cnt_tbl_name} AS
                SELECT "inv"."val", cnt FROM {inv_cnt} inv JOIN {tbl} agg on inv."val" = agg."val" order by cnt desc
            """
        query = sql.SQL(sql_str).format(
            cnt_tbl_name=sql.Identifier(cnt_tbl_name),
            inv_cnt=sql.Identifier(idx_cnt_name),
            tbl=sql.Identifier(agg_tbl),
        )

        self.cur.execute(query)

    def get_variable_stats(self, agg_tbl_name: str, var_name: str):
        sql_str = """
            select sum({var}), sum({var}^2), round(avg({var})::numeric, 4), round((count(*)-1)*var_samp({var})::numeric,4), count(*)from {agg_tbl};
        """
        query = sql.SQL(sql_str).format(
            var=sql.Identifier(var_name), agg_tbl=sql.Identifier(agg_tbl_name)
        )
        self.cur.execute(query)
        query_res = self.cur.fetchall()[0]
        return {
            "sum": float(query_res[0]) if query_res[0] is not None else None,
            "sum_square": float(query_res[1]) if query_res[1] is not None else None,
            "avg": float(query_res[2]) if query_res[2] is not None else None,
            "res_sum": float(query_res[3]) if query_res[3] is not None else None,
            "cnt": int(query_res[4]) if query_res[4] is not None else None,
        }

    def get_row_cnt(self, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        sql_str = """
            SELECT count(*) from {tbl};
           """
        query = sql.SQL(sql_str).format(
            tbl=sql.Identifier(spatio_temporal_key.get_agg_tbl_name(tbl_id))
        )
        self.cur.execute(query)
        return self.cur.fetchall()[0][0]

    def join_two_tables_on_spatio_temporal_keys(self, agg_tbl1: str, variables1: List[Variable],
                                                agg_tbl2: str, variables2: List[Variable],
                                                use_outer: bool = False):
        agg_join_sql = """
            SELECT a1.val, {agg_vars} FROM
            {agg_tbl1} a1 JOIN {agg_tbl2} a2
            ON a1.val = a2.val
        """
        if use_outer:
            agg_join_sql = """
            SELECT a1.val as key1, a2.val as key2, {agg_vars} FROM
            {agg_tbl1} a1 FULL JOIN {agg_tbl2} a2
            ON a1.val = a2.val
            """

        query = sql.SQL(agg_join_sql).format(
            agg_vars=sql.SQL(",").join(
                [
                    sql.SQL("{} AS {}").format(
                        sql.Identifier("a1", var.var_name),
                        sql.Identifier(var.proj_name),
                    )
                    for var in variables1
                ]
                + [
                    sql.SQL("{} AS {}").format(
                        sql.Identifier("a2", var.var_name),
                        sql.Identifier(var.proj_name),
                    )
                    for var in variables2
                ]
            ),
            agg_tbl1=sql.Identifier(agg_tbl1),
            agg_tbl2=sql.Identifier(agg_tbl2),
        )

        self.cur.execute(query)

        df = pd.DataFrame(self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description])
        return df, self.cur.mogrify(query)

    def join_multi_agg_tbls(self, tbl_cols: Dict[str, List[Variable]]):
        tbls = list(tbl_cols.keys())
        sql_str = "SELECT {attrs} FROM {base_tbl} {join_clauses}"
        query = sql.SQL(sql_str).format(
            attrs=sql.SQL(",").join([
                sql.SQL("{} AS {}").format(sql.Identifier(tbl, col.var_name), sql.Identifier(col.proj_name))
                for tbl, cols in tbl_cols.items() for col in cols
            ]),
            base_tbl=sql.Identifier(tbls[0]),
            join_clauses=sql.SQL(" ").join(
                [sql.SQL("INNER JOIN {next_tbl} ON {tbl}.val = {next_tbl}.val").format(tbl=sql.Identifier(tbls[0]),
                                                                                       next_tbl=sql.Identifier(tbl)) for
                 tbl in tbls[1:]]
            ),
        )
        self.cur.execute(query)
        df = pd.DataFrame(self.cur.fetchall(), columns=[desc[0] for desc in cur.description])
        return df.astype(float).round(3)

    def join_multi_vars(self, variables: List[Variable], constraints=None):
        tbl_cols = collections.defaultdict(list)
        for var in variables:
            tbl_cols[var.tbl_id].append(var.attr_name)
        # join tbls and project attr names
        tbls = list(tbl_cols.keys())
        constaint_tbls = []
        constaint_vals = []
        if not constraints:
            sql_str = "SELECT {attrs} FROM {base_tbl} {join_clauses}"
        else:
            for tbl, threshold in constraints.items():
                constaint_tbls.append(tbl)
                constaint_vals.append(threshold)
            sql_str = "SELECT {attrs} FROM {base_tbl} {join_clauses} WHERE {filter}"
        query = sql.SQL(sql_str).format(
            attrs=sql.SQL(",").join([
                                        sql.SQL("{}").format(sql.Identifier(tbl, col))
                                        for tbl, cols in tbl_cols.items() for col in cols
                                    ] + [sql.SQL("{} AS {}").format(sql.Identifier(tbl, 'count'),
                                                                    sql.Identifier(f'{tbl}_samples')) for tbl in
                                         tbl_cols.keys()]),
            base_tbl=sql.Identifier(tbls[0]),
            join_clauses=sql.SQL(" ").join(
                [sql.SQL("INNER JOIN {next_tbl} ON {tbl}.val = {next_tbl}.val").format(tbl=sql.Identifier(tbls[0]),
                                                                                       next_tbl=sql.Identifier(tbl)) for
                 tbl in tbls[1:]]
            ),
            filter=sql.SQL(" AND ").join(
                [sql.SQL("{col} >= %s").format(col=sql.Identifier(tbl, 'count')) for tbl in constaint_tbls]
            )
        )
        if not constraints:
            self.cur.execute(query)
        else:
            self.cur.execute(query, constaint_vals)
        df = pd.DataFrame(self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description])
        return df, self.cur.mogrify(query, constaint_vals)

    def read_agg_tbl(self, agg_tbl: str, variables: List[Variable] = []):
        if len(variables) == 0:
            sql_str = """
            SELECT * FROM {agg_tbl};
        """
        else:
            sql_str = """
                SELECT val, {agg_vars} FROM {agg_tbl};
            """

        query = sql.SQL(sql_str).format(
            agg_vars=sql.SQL(",").join(
                [
                    sql.SQL("{}").format(
                        sql.Identifier(var.var_name),
                    )
                    for var in variables
                ]),
            agg_tbl=sql.Identifier(agg_tbl)
        )

        self.cur.execute(query)

        df = pd.DataFrame(self.cur.fetchall(), columns=[desc[0] for desc in self.cur.description])
        return df.astype(float).round(3)
