import duckdb
import collections
import pandas as pd
from nexus.utils.data_model import SpatioTemporalKey, Variable
from typing import List, Dict
from nexus.db_connector.database_connecter import DatabaseConnectorInterface, IndexType


class DuckDBConnector(DatabaseConnectorInterface):
    def __init__(self, conn_str, read_only=True):
        self.cur = duckdb.connect(database=conn_str, read_only=read_only)

    def close(self):
        self.cur.close()

    def create_tbl(self, tbl_id: str, df: pd.DataFrame, mode='replace'):
        if mode == 'replace':
            self.cur.sql(f'CREATE OR REPLACE TABLE "{tbl_id}" AS SELECT * FROM df')
        elif mode == 'append':
            self.cur.sql(f'INSERT INTO "{tbl_id}" AS SELECT * FROM df')

    def delete_tbl(self, tbl_id: str):
        query = 'DROP TABLE IF EXISTS "{tbl_id}"'.format(tbl_id=tbl_id)
        self.cur.query(query)

    def create_aggregate_tbl(self, tbl_id: str, spatio_temporal_key: SpatioTemporalKey, variables: List[Variable]):
        col_names = spatio_temporal_key.get_col_names_with_granu()
        agg_tbl_name = "{}_{}".format(tbl_id, "_".join([col for col in col_names]))

        self.delete_tbl(agg_tbl_name)

        query = """
                   CREATE TABLE "{agg_tbl}" AS
                   SELECT CONCAT_WS(',', {fields}) as val, {agg_stmts} FROM "{tbl}" GROUP BY {fields}
                   HAVING {not_null_stmts}
                   """.format(
            agg_tbl=agg_tbl_name,
            fields=",".join([f'"{col}"' for col in col_names]),
            agg_stmts=",".join(
                [
                    "{func}(*) as {var_name}".format(
                        func=var.agg_func.name,
                        var_name=var.var_name,
                    )
                    if var.attr_name == "*"
                    else '{func}("{attr_name}") as "{var_name}"'.format(
                        func=var.agg_func.name,
                        attr_name=var.attr_name,
                        var_name=var.var_name,
                    )
                    for var in variables
                ]
            ),
            tbl=tbl_id,
            not_null_stmts=" AND ".join(
                [
                    "{} is not NULL".format(field)
                    for field in col_names
                ]
            ),
        )
        self.cur.sql(query)

        alter_type_sql = """
            ALTER TABLE "{agg_tbl}" ALTER val TYPE VARCHAR;
        """.format(
            agg_tbl=agg_tbl_name,
        )

        self.cur.sql(alter_type_sql)

        if len(agg_tbl_name) >= 63:
            idx_name = agg_tbl_name[:59] + "_idx"
        else:
            idx_name = agg_tbl_name + "_idx"

        self.create_indices_on_tbl(
            idx_name, agg_tbl_name, ["val"]
        )

        return agg_tbl_name

    def create_cnt_tbl_for_agg_tbl(self, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        idx_cnt_name = "{}_inv_cnt".format(spatio_temporal_key.get_idx_tbl_name())
        agg_tbl = spatio_temporal_key.get_agg_tbl_name(tbl_id)
        if len(agg_tbl) >= 63:
            cnt_tbl_name = agg_tbl[:59] + "_cnt"
        else:
            cnt_tbl_name = f"{agg_tbl}_cnt"
        self.delete_tbl(cnt_tbl_name)
        query = """
                CREATE TABLE "{cnt_tbl_name}" AS
                SELECT "inv"."val", cnt FROM "{inv_cnt}" inv JOIN "{tbl}" agg on inv."val" = agg."val" order by cnt desc
        """.format(
            cnt_tbl_name=cnt_tbl_name,
            inv_cnt=idx_cnt_name,
            tbl=agg_tbl)
        self.cur.sql(query)

    def create_indices_on_tbl(self, idx_name: str, tbl_id: str, col_names: List[str], mode=None):
        """
        duckdb only supports min-max index and  Adaptive Radix Tree (ART) index
        """
        query = """CREATE UNIQUE INDEX "{idx_name}" ON "{tbl}" ({cols});""".format(
            idx_name=idx_name,
            tbl=tbl_id,
            cols=",".join([col for col in col_names]),
        )
        self.cur.sql(query)

    def create_inv_index_tbl(self, inv_index_tbl):
        query = """
            CREATE TABLE IF NOT EXISTS {idx_tbl} (
                val TEXT UNIQUE,
                spatio_temporal_keys TEXT[]
            )
        """.format(idx_tbl=inv_index_tbl)
        self.cur.sql(query)

    def create_cnt_tbl_for_an_inverted_index(self, idx_name):
        tbl_name = f"{idx_name}_cnt"
        self.delete_tbl(tbl_name)
        query = """
                     CREATE TABLE "{tbl_name}" AS
                     SELECT val, array_length(spatio_temporal_keys, 1) as cnt from "{idx_name}"
                """.format(
            tbl_name=tbl_name,
            idx_name=idx_name
        )
        self.cur.execute(query)
        self.create_indices_on_tbl(tbl_name + "_i", tbl_name, ["val"], IndexType.HASH)

    def insert_spatio_temporal_key_to_inv_idx(self, inv_idx: str, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        def merge_lists(row):
            flag = pd.notna(row['spatio_temporal_keys_y'])
            if type(flag) is list and flag.all():
                return list(set(row['spatio_temporal_keys_x'] + row['spatio_temporal_keys_y']))
            elif type(flag) is bool and flag:
                return list(set(row['spatio_temporal_keys_x'] + row['spatio_temporal_keys_y']))
            else:
                return row['spatio_temporal_keys_x']

        agg_tbl = spatio_temporal_key.get_agg_tbl_name(tbl_id)
        print(agg_tbl)
        retrieval_query = """
            SELECT val, spatio_temporal_keys FROM "{inv_idx}" WHERE val IN (SELECT val FROM "{agg_tbl}")
        """.format(
            inv_idx=inv_idx,
            agg_tbl=agg_tbl
        )
        existing_lists = self.cur.sql(retrieval_query).df()

        retrieval_query = """
                  SELECT val, ARRAY[?] as spatio_temporal_keys FROM "{agg_tbl}"
              """.format(
            agg_tbl=agg_tbl,
        )
        all_lists = self.cur.execute(retrieval_query, [spatio_temporal_key.get_id(tbl_id)]).df()

        merged_df = pd.merge(all_lists, existing_lists, on='val', how='left')
        merged_df['spatio_temporal_keys'] = merged_df.apply(merge_lists, axis=1)
        merged_df = merged_df.drop(['spatio_temporal_keys_x', 'spatio_temporal_keys_y'], axis=1)

        delete_query = """
            DELETE FROM "{inv_idx}" WHERE val IN (SELECT val FROM "{agg_tbl}")
        """.format(
            inv_idx=inv_idx,
            agg_tbl=agg_tbl
        )
        self.cur.sql(delete_query)

        insert_query = """
            INSERT INTO "{inv_idx}" SELECT * FROM merged_df
        """.format(
            inv_idx=inv_idx
        )

        self.cur.sql(insert_query)

    def get_variable_stats(self, agg_tbl_name: str, var_name: str):
        query = """
                   select round(sum("{var}"), 4), round(sum("{var}"^2), 4), round(avg("{var}"), 4), 
                  round((count(*)-1)*var_samp("{var}"),4), count(*) from "{agg_tbl}";
        """.format(
            var=var_name,
            agg_tbl=agg_tbl_name
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
        query = """
            SELECT count(*) from "{tbl}";
           """.format(tbl=spatio_temporal_key.get_agg_tbl_name(tbl_id))

        self.cur.execute(query)
        return self.cur.fetchall()[0][0]

    def join_two_tables_on_spatio_temporal_keys(self, agg_tbl1: str, variables1: List[Variable],
                                                agg_tbl2: str, variables2: List[Variable],
                                                use_outer: bool = False):
        if not use_outer:
            agg_join_sql = """
                SELECT a1.val, {agg_vars} FROM
                "{agg_tbl1}" a1 JOIN "{agg_tbl2}" a2
                ON a1.val = a2.val
            """.format(
                agg_vars=",".join(
                    [
                        '{original_name} AS "{proj_name}"'.format(
                            original_name=f'a1."{var.var_name}"',
                            proj_name=var.proj_name,
                        )
                        for var in variables1
                    ]
                    + [
                        '{original_name} AS "{proj_name}"'.format(
                            original_name=f'a2."{var.var_name}"',
                            proj_name=var.proj_name,
                        )
                        for var in variables2
                    ]
                ),
                agg_tbl1=agg_tbl1,
                agg_tbl2=agg_tbl2,
            )
        else:
            agg_join_sql = """
            SELECT a1.val as key1, a2.val as key2, {agg_vars} FROM
            {agg_tbl1} a1 FULL JOIN {agg_tbl2} a2
            ON a1.val = a2.val
            """.format(
                agg_vars=",".join(
                    [
                        '{original_name} AS "{proj_name}"'.format(
                            original_name=f'a1."{var.var_name}"',
                            proj_name=var.proj_name,
                        )
                        for var in variables1
                    ]
                    + [
                        '{original_name} AS "{proj_name}"'.format(
                            original_name=f'a2."{var.var_name}"',
                            proj_name=var.proj_name,
                        )
                        for var in variables2
                    ]
                ),
                agg_tbl1=agg_tbl1,
                agg_tbl2=agg_tbl2,
            )

        return self.cur.execute(agg_join_sql).df(), agg_join_sql

    def join_multi_agg_tbls(self, tbl_cols: Dict[str, List[Variable]]):
        tbls = list(tbl_cols.keys())
        query = """
                    SELECT {attrs} FROM "{base_tbl}" {join_clauses}
                """.format(
            attrs=",".join([
                '{original_name} AS "{proj_name}"'.format(
                    original_name=f'"{tbl}"."{col.var_name}"',
                    proj_name=col.proj_name)
                for tbl, cols in tbl_cols.items() for col in cols
            ]),
            base_tbl=tbls[0],
            join_clauses=" ".join(
                ['INNER JOIN "{next_tbl}" ON "{tbl}".val = "{next_tbl}".val'.format(tbl=tbls[0], next_tbl=tbl)
                 for tbl in tbls[1:]]
            ),
        )
        return self.cur.sql(query).df().astype(float).round(3)

    def join_multi_vars(self, variables: List[Variable], constraints: Dict = {}):
        tbl_cols = collections.defaultdict(list)
        for var in variables:
            tbl_cols[var.tbl_id].append(var.attr_name)
        # join tbls and project attr names
        tbls = list(tbl_cols.keys())
        constaint_tbls = []
        constaint_vals = []
        if len(constraints.keys()) == 0:
            sql_str = 'SELECT {attrs} FROM "{base_tbl}" {join_clauses}'
        else:
            for tbl, threshold in constraints.items():
                constaint_tbls.append(tbl)
                constaint_vals.append(threshold)
            sql_str = 'SELECT {attrs} FROM "{base_tbl}" {join_clauses} WHERE {filter}'
        query = sql_str.format(
            attrs=','.join([f'"{tbl}".{col}' for tbl, cols in tbl_cols.items() for col in cols]),
                        #    + [f'"{tbl}".count AS "{tbl}_samples"' for tbl in tbl_cols.keys()]),
            base_tbl=tbls[0],
            join_clauses=' '.join(
                ['INNER JOIN "{next_tbl}" ON "{tbl}".val = "{next_tbl}".val'.format(tbl=tbls[0], next_tbl=tbl) for
                 tbl in tbls[1:]]
            ),
            filter=" AND ".join(
                ['{col} >= {threshold}'.format(col=f'"{tbl}".count', threshold=threshold)
                 for tbl, threshold in constraints.items()]
            )
        )

        return self.cur.sql(query).df(), query

    def read_agg_tbl(self, agg_tbl: str, variables: List[Variable] = []):
        if len(variables) == 0:
            sql_str = """
               SELECT * FROM "{agg_tbl}";
           """
        else:
            sql_str = """
                   SELECT val, {agg_vars} FROM "{agg_tbl}";
               """

        query = sql_str.format(
            agg_vars=",".join([f"{var.var_name}" for var in variables]),
            agg_tbl=agg_tbl
        )

        return self.cur.sql(query).df().astype(float).round(3)

    def get_total_row_to_read_and_max_joinable_tables(self, tbl_id, spatio_temporal_key: SpatioTemporalKey,
                                                      threshold: int):
        agg_name = spatio_temporal_key.get_agg_tbl_name(tbl_id)
        if len(agg_name) >= 63:
            agg_cnt_tbl = agg_name[:59] + "_cnt"
        else:
            agg_cnt_tbl = agg_name + "_cnt"

        query = """
               SELECT count(cnt), sum(cnt) FROM "{inv_cnt}"
           """.format(
            inv_cnt=agg_cnt_tbl
        )

        res = self.cur.sql(query).fetchone()
        total_lists, total_elements = res[0], res[1]

        max_joinable_tbls = (total_elements - total_lists) // threshold

        return total_elements, max_joinable_tbls

    def estimate_joinable_candidates(
            self, tbl_id, spatio_temporal_key: SpatioTemporalKey, threshold: int, rows_to_sample: int = 0
    ):
        if rows_to_sample < 1:
            rows_to_sample = 0
        inv_idx_name = "{}_inv".format(spatio_temporal_key.get_idx_tbl_name())
        sql_str = """
            SELECT cand, count(*) as cnt
            FROM( 
                SELECT unnest("spatio_temporal_keys") as cand FROM "{inv_idx}" inv JOIN "{agg_tbl}" agg ON inv."val" = agg."val"
            ) subquery
            GROUP BY cand
        """

        if rows_to_sample > 0:
            sql_str = """
            WITH sampled_table AS (
                 SELECT "val" FROM "{agg_tbl}" limit ?
            )
            SELECT cand, count(*) as cnt
            FROM(
                SELECT unnest("spatio_temporal_keys") as cand FROM "{inv_idx}" inv where inv."val" in (SELECT "val" from sampled_table)
            ) subquery
            GROUP BY cand
            """
        query = sql_str.format(
            inv_idx=inv_idx_name,
            agg_tbl=f"{spatio_temporal_key.get_agg_tbl_name(tbl_id)}_cnt",
        )
        if rows_to_sample == 0:
            query_res = self.cur.sql(query).df()
        else:
            print("rows_to_sample", rows_to_sample)
            query_res = self.cur.execute(query, [rows_to_sample]).df()

        result = []
        sampled_cnt = 0
        for _, row in query_res.iterrows():
            cand, overlap = tuple(row['cand'].split(",")), row['cnt']
            tbl2_id = cand[0]
            if tbl2_id == tbl_id:
                continue
            sampled_cnt += overlap
            if rows_to_sample == 0 and overlap < threshold:
                continue
            candidate_spatio_temporal_key = spatio_temporal_key.from_attr_names(cand[1:])
            result.append([tbl2_id, candidate_spatio_temporal_key, overlap])
        if rows_to_sample >= 0:
            return result, sampled_cnt
        return result, 0
