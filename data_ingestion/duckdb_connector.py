import duckdb
import pandas as pd
from utils.data_model import SpatioTemporalKey, Variable
from typing import List
from data_ingestion.database_connecter import DatabaseConnectorInterface, IndexType


class DuckDBConnector(DatabaseConnectorInterface):
    def __init__(self, conn_str):
        self.cur = duckdb.connect(database=conn_str)

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
            fields=",".join([col for col in col_names]),
            agg_stmts=",".join(
                [
                    "{func}(*) as {var_name}".format(
                        func=var.agg_func.name,
                        var_name=var.var_name,
                    )
                    if var.attr_name == "*"
                    else "{func}({attr_name}) as {var_name}".format(
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

        if len(agg_tbl_name) >= 63:
            idx_name = agg_tbl_name[:59] + "_idx"
        else:
            idx_name = agg_tbl_name + "_idx"

        self.create_indices_on_tbl(
            idx_name, agg_tbl_name, ["val"]
        )

        return agg_tbl_name

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

    def insert_spatio_temporal_key_to_inv_idx(self, inv_idx: str, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        query = """
            INSERT INTO "{inv_idx}" (val, spatio_temporal_keys)
            SELECT val, ARRAY[?] as spatio_temporal_keys FROM "{agg_tbl}"
            ON CONFLICT (val) 
            DO
                UPDATE SET spatio_temporal_keys = array_distinct(flatten([{original}, EXCLUDED.spatio_temporal_keys]));
        """.format(
            inv_idx=inv_idx,
            agg_tbl=spatio_temporal_key.get_agg_tbl_name(tbl_id),
            original='"{inv_idx}".spatio_temporal_keys'.format(inv_idx=inv_idx),
        )
        self.cur.execute(query, [spatio_temporal_key.get_id(tbl_id)])

    def create_cnt_tbl_for_agg_tbl(self, tbl_id: str, spatio_temporal_key: SpatioTemporalKey):
        pass
