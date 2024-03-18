from psycopg2 import sql
import sys
import pandas as pd
from enum import Enum


class IndexType(Enum):
    B_TREE = "B_TREE"
    HASH = "HASH"


"""
Data Ingestion
"""
# Define a function that handles and parses psycopg2 exceptions
def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # print the connect() error
    print("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


"""
BASIC DDL
"""


def select_columns(cur, tbl, col_names, format=None, concat=False):
    if not concat:
        sql_str = """
            SELECT {fields} FROM {tbl};
        """
        query = sql.SQL(sql_str).format(
            fields=sql.SQL(",").join([sql.Identifier(col) for col in col_names]),
            tbl=sql.Identifier(tbl),
        )

        cur.execute(query)
    else:
        sql_str = """
            SELECT CONCAT_WS(',', {fields}) FROM {tbl};
        """
        query = sql.SQL(sql_str).format(
            fields=sql.SQL(",").join([sql.Identifier(col) for col in col_names]),
            tbl=sql.Identifier(tbl),
        )

        cur.execute(query)

    if format is None:
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        return df
    elif format == "RAW":
        data = cur.fetchall()
        if len(data) == 0:
            return []
        if len(data[0]) == 1:
            return [str(x[0]) for x in data]
        elif len(data[0]) == 2:
            return [str(x[0]) + str(x[1]) for x in data]


def create_inv_index(cur, idx_tbl):
    """
    aggregate index tables to inverted indices
    """
    inv_tbl_name = "{}_inv".format(idx_tbl)
    del_tbl(cur, inv_tbl_name)
    sql_str = """
        CREATE TABLE {inv_idx_tbl} AS
        SELECT "val", array_agg("st_schema") as st_schema_list FROM {idx_tbl}
        GROUP BY "val"
    """
    query = sql.SQL(sql_str).format(
        inv_idx_tbl=sql.Identifier(inv_tbl_name),
        idx_tbl=sql.Identifier(idx_tbl),
    )
    cur.execute(query)

    create_indices_on_tbl(
        cur, inv_tbl_name + "_idx", inv_tbl_name, ["val"], mode=IndexType.HASH
    )


def create_indices_on_tbl(cur, idx_name, tbl, col_names, mode=IndexType.B_TREE):
    if mode == IndexType.B_TREE:
        sql_str = """
                CREATE INDEX {idx_name} ON {tbl} ({cols});
            """

        query = sql.SQL(sql_str).format(
            idx_name=sql.Identifier(idx_name),
            tbl=sql.Identifier(tbl),
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
            tbl=sql.Identifier(tbl),
            col=sql.Identifier(col_name),
        )
    # print(cur.mogrify(query))
    cur.execute(query)


def del_tbl(cur, tbl_name):
    sql_str = """DROP TABLE IF EXISTS {tbl}"""
    cur.execute(sql.SQL(sql_str).format(tbl=sql.Identifier(tbl_name)))


def create_correlation_sketch_tbl(cur, agg_tbl, k, keys):
    tbl_name = f"{agg_tbl}_sketch_{k}"
    del_tbl(cur, tbl_name)
    sql_str = """
        CREATE TABLE {tbl_name} AS
        SELECT * FROM {agg_tbl} WHERE val IN %s;
    """
    query = sql.SQL(sql_str).format(
        tbl_name=sql.Identifier(tbl_name), agg_tbl=sql.Identifier(agg_tbl)
    )
    cur.execute(query, (tuple(keys),))

def read_key(cur, agg_tbl: str):
    sql_str = """
        SELECT val FROM {agg_tbl};
    """

    query = sql.SQL(sql_str).format(
        agg_tbl=sql.Identifier(agg_tbl)
    )
    cur.execute(query)
    return [r[0] for r in cur.fetchall()]