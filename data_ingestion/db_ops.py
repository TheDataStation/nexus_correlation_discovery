from typing import List
from data_search.data_model import Unit, Variable, AggFunc, ST_Schema, SchemaType
from psycopg2 import sql
from psycopg2.extras import execute_batch
import sys
from io import StringIO
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


def copy_from_dataFile_StringIO(cur, df, tbl_name):
    copy_sql = """
    COPY "{}"
    FROM STDIN
    DELIMITER ',' 
    CSV HEADER
    """.format(
        tbl_name
    )
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    cur.copy_expert(copy_sql, buffer)


def create_idx_tbl(cur, idx_tbl):
    sql_str = """
        CREATE TABLE IF NOT EXISTS {idx_tbl} (
            val text UNIQUE,
            st_schema_list _text
        )
    """
    query = sql.SQL(sql_str).format(idx_tbl=sql.Identifier(idx_tbl))
    cur.execute(query)


def create_cnt_tbl_for_agg_tbl(cur, tbl, st_schema: ST_Schema):
    idx_cnt_name = "{}_inv_cnt".format(st_schema.get_idx_tbl_name())
    agg_tbl = st_schema.get_agg_tbl_name(tbl)
    if len(agg_tbl) >= 63:
        cnt_tbl_name = agg_tbl[:59] + "_cnt"
    else:
        cnt_tbl_name = f"{agg_tbl}_cnt"
    del_tbl(cur, cnt_tbl_name)
    sql_str = """
            CREATE TABLE {cnt_tbl_name} AS
            SELECT "inv"."val", cnt FROM {inv_cnt} inv JOIN {tbl} agg on inv."val" = agg."val" order by cnt desc
        """
    query = sql.SQL(sql_str).format(
        cnt_tbl_name=sql.Identifier(cnt_tbl_name),
        inv_cnt=sql.Identifier(idx_cnt_name),
        tbl=sql.Identifier(agg_tbl),
    )

    cur.execute(query)


def insert_to_idx_tbl(cur, idx_tbl, id, agg_tbl):
    sql_str = """
        INSERT INTO {idx_tbl} (val, st_schema_list)
        SELECT val, ARRAY[%s] as st_schema_list FROM {agg_tbl}
        ON CONFLICT (val) 
        DO
            UPDATE SET st_schema_list = (SELECT array_agg(distinct x)
                                         FROM unnest({original} || EXCLUDED.st_schema_list) as t(x));
    """
    query = sql.SQL(sql_str).format(
        idx_tbl=sql.Identifier(idx_tbl),
        agg_tbl=sql.Identifier(agg_tbl),
        original=sql.Identifier(idx_tbl, "st_schema_list"),
    )

    cur.execute(query, (id,))


def create_agg_tbl(
    cur,
    tbl: str,
    st_schema: ST_Schema,
    vars: List[Variable],
):
    col_names = st_schema.get_col_names_with_granu()
    agg_tbl_name = "{}_{}".format(tbl, "_".join([col for col in col_names]))

    del_tbl(cur, agg_tbl_name)

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
                for var in vars
            ]
        ),
        tbl=sql.Identifier(tbl),
        not_null_stmts=sql.SQL(" AND ").join(
            [
                sql.SQL("{} is not NULL").format(sql.Identifier(field))
                for field in col_names
            ]
        ),
    )

    cur.execute(query)
    if len(agg_tbl_name) >= 63:
        idx_name = agg_tbl_name[:59] + "_idx"
    else:
        idx_name = agg_tbl_name + "_idx"
    create_indices_on_tbl(
        cur, idx_name, agg_tbl_name, ["val"], IndexType.HASH
    )

    return agg_tbl_name


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


def create_inv_idx_cnt_tbl(cur, idx_names):
    for idx_name in idx_names:
        tbl_name = f"{idx_name}_cnt"
        print(idx_name, tbl_name)
        del_tbl(cur, tbl_name)
        sql_str = """
             CREATE TABLE {tbl_name} AS
             SELECT val, array_length(st_schema_list, 1) as cnt from {idx_name}
        """
        query = sql.SQL(sql_str).format(
            tbl_name=sql.Identifier(tbl_name), idx_name=sql.Identifier(idx_name)
        )
        cur.execute(query)

        create_indices_on_tbl(cur, tbl_name + "_i", tbl_name, ["val"], IndexType.HASH)


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