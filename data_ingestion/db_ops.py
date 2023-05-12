from typing import List
from data_search.data_model import Unit, Variable, AggFunc
from psycopg2 import sql
import sys
from io import StringIO


def get_col_names_with_granu(units: List[Unit]):
    return [unit.to_int_name() for unit in units]


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


def del_tbl(cur, tbl_name):
    sql_str = """DROP TABLE IF EXISTS {tbl}"""
    cur.execute(sql.SQL(sql_str).format(tbl=sql.Identifier(tbl_name)))


def create_agg_tbl(
    cur,
    tbl: str,
    units: List[Unit],
    vars: List[Variable],
):
    col_names = get_col_names_with_granu(units)
    agg_tbl_name = "{}_{}".format(tbl, "_".join([col for col in col_names]))

    del_tbl(cur, agg_tbl_name)

    sql_str = """
        CREATE TABLE {agg_tbl} AS
        SELECT {fields}, {agg_stmts} FROM {tbl} GROUP BY {fields}
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

    create_indices_on_tbl(cur, agg_tbl_name, col_names)


def create_indices_on_tbl(cur, tbl, col_names):
    sql_str = """
            CREATE INDEX {idx_name} ON {tbl} ({cols});
        """

    query = sql.SQL(sql_str).format(
        idx_name=sql.Identifier("{}_idx".format(tbl)),
        tbl=sql.Identifier(tbl),
        cols=sql.SQL(",").join([sql.Identifier(col) for col in col_names]),
    )

    cur.execute(query)
