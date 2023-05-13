from typing import List
from data_search.data_model import Unit, Variable, AggFunc, ST_Schema, SchemaType
from psycopg2 import sql

"""
Intersection Query
"""


def get_intersection_agg_idx(cur, tbl, st_schema: ST_Schema, exclude_tbls, threshold):
    # query aggregated index tables to find joinable tables
    idx_tbl = st_schema.get_idx_tbl_name()
    query = sql.SQL(
        "select tbl_id, {attrs}, count(*) from {idx_tbl} \
                        where tbl_id NOT IN %s and ({attrs}) in \
                        (select {agg_attrs} from {agg_tbl}) \
                        group by tbl_id, {attrs} \
                        having count(*) >= %s"
    ).format(
        attrs=sql.SQL(",").join(st_schema.get_idx_col_names()),
        idx_tbl=sql.Identifier(idx_tbl),
        agg_attrs=sql.SQL(",").join(st_schema.get_col_names_with_granu()),
        agg_tbl=sql.Identifier(st_schema.get_agg_tbl_name(tbl)),
    )

    cur.execute(
        query,
        (tuple(exclude_tbls), threshold),
    )

    query_res = cur.fetchall()
    result = []
    for row in query_res:
        tbl2_id = row[0]
        if st_schema.type == SchemaType.TS:
            st_schema2 = ST_Schema(
                t_unit=Unit(row[1], st_schema.t_unit.granu),
                s_unit=Unit(row[2], st_schema.s_unit.granu),
            )
        elif st_schema.type == SchemaType.TIME:
            st_schema2 = ST_Schema(
                t_unit=Unit(row[1], st_schema.t_unit.granu),
            )
        else:
            st_schema2 = ST_Schema(
                t_unit=Unit(row[1], st_schema.s_unit.granu),
            )
        overlap = int(row[-1])
        result.append([tbl2_id, st_schema2, overlap])

    return result
