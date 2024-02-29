from typing import List, Dict
from utils.data_model import Attr, Variable, SpatioTemporalKey, KeyType
from psycopg2 import sql
import pandas as pd
from data_ingestion.db_ops import select_columns
import shelve
from collections import Counter
import collections

"""
Intersection Query
"""

def get_intersection(cur, agg_name1, agg_name2):
    # sql_str = """
    # SELECT count(*) FROM (
    #     SELECT val FROM {agg_tbl1} a1 
    #     INTERSECT
    #     SELECT val FROM {agg_tbl2} a2
    #     ) subquery
    # """
    sql_str = """
        SELECT count('val') FROM {agg_tbl1} a1 join {agg_tbl2} a2 on a1.val = a2.val
    """
    query = sql.SQL(sql_str).format(
        agg_tbl1=sql.Identifier(f"{agg_name1}_cnt"),
        agg_tbl2=sql.Identifier(f"{agg_name2}_cnt"),
    )
    cur.execute(query)
    res = cur.fetchone()[0]
    return res


def __get_intersection_inv_idx(cur, tbl, st_schema: SpatioTemporalKey, threshold):
    agg_tbl = st_schema.get_agg_tbl_name(tbl)
    col_names = st_schema.get_col_names_with_granu()
    val_list = select_columns(cur, agg_tbl, col_names, format="RAW")
    inv_idx_name = st_schema.get_idx_tbl_name()
    print(inv_idx_name)
    with shelve.open("inverted_indices/chicago_1k/{}".format(inv_idx_name)) as db:
        counter = Counter()
        for val in val_list:
            counter.update(db[val])
        result = []
        for cand in counter:
            cnt = counter[cand]
            if cnt >= threshold and cand[0] != tbl:
                result.append((cand, cnt))
        return result


def get_inv_idx_cnt(cur, inv_idx_names):
    res = {}
    for inv_idx in inv_idx_names:
        sql_str = """   
            select val, array_length(st_schema_list, 1) from {inv_idx} 
        """
        query = sql.SQL(sql_str).format(inv_idx=inv_idx)
        cur.execute(query)
        query_res = cur.fetchall()
        val_cnt = {}
        for val, cnt in query_res:
            val_cnt[val] = cnt
        res[inv_idx] = val_cnt
    return res


def get_inv_cnt(cur, tbl, st_schema: SpatioTemporalKey, threshold: int):
    agg_name = st_schema.get_agg_tbl_name(tbl)
    if len(agg_name) >= 63:
        agg_cnt_tbl = agg_name[:59] + "_cnt"
    else:
        agg_cnt_tbl = agg_name + "_cnt"

    # agg_cnt_tbl = f"{st_schema.get_agg_tbl_name(tbl)}_cnt"

    sql_str = """
        SELECT count(cnt), sum(cnt) FROM {inv_cnt}
    """

    query = sql.SQL(sql_str).format(inv_cnt=sql.Identifier(agg_cnt_tbl))
    cur.execute(query)
    res = cur.fetchone()
    total_lists, total_elements = res[0], res[1]
    # res = [x[0] for x in cur.fetchall()]
    # total_elements = sum(res)

    max_joinable_tbls = (total_elements - total_lists) // threshold

    # if threshold - 1 >= len(res):
    #     max_joinable_tbls = res[-1]
    # else:
    #     max_joinable_tbls = res[threshold - 1]
    return total_elements, max_joinable_tbls


def get_val_cnt(cur, tbl, st_schema: SpatioTemporalKey):
    sql_str = """
        SELECT count(*) from {agg_tbl};
    """
    query = sql.SQL(sql_str).format(
        agg_tbl=sql.Identifier(st_schema.get_agg_tbl_name(tbl))
    )
    cur.execute(query)
    query_res = cur.fetchall()[0][0]
    return query_res


def get_intersection_inv_idx(
    cur, tbl, st_schema: SpatioTemporalKey, threshold: int, sample_ratio: int = 0
):
    inv_idx_name = "{}_inv".format(st_schema.get_idx_tbl_name())
    agg_tbl = st_schema.get_agg_tbl_name(tbl)
    # col_names = st_schema.get_col_names_with_granu()
    sql_str = """
        SELECT cand, count(*) as cnt
        FROM( 
            SELECT unnest("st_schema_list") as cand FROM {inv_idx} inv JOIN {tbl} agg ON inv."val" = agg."val"
        ) subquery
        GROUP BY cand
    """
    # WITH sampled_table AS (
            #     SELECT "val"
            #     FROM {tbl_cnt} limit %s
            # )
    #  WITH sampled_table AS (
    #             SELECT "val"
    #             FROM {tbl_cnt} TABLESAMPLE SYSTEM(%s)
    #         )
    #  SELECT "val"
    #             FROM {tbl_cnt} order by cnt desc limit %s
    if sample_ratio > 0:
        sql_str = """
        WITH sampled_table AS (
             SELECT "val" FROM {tbl_cnt} limit %s
        )
        SELECT val, count(*) as cnt
        FROM(
            SELECT unnest("st_schema_list") as val FROM {inv_idx} inv where inv."val" in (SELECT "val" from sampled_table)
        ) subquery
        GROUP BY val
        """
    query = sql.SQL(sql_str).format(
        inv_idx=sql.Identifier(inv_idx_name),
        # fields=sql.SQL(",").join([sql.Identifier(col) for col in col_names]),
        tbl_cnt = sql.Identifier(f"{st_schema.get_agg_tbl_name(tbl)}_cnt"),
        tbl=sql.Identifier(agg_tbl),
    )
    if sample_ratio == 0:
        cur.execute(query)
    else:
        cur.execute(query, [sample_ratio])
    
    query_res = cur.fetchall()

    result = []
    sampled_cnt = 0
    # parsed_candidates = []
    # for t in candidates:
    for t in query_res:
        cand, overlap = tuple(t[0].split(",")), t[1]
        tbl2_id = cand[0]
        if tbl2_id == tbl:
            continue
        sampled_cnt += overlap
        if sample_ratio == 0 and overlap < threshold:
            continue

        if st_schema.type == KeyType.TIME_SPACE:
            st_schema2 = SpatioTemporalKey(
                temporal_attr=Attr(cand[1], st_schema.temporal_attr.granu),
                spatial_attr=Attr(cand[2], st_schema.spatial_attr.granu),
            )
        elif st_schema.type == KeyType.TIME:
            st_schema2 = SpatioTemporalKey(
                temporal_attr=Attr(cand[1], st_schema.temporal_attr.granu),
            )
        else:
            st_schema2 = SpatioTemporalKey(
                spatial_attr=Attr(cand[1], st_schema.spatial_attr.granu),
            )
        # parsed_candidates.append([st_schema2.get_agg_tbl_name(tbl2_id), overlap])
        result.append([tbl2_id, st_schema2, overlap])
    if sample_ratio > 0:
        return result, sampled_cnt
    return result


def get_intersection_agg_idx(cur, tbl, st_schema: SpatioTemporalKey, exclude_tbls, threshold):
    # query aggregated index tables to find joinable tables
    idx_tbl = st_schema.get_idx_tbl_name()

    query = sql.SQL(
        "select tbl_id, {attrs}, count(*) from {idx_tbl} \
                        where tbl_id != %s and ({v_attrs}) in \
                        (select {agg_attrs} from {agg_tbl}) \
                        group by tbl_id, {attrs} \
                        having count(*) >= %s"
    ).format(
        attrs=sql.SQL(",").join(
            [sql.Identifier(attr_name) for attr_name in st_schema.get_idx_attr_names()]
        ),
        idx_tbl=sql.Identifier(idx_tbl),
        v_attrs=sql.SQL(",").join(
            [sql.Identifier(attr_name) for attr_name in st_schema.get_idx_col_names()]
        ),
        agg_attrs=sql.SQL(",").join(
            [
                sql.Identifier(agg_attr)
                for agg_attr in st_schema.get_col_names_with_granu()
            ]
        ),
        agg_tbl=sql.Identifier(st_schema.get_agg_tbl_name(tbl)),
    )

    # cur.execute(
    #     query,
    #     (tuple(exclude_tbls), threshold),
    # )
    cur.execute(
        query,
        (tbl, threshold),
    )

    query_res = cur.fetchall()
    result = []
    for row in query_res:
        tbl2_id = row[0]
        if st_schema.type == KeyType.TIME_SPACE:
            st_schema2 = SpatioTemporalKey(
                temporal_attr=Attr(row[1], st_schema.temporal_attr.granu),
                spatial_attr=Attr(row[2], st_schema.spatial_attr.granu),
            )
        elif st_schema.type == KeyType.TIME:
            st_schema2 = SpatioTemporalKey(
                temporal_attr=Attr(row[1], st_schema.temporal_attr.granu),
            )
        else:
            st_schema2 = SpatioTemporalKey(
                temporal_attr=Attr(row[1], st_schema.spatial_attr.granu),
            )
        overlap = int(row[-1])
        result.append([tbl2_id, st_schema2, overlap])

    return result


def _join_two_agg_tables(
    cur,
    tbl1: str,
    st_schema1: SpatioTemporalKey,
    vars1: List[Variable],
    tbl2: str,
    st_schema2: SpatioTemporalKey,
    vars2: List[Variable],
):
    col_names1 = st_schema1.get_col_names_with_granu()
    col_names2 = st_schema2.get_col_names_with_granu()
    agg_tbl1 = st_schema1.get_agg_tbl_name(tbl1)
    agg_tbl2 = st_schema2.get_agg_tbl_name(tbl2)

    agg_join_sql = """
        SELECT {a1_fields1}, {agg_vars} FROM
        {agg_tbl1} a1 JOIN {agg_tbl2} a2
        ON {join_cond}
        """

    query = sql.SQL(agg_join_sql).format(
        a1_fields1=sql.SQL(",").join([sql.Identifier("a1", col) for col in col_names1]),
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
        agg_tbl1=sql.Identifier(agg_tbl1),
        agg_tbl2=sql.Identifier(agg_tbl2),
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

    cur.execute(query)

    df = pd.DataFrame(
        cur.fetchall(), columns=[desc[0] for desc in cur.description]
    ).dropna(
        subset=col_names1
    )  # drop empty keys
    return df


def join_two_agg_tables(
    cur,
    tbl1: str,
    agg_tbl1: str,
    vars1: List[Variable],
    tbl2: str,
    agg_tbl2: str,
    vars2: List[Variable],
    outer: bool = False,
):
    agg_join_sql = """
        SELECT a1.val, {agg_vars} FROM
        {agg_tbl1} a1 JOIN {agg_tbl2} a2
        ON a1.val = a2.val
        """
    if outer:
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
                for var in vars1
            ]
            + [
                sql.SQL("{} AS {}").format(
                    sql.Identifier("a2", var.var_name),
                    sql.Identifier(var.proj_name),
                )
                for var in vars2
            ]
        ),
        agg_tbl1=sql.Identifier(agg_tbl1),
        agg_tbl2=sql.Identifier(agg_tbl2),
    )

    cur.execute(query)

    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df


def join_two_agg_tables_api(
    cur,
    agg_tbl1: str,
    var1: List[str],
    agg_tbl2: str,
    var2: List[str],
    outer=False,
):
    agg_join_sql = """
        SELECT a1.val, {agg_vars} FROM
        {agg_tbl1} a1 JOIN {agg_tbl2} a2
        ON a1.val = a2.val
        """
    if outer:
        agg_join_sql = """
        SELECT a1.val as key1, a2.val as key2, {agg_vars} FROM
        {agg_tbl1} a1 FULL JOIN {agg_tbl2} a2
        ON a1.val = a2.val
        """
    query = sql.SQL(agg_join_sql).format(
        agg_vars=sql.SQL(",").join(
            [
                sql.SQL("{} AS {}").format(
                    sql.Identifier("a1", var1),
                    sql.Identifier(var1),
                )
            ]
            + [
                sql.SQL("{} AS {}").format(
                    sql.Identifier("a2", var2),
                    sql.Identifier(var2),
                )
            ]
        ),
        agg_tbl1=sql.Identifier(agg_tbl1),
        agg_tbl2=sql.Identifier(agg_tbl2),
    )

    cur.execute(query)

    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df, cur.mogrify(query)

def join_multi_agg_tbls(cur, tbl_cols: Dict[str, List[Variable]]):
    tbls = list(tbl_cols.keys())
    sql_str = "SELECT {attrs} FROM {base_tbl} {join_clauses}"
    query = sql.SQL(sql_str).format(
            attrs = sql.SQL(",").join([
                sql.SQL("{} AS {}").format(sql.Identifier(tbl, col.var_name), sql.Identifier(col.proj_name))
                for tbl, cols in tbl_cols.items() for col in cols
            ]),
            base_tbl=sql.Identifier(tbls[0]),
            join_clauses=sql.SQL(" ").join(
                [sql.SQL("INNER JOIN {next_tbl} ON {tbl}.val = {next_tbl}.val").format(tbl=sql.Identifier(tbls[0]), next_tbl=sql.Identifier(tbl)) for tbl in tbls[1:]]
            ),
        )
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df.astype(float).round(3)
    

def join_multi_vars(cur, variables: List[Variable], constraints=None):
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
        attrs = sql.SQL(",").join([
            sql.SQL("{}").format(sql.Identifier(tbl, col))
            for tbl, cols in tbl_cols.items() for col in cols
        ]+[sql.SQL("{} AS {}").format(sql.Identifier(tbl, 'count'), sql.Identifier(f'{tbl}_samples')) for tbl in tbl_cols.keys()]),
        base_tbl=sql.Identifier(tbls[0]),
        join_clauses=sql.SQL(" ").join(
            [sql.SQL("INNER JOIN {next_tbl} ON {tbl}.val = {next_tbl}.val").format(tbl=sql.Identifier(tbls[0]), next_tbl=sql.Identifier(tbl)) for tbl in tbls[1:]]
        ),
        filter=sql.SQL(" AND ").join(
            [sql.SQL("{col} >= %s").format(col=sql.Identifier(tbl, 'count')) for tbl in constaint_tbls]
        )
        )
    if not constraints:
        cur.execute(query)
    else:
        cur.execute(query, constaint_vals)
    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df, cur.mogrify(query, constaint_vals)


def read_agg_tbl(cur, agg_tbl: str, vars: List[Variable]=[]):
    if len(vars) == 0:
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
                for var in vars
            ]),
        agg_tbl=sql.Identifier(agg_tbl)
    )

    cur.execute(query)

    df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df.astype(float).round(3)
