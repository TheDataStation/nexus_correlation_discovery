import psycopg2
from data_ingestion import db_ops

conn_str = "postgresql://yuegong@localhost/st_tables"
conn = psycopg2.connect(conn_str)
conn.autocommit = True
cur = conn.cursor()

def read_vars(cur, tbl, attrs):
    df = db_ops.read_agg_tbl(cur, tbl, st_schema, vars)
  
def join_clustered_vars(source_tbl_id, source_tbl_name, source_col, granu, df):
    # get all correlations with the input varaible in a cluster
    corrs = df[((df['tbl_id1']==source_tbl_id) & (df['agg_attr1']==source_col)) | ((df['tbl_id2']==source_tbl_id) & (df['agg_attr2']==source_col))]

    # join the source variable with all correlated varaibles
    for _, row in corrs.iterrows():
        tbl1, attr1, tbl2, attr2 = row['tbl_id1'], row['agg_attr1'], row['tbl_id2'], row['agg_attr2']
        if (source_tbl_id, source_col) == (tbl1, attr1):
            tbl2, attr2 = tbl2, attr2
        elif (source_tbl_id, source_col) == (tbl2, attr2):
            tbl2, attr2 = tbl1, attr1

  
