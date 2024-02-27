import duckdb
import pandas as pd

class DuckDBConnector:
    def __init__(self, conn_str):
        self.cur = duckdb.connect(database=conn_str)
    
    def create_tbl(self, tbl_name: str, df: pd.DataFrame, mode='replace'):
        if mode == 'replace':
            self.cur.sql(f'CREATE OR REPLACE TABLE "{tbl_name}" AS SELECT * FROM df')
        elif mode == 'append':
            self.cur.sql(f'INSERT INTO "{tbl_name}" AS SELECT * FROM df')