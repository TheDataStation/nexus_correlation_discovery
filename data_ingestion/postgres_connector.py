import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from io import StringIO

class PostgresConnector:
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

    def create_tbl(self, tbl_name: str, df: pd.DataFrame, mode='replace'):
        # create the table if not exists
        schema = df.iloc[:0]
        schema.to_sql(
            tbl_name,
            con=self.conn,
            if_exists=mode,
            index=False,
        )
        # ingest data
        self._copy_from_dataFile_StringIO(df, tbl_name)
