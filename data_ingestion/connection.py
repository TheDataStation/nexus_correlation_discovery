from sqlalchemy.dialects.postgresql import psycopg2

from data_ingestion.duckdb_connector import DuckDBConnector
from data_ingestion.postgres_connector import PostgresConnector


class ConnectionFactory:
    @staticmethod
    def create_connection(conn_str: str, engine='postgres'):
        if engine == 'postgres':
            db_engine = PostgresConnector(conn_str)
        elif engine == 'duckdb':
            db_engine = DuckDBConnector(conn_str)
        return db_engine
