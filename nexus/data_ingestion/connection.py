from nexus.db_connector.duckdb_connector import DuckDBConnector
from nexus.db_connector.postgres_connector import PostgresConnector


class ConnectionFactory:
    @staticmethod
    def create_connection(conn_str: str, engine='postgres', read_only=True):
        if engine == 'postgres':
            db_engine = PostgresConnector(conn_str)
        elif engine == 'duckdb':
            db_engine = DuckDBConnector(conn_str, read_only)
        return db_engine
