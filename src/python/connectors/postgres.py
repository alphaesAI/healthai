import psycopg2
from psycopg2 import sql
from typing import Any, Dict, Optional
from airflow.hooks.base_hook import BaseHook
from .base import BaseConnector


class PostgresConnector(BaseConnector):
    """PostgreSQL connector using Airflow connection ID."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.connection_id = config.get('connection_id')
        self._connection = None
        self._cursor = None
    
    def connect(self) -> None:
        """Establish PostgreSQL connection using Airflow connection."""
        try:
            if not self.connection_id:
                raise ValueError("Connection ID is required for PostgreSQL connector")
            
            # Get connection from Airflow
            airflow_conn = BaseHook.get_connection(self.connection_id)
            
            # Build connection parameters
            conn_params = {
                'host': airflow_conn.host,
                'port': airflow_conn.port or 5432,
                'database': airflow_conn.schema or airflow_conn.conn_type,
                'user': airflow_conn.login,
                'password': airflow_conn.password
            }
            
            # Add any additional config
            conn_params.update(self.config.get('connection_params', {}))
            
            self._connection = psycopg2.connect(**conn_params)
            self._cursor = self._connection.cursor()
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")
    
    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        try:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            if self._connection:
                self._connection.close()
                self._connection = None
        except Exception as e:
            raise ConnectionError(f"Error disconnecting from PostgreSQL: {e}")
    
    def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
        try:
            if not self._connection:
                return False
            
            self._cursor.execute("SELECT 1")
            result = self._cursor.fetchone()
            return result[0] == 1
        except Exception:
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get PostgreSQL connection information."""
        if not self._connection:
            return {"status": "disconnected"}
        
        try:
            self._cursor.execute("SELECT version()")
            version = self._cursor.fetchone()[0]
            
            return {
                "status": "connected",
                "connection_id": self.connection_id,
                "database": self._connection.get_dsn_parameters().get('dbname'),
                "host": self._connection.get_dsn_parameters().get('host'),
                "port": self._connection.get_dsn_parameters().get('port'),
                "version": version
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> list:
        """Execute a query and return results."""
        if not self._connection or not self._cursor:
            raise ConnectionError("Not connected to PostgreSQL")
        
        try:
            self._cursor.execute(query, params)
            if self._cursor.description:
                return self._cursor.fetchall()
            self._connection.commit()
            return []
        except Exception as e:
            self._connection.rollback()
            raise e
    
    def execute_many(self, query: str, params_list: list) -> None:
        """Execute a query with multiple parameter sets."""
        if not self._connection or not self._cursor:
            raise ConnectionError("Not connected to PostgreSQL")
        
        try:
            self._cursor.executemany(query, params_list)
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            raise e