from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Any, Dict, Optional
from .base import BaseConnector
from .registry import ConnectorRegistry


class PostgresConnector(BaseConnector):
    """PostgreSQL connector using YAML configuration and SQLAlchemy."""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self._engine: Optional[Engine] = None
    
    def connect(self) -> None:
        """Establish PostgreSQL connection using YAML configuration and SQLAlchemy."""
        try:
            # Get connection configuration from YAML
            connection_config = self.config.get('connection', {})
            
            # Build connection parameters from YAML config only
            conn_params = {
                'host': connection_config.get('host'),
                'port': connection_config.get('port'),
                'database': connection_config.get('database'),
                'user': connection_config.get('user'),
                'password': connection_config.get('password')
            }
            
            # Remove None values
            conn_params = {k: v for k, v in conn_params.items() if v is not None}
            
            # Add any additional config, separating SQLAlchemy-specific params
            connection_params = connection_config.get('connection_params', {})
            
            # Separate PostgreSQL-specific parameters from SQLAlchemy engine parameters
            pg_params = {}
            engine_params = {}
            
            for key, value in connection_params.items():
                if key in ['connect_timeout', 'application_name']:
                    # These go in the connection string
                    pg_params[key] = value
                else:
                    # These go to create_engine
                    engine_params[key] = value
            
            # Build connection URL for SQLAlchemy
            username = conn_params.pop('user', None)
            password = conn_params.pop('password', None)
            host = conn_params.pop('host', 'localhost')
            port = conn_params.pop('port', 5432)
            database = conn_params.pop('database', None)
            
            if not database:
                raise ValueError("Database name is required")
            
            # Create connection URL with PostgreSQL-specific parameters
            connection_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            
            # Add PostgreSQL-specific parameters to URL
            if pg_params:
                param_string = "&".join([f"{k}={v}" for k, v in pg_params.items()])
                connection_url += f"?{param_string}"
            
            # Create SQLAlchemy engine with valid engine parameters
            self._engine = create_engine(connection_url, **engine_params)
            
            # Test connection
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._connection = self._engine
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")
    
    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        try:
            if self._engine:
                self._engine.dispose()
                self._engine = None
            self._connection = None
        except Exception as e:
            raise ConnectionError(f"Error disconnecting from PostgreSQL: {e}")
    
    def get_connection(self) -> Optional[Engine]:
        """Return the live database engine."""
        return self._engine
    
    def execute_query(self, query: str, params: Optional[dict] = None) -> list:
        """Execute a query and return results."""
        if not self._engine:
            raise ConnectionError("Not connected to PostgreSQL")
        
        try:
            with self._engine.connect() as conn:
                # Use text() for SQL injection protection
                sql_query = text(query)
                
                # Execute query with parameters
                if params:
                    result = conn.execute(sql_query, params)
                else:
                    result = conn.execute(sql_query)
                
                # Check if query returns data
                if result.returns_rows:
                    # Get column names
                    columns = list(result.keys())
                    rows = result.fetchall()
                    # Convert to list of dictionaries
                    return [dict(zip(columns, row)) for row in rows]
                
                return []
                
        except Exception as e:
            raise e


# Register the connector
ConnectorRegistry.register("postgres", PostgresConnector)