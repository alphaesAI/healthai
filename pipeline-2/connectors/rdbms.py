from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from ..settings import RDBMSSettings

class RDBMSConnector(BaseConnector):
    def __init__(self, name: str, config: RDBMSSettings):
        super().__init__(name, config)
        self.name = name
        self.config = config
        self._engine = None

    def connect(self) -> None:
        """  """
        url_params = {
            "drivername": self.config.db_type,
            "username": self.config.user,
            "password": self.config.password,
            "host": self.config.host,
            "port": self.config.port,
            "database": self.config.database,
        }

        connection_url = URL.create(**{k: v for k, v in url_params.items() if v is not None})

        self._engine = create_engine(connection_url, **self.config.connection_params)
        
        with self._engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        self._connection = self._engine

ConnectorRegistry.register("rdbms", RDBMSConnector)