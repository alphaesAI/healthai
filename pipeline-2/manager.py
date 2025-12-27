import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict
from .settings import RDBMSSettings
from .rdbms import RDBMSConnector

class LocalEnv(BaseSettings):
    """ Maps .env variables to class attributes. """
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

def get_connector(name: str):
    env = LocalEnv()

    with open("pipeline-2/connectors/connector_config.yml", "r") as f:
        yaml_data = yaml.safe_load(f)['connectors'][name]

    settings = RDBMSSettings(
        db_type=yaml_data.get("db_type"),
        user=env.db_user,
        password=env.db_password,
        host=env.db_host,
        port=env.db_port,
        database=yaml_data.get("connection_params", {})
    )

    return RDBMSConnector(name=name, config=settings)