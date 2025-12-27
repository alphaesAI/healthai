from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class RDBMSSettings(BaseModel):
    """ Pydantic model for RDBMS configuration. """
    db_type: str = Field(..., description="e.g., postgresql, mysql, sqlite")
    user: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    connection_params: Optional[Dict[str, Any]] = Field(default_factor=dict)