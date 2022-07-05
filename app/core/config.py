from pathlib import Path
from typing import List, Union

from pydantic import AnyHttpUrl, BaseSettings, validator


class Settings(BaseSettings):
    PROJECT_NAME: str
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []

    DUCKDB_PATH: Path = Path("duck.db")

    @validator("BACKEND_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    # NOTE: MySQL is not used, just DuckDB
    # MYSQL_USER: str
    # MYSQL_PASSWORD: str
    # MYSQL_HOST: str
    # MYSQL_PORT: str
    # MYSQL_DATABASE: str
    # DATABASE_URI: Optional[str] = None

    # @validator("DATABASE_URI", pre=True)
    # def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
    #     if isinstance(v, str):
    #         return v
    #     return (
    #         f"mysql://{values.get('MYSQL_USER')}:{values.get('MYSQL_PASSWORD')}@{values.get('MYSQL_HOST')}:"
    #         f"{values.get('MYSQL_PORT')}/{values.get('MYSQL_DATABASE')}"
    #     )

    class Config:
        case_sensitive = True
        env_file = ".env"


settings = Settings()
