from subtypes import Enum


class Dialect(Enum):
    """Enum of known dialect drivers."""
    MS_SQL, MY_SQL, SQLITE, POSTGRESQL, ORACLE = "mssql", "mysql", "sqlite", "postgresql+psycopg2", "oracle"


class IfExists(Enum):
    """Enum describing operation if a table being created already exists."""
    FAIL, REPLACE, APPEND = "fail", "replace", "append"
