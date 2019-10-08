from __future__ import annotations

from maybe import Maybe
from subtypes import Enum
import miscutils
import sqlhandler

from sqlalchemy.engine.url import URL


class Dialect(Enum):
    MS_SQL, MY_SQL, SQLITE, POSTGRESQL, ORACLE = "mssql", "mysql", "sqlite", "posgresql", "oracle"


class Url(URL):
    def __init__(self, drivername: str = None, username: str = None, password: str = None, host: str = None, port: str = None, database: str = None, query: dict = None) -> None:
        super().__init__(drivername=drivername, username=Maybe(username).else_(""), password=password, host=host, port=port, database=database, query=query)


class Config(miscutils.Config):
    Dialect = Dialect
    app_name = sqlhandler.__name__
    default = {"default_connection": "", "connections": {"memory": {"host": "", "drivername": "sqlite", "default_database": None, "username": None, "password": None, "port": None, "query": None}}}

    def add_connection(self, connection: str, host: str, drivername: str, default_database: str, username: str = None, password: str = None, port: str = None, query: dict = None, is_default: bool = False) -> None:
        self.data.connections[connection] = miscutils.NameSpaceDict(host=host, drivername=drivername, default_database=default_database, username=username, password=password, port=port, query=query)
        if is_default:
            self.set_default_connection(connection=connection)

    def add_mssql_connection_with_integrated_security(self, connection: str, host: str, default_database: str, is_default: bool = False):
        self.add_connection(connection=connection, host=host, drivername=Dialect.MS_SQL, default_database=default_database, is_default=is_default, query={"driver": "SQL+Server"})

    def set_default_connection(self, connection: str) -> None:
        if connection in self.data.connections:
            self.data.default_connection = connection
        else:
            raise ValueError(f"Connection {connection} is not one of the currently registered connections: {', '.join(self.data.connections)}. Use {type(self).__name__}.add_connection() first.")

    def generate_url(self, connection: str = None, database: str = None) -> str:
        settings = self.data.connections[Maybe(connection).else_(self.data.default_connection)]
        database = Maybe(database).else_(settings.default_database)
        return Url(drivername=settings.drivername, username=settings.username, password=settings.password, host=settings.host, port=settings.port, database=database, query=Maybe(settings.query).else_(None))
