from __future__ import annotations

from maybe import Maybe
import iotools

import sqlhandler
from sqlhandler.enums import Dialect

from sqlalchemy.engine.url import URL


class Url(URL):
    """Class representing a sql connection URL."""

    def __init__(self, drivername: Dialect = None, database: str = None, username: str = None, password: str = None, host: str = None, port: int = None, query: dict = None) -> None:
        super().__init__(drivername=str(drivername), username=Maybe(username).else_(""), password=password, host=host, port=port, database=str(database), query=query)


class Config(iotools.Config):
    """A config class granting access to an os-specific appdata directory for use by this library."""
    name = sqlhandler.__name__
    default = {"default_connection": "memory", "connections": {"memory": {"drivername": "sqlite", "default_database": None, "username": None, "password": None, "host": "", "port": None, "query": None}}}

    def add_connection(self, connection: str, drivername: str, default_database: str, username: str = None, password: str = None, host: str = None, port: str = None, query: dict = None, is_default: bool = False) -> None:
        """Add a new connection with the given arguments."""
        self.data.connections[connection] = dict(drivername=str(drivername), default_database=default_database, username=username, password=password, host=host, port=port, query=query)
        if is_default:
            self.set_default_connection(connection=connection)

    def add_mssql_connection_with_integrated_security(self, connection: str, default_database: str, host: str, is_default: bool = False):
        """Add a SQL server connection that will use Windows integrated security."""
        self.add_connection(connection=connection, drivername=Dialect.MS_SQL, default_database=default_database, host=host, query={"driver": "SQL+Server"}, is_default=is_default)

    def set_default_connection(self, connection: str) -> None:
        """Set the connection that will be used by default."""
        if connection in self.data.connections:
            self.data.default_connection = connection
        else:
            raise ValueError(f"Connection {connection} is not one of the currently registered connections: {', '.join(self.data.connections)}. Use {type(self).__name__}.add_connection() first.")

    def generate_url(self, connection: str = None, database: str = None) -> Url:
        """Generate a sql connection URL from the current config with optional overrides passed as arguments."""
        if connection is None or connection in self.data.connections:
            settings = self.data.connections[Maybe(connection).else_(self.data.default_connection)]
            database = Maybe(database).else_(settings.default_database)
            return Url(drivername=settings.drivername, database=database, username=settings.username, password=settings.password, host=settings.host, port=settings.port, query=Maybe(settings.query).else_(None))
        else:
            raise RuntimeError(f"Connection '{connection}' not found in preconfigured connections:\n\n{list(self.data.connections)}.")
