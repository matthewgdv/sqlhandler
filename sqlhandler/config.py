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
    default = {"default_host": "", "hosts": {"": {"drivername": "sqlite", "default_database": None, "username": None, "password": None, "port": None, "query": None}}}

    def add_host(self, host: str, drivername: str, default_database: str, username: str = None, password: str = None, port: str = None, query: dict = None, is_default: bool = False) -> None:
        self.data.hosts[host] = miscutils.NameSpaceDict(drivername=drivername, default_database=default_database, username=username, password=password, port=port, query=query)
        if is_default:
            self.set_default_host(host=host)

    def add_mssql_host_with_integrated_security(self, host: str, default_database: str, is_default: bool = False):
        self.add_host(host=host, drivername=Dialect.MS_SQL, default_database=default_database, is_default=is_default, query={"driver": "SQL+Server"})

    def set_default_host(self, host: str) -> None:
        if host in self.data.hosts:
            self.data.default_host = host
        else:
            raise ValueError(f"Host {host} is not one of the currently registered hosts: {', '.join(self.data.hosts)}. Use {type(self).__name__}.add_host() first.")

    def generate_url(self, host: str = None, database: str = None) -> str:
        host = Maybe(host).else_(self.data.default_host)
        host_settings = self.data.hosts[host]
        database = Maybe(database).else_(host_settings.default_database)
        return Url(drivername=host_settings.drivername, username=host_settings.username, password=host_settings.password, host=host, port=host_settings.port, database=database, query=Maybe(host_settings.query).else_(None))
