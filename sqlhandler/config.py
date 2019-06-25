from __future__ import annotations

from maybe import Maybe
from subtypes import Enum
from pathmagic import PathLike, File

from sqlhandler import resources

from sqlalchemy.engine.url import URL


class Config:
    class Dialect(Enum):
        MsSql, MySql, Sqlite, PostgreSQL, Oracle = "mssql", "mysql", "sqlite", "posgresql", "oracle"

    def __init__(self, path: PathLike = None) -> None:
        self.resources = File.from_pathlike(Maybe(path).else_(resources.newfile("config", "json")))
        self.data = Maybe(self.resources.contents).else_({"default_host": None, "hosts": {}})

    def __repr__(self) -> str:
        return repr(self.data)

    def add_host(self, host: str, drivername: str, default_database: str, username: str = None, password: str = None, port: str = None, query: dict = None, is_default: bool = False) -> None:
        self.data["hosts"][host] = {
            "drivername": drivername,
            "default_database": default_database,
            "username": username,
            "password": password,
            "port": port,
            "query": query,
        }

        if is_default:
            self.set_default_host(host=host)

    def set_default_host(self, host: str) -> None:
        if host in self.data["hosts"]:
            self.data["default_host"] = host
        else:
            raise ValueError(f"Host {host} is not one of the currently registered hosts: {', '.join(self.data['hosts'])}. Use {type(self).__name__}.add_host() first.")

    def clear_config(self) -> None:
        self.data = None
        self.save()

    def save(self) -> None:
        self.resources.contents = self.data

    def generate_url(self, host: str = None, database: str = None) -> str:
        host = Maybe(host).else_(self.data.get("default_host"))
        host_settings = self.data["hosts"][host]
        database = Maybe(database).else_(host_settings["default_database"])
        return Url(drivername=host_settings["drivername"], username=host_settings["username"], password=host_settings["password"], host=host, port=host_settings["port"], database=database, query=host_settings["query"])


class Url(URL):
    def __init__(self, drivername: str = None, username: str = None, password: str = None, host: str = None, port: str = None, database: str = None, query: dict = None) -> None:
        super().__init__(drivername=drivername, username=Maybe(username).else_(""), password=password, host=host, port=port, database=database, query=query)
