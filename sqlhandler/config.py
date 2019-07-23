from __future__ import annotations

from maybe import Maybe
from subtypes import Enum
from pathmagic import PathLike, File
from miscutils import NameSpace

from sqlalchemy.engine.url import URL

from .appdata import appdata


class Config:
    class Dialect(Enum):
        MS_SQL, MY_SQL, SQLITE, POSTGRESQL, ORACLE = "mssql", "mysql", "sqlite", "posgresql", "oracle"

    def __init__(self, path: PathLike = None) -> None:
        self.resources = appdata.newfile(name="config", extension="json") if path is None else File.from_pathlike(path)
        self.data: NameSpace = self._read_to_namespace(self.resources)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

    def add_host(self, host: str, drivername: str, default_database: str, username: str = None, password: str = None, port: str = None, query: dict = None, is_default: bool = False) -> None:
        self.data.hosts[host] = NameSpace(drivername=drivername, default_database=default_database, username=username, password=password, port=port, query=query)
        if is_default:
            self.set_default_host(host=host)

    def set_default_host(self, host: str) -> None:
        if host in self.data.hosts:
            self.data.default_host = host
        else:
            raise ValueError(f"Host {host} is not one of the currently registered hosts: {', '.join(self.data.hosts)}. Use {type(self).__name__}.add_host() first.")

    def clear_config(self) -> None:
        self.data = None
        self.save()

    def save(self) -> None:
        self.resources.contents = self.data

    def import_(self, path: PathLike) -> None:
        self.data = self._read_to_namespace(File.from_pathlike(path))

    def export(self, path: PathLike) -> None:
        self.resources.copy(path)

    def export_to(self, path: PathLike) -> None:
        self.resources.copyto(path)

    def open(self) -> File:
        return self.resources.open()

    def generate_url(self, host: str = None, database: str = None) -> str:
        host = Maybe(host).else_(self.data.default_host)
        host_settings = self.data.hosts[host]
        database = Maybe(database).else_(host_settings.default_database)
        return Url(drivername=host_settings.drivername, username=host_settings.username, password=host_settings.password, host=host, port=host_settings.port, database=database, query=host_settings.query.to_dict())

    @staticmethod
    def _read_to_namespace(file: File) -> NameSpace:
        return Maybe(file.contents).else_(NameSpace(default_host=None, hosts={}))



class Url(URL):
    def __init__(self, drivername: str = None, username: str = None, password: str = None, host: str = None, port: str = None, database: str = None, query: dict = None) -> None:
        super().__init__(drivername=drivername, username=Maybe(username).else_(""), password=password, host=host, port=port, database=database, query=query)
