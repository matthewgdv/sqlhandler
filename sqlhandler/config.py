from __future__ import annotations

from maybe import Maybe
from subtypes import Enum
from pathmagic import PathLike, File
from miscutils import NameSpace

from sqlalchemy.engine.url import URL

from .appdata import appdata, global_appdata


class Dialect(Enum):
    MS_SQL, MY_SQL, SQLITE, POSTGRESQL, ORACLE = "mssql", "mysql", "sqlite", "posgresql", "oracle"


class Config:
    Dialect = Dialect

    def __init__(self, path: PathLike = None, global_config: bool = False) -> None:
        self.file = (appdata if not global_config else global_appdata).newfile(name="config", extension="json") if path is None else File.from_pathlike(path)
        self.data: NameSpace = self._read_to_namespace(self.file)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

    def add_host(self, host: str, drivername: str, default_database: str, username: str = None, password: str = None, port: str = None, query: dict = None, is_default: bool = False) -> None:
        self.data.hosts[host] = NameSpace(drivername=drivername, default_database=default_database, username=username, password=password, port=port, query=query)
        if is_default:
            self.set_default_host(host=host)

    def add_mssql_host_with_integrated_security(self, host: str, default_database: str, is_default: bool = False):
        self.add_host(host=host, drivername=Dialect.MS_SQL, default_database=default_database, is_default=is_default, query={"driver": "SQL+Server"})

    def set_default_host(self, host: str) -> None:
        if host in self.data.hosts:
            self.data.default_host = host
        else:
            raise ValueError(f"Host {host} is not one of the currently registered hosts: {', '.join(self.data.hosts)}. Use {type(self).__name__}.add_host() first.")

    def clear_config(self) -> None:
        self.data = None
        self.save()

    def save(self) -> None:
        self.file.contents = self.data

    def import_(self, path: PathLike) -> None:
        self.data = self._read_to_namespace(File.from_pathlike(path))

    def export(self, path: PathLike) -> None:
        self.file.copy(path)

    def export_to(self, path: PathLike) -> None:
        self.file.copyto(path)

    def open(self) -> File:
        return self.file.open()

    def generate_url(self, host: str = None, database: str = None) -> str:
        host = Maybe(host).else_(self.data.default_host)
        host_settings = self.data.hosts[host]
        database = Maybe(database).else_(host_settings.default_database)
        return Url(drivername=host_settings.drivername, username=host_settings.username, password=host_settings.password, host=host, port=host_settings.port, database=database, query=Maybe(host_settings.query).to_dict().else_(None))

    @staticmethod
    def _read_to_namespace(file: File) -> NameSpace:
        file = File.from_pathlike(file)

        if file.extension != "json":
            raise TypeError(f"Config file must be type 'json'.")

        contents = file.contents
        return contents if contents is not None else NameSpace(default_host="", hosts={"": dict(drivername="sqlite", default_database=None, username=None, password=None, port=None, query=None)})


class Url(URL):
    def __init__(self, drivername: str = None, username: str = None, password: str = None, host: str = None, port: str = None, database: str = None, query: dict = None) -> None:
        super().__init__(drivername=drivername, username=Maybe(username).else_(""), password=password, host=host, port=port, database=database, query=query)
