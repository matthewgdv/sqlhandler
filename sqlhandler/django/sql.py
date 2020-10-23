from __future__ import annotations

from typing import Any

from django import db

from subtypes import Dict

from .config import SqlConfig
from .database import DjangoDatabase, DjangoApps
from .model import SqlModel

from sqlhandler.utils import Url


class DjangoSql(SqlConfig.Sql):
    class Settings(SqlConfig.Sql.Settings):
        eager_reflection = cache_metadata = True
        reflect_views = False

    class Constructors(SqlConfig.Sql.Constructors):
        ReflectedModel, Database = SqlModel, DjangoDatabase

    @property
    def django(self) -> DjangoApps:
        return self.database.django

    def _create_url(self, connection: str, **kwargs: Any) -> Url:
        detail = Dict(db.connections.databases[connection])
        drivername = SqlConfig.settings.ENGINES[detail.ENGINE.rpartition(".")[-1]]
        return Url(drivername=drivername, database=detail.NAME, username=detail.USER or None, password=detail.PASSWORD or None, host=detail.HOST or None, port=detail.PORT or None)
