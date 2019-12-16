from __future__ import annotations

from typing import Any

from django import db

from subtypes import Dict_

from ..config import Url
from .config import SqlConfig
from .database import DjangoDatabase, DjangoApps
from .model import SqlModel


class DjangoSql(SqlConfig.Sql):
    CACHE_METADATA = False
    constructors = SqlConfig.Sql.Constructors()
    constructors.Model, constructors.Database = SqlModel, DjangoDatabase

    @property
    def django(self) -> DjangoApps:
        return self.database.django

    def _create_url(self, connection: str, **kwargs: Any) -> Url:
        detail = Dict_(db.connections.databases[connection])
        drivername = SqlConfig.settings.ENGINES[detail.ENGINE.rpartition(".")[-1]]
        return Url(drivername=drivername, database=detail.NAME, username=detail.USER or None, password=detail.PASSWORD or None, host=detail.HOST or None, port=detail.PORT or None)
