from __future__ import annotations

import os

from typing import TYPE_CHECKING

from django.apps import AppConfig
from django import db
from django.conf import settings

from subtypes import Dict_

from sqlhandler.sql import Sql


if TYPE_CHECKING:
    from .sql import DjangoSql


class NullOp:
    pass


class SqlConfig(AppConfig):
    name, Sql, connections = "sqlhandler", Sql, Dict_()
    sql: DjangoSql = None

    settings = Dict_(
        {
            "SCHEMAS": [None],
            "ENGINES": {
                "sqlite3": "sqlite",
                "mysql": "mysql",
                "postgresql": "postgresql",
                "postgresql_psycopg2": "postgresql+psycopg2",
                "oracle": "oracle",
            },
            "MODEL_MIXIN": NullOp,
        }
    )

    def ready(self) -> None:
        if os.environ.get("RUN_MAIN", None) != "true":
            self.setup()

    def setup(self) -> None:
        from .sql import DjangoSql
        import sqlhandler.django as root

        self.settings.update(getattr(settings, "SQLHANDLER_SETTINGS", {}))
        for connection in db.connections.databases:
            self.connections[connection] = DjangoSql(connection)

        type(self).sql = root.sql = self.connections.default or None
