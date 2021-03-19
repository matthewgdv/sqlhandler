from __future__ import annotations

from typing import TYPE_CHECKING

from django.apps import AppConfig
from django import db
from django.conf import settings

from subtypes import Dict

from sqlhandler.sql import Sql


if TYPE_CHECKING:
    from .sql import DjangoSql


class NullOp:
    pass


class SqlConfig(AppConfig):
    name, Sql, connections = "sqlhandler", Sql, Dict()
    sql: DjangoSql = None

    settings = Dict(
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
        self.setup()

    def setup(self) -> None:
        import sqlhandler.django as root

        self.settings.update(getattr(settings, "SQLHANDLER_SETTINGS", {}))
        con = db.connections
        for name, connection in db.connections.databases.items():
            driver = self.settings.ENGINES[connection["ENGINE"].split(".")[-1]]
            url = Sql.Url(drivername=driver, database=connection["NAME"], username=connection["USER"], password=connection["PASSWORD"], host=connection["HOST"], port=connection["PORT"])
            self.connections[name] = self.initialize_database(url)

        type(self).sql = root.sql = self.connections.default or None

    def initialize_database(self, url: Sql.Url):
        from .sql import DjangoSql

        try:
            return DjangoSql(url)
        except Exception as ex:
            import traceback
            print(f"The following exception caused sqlhandler.django to fail to start but was suppressed:\n\n{traceback.format_exc()}")
            return None
