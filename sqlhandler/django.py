from typing import Any

from django.apps import AppConfig, apps
from django.db import connections
from django.conf import settings

from subtypes import Dict_

from .sql import Sql
from .config import Url
import sqlhandler

con = Dict_()
sql = None


class DjangoSql(Sql):
    CACHE_METADATA = False

    SQLALCHEMY_ENGINES = {
        "sqlite3": "sqlite",
        "mysql": "mysql",
        "postgresql": "postgresql",
        "postgresql_psycopg2": "postgresql+psycopg2",
        "oracle": "oracle",
    }
    SQLALCHEMY_ENGINES.update(getattr(settings, "SQLHANDLER_ENGINES", {}))

    def _create_url(self, connection: str, **kwargs: Any) -> Url:
        detail = Dict_(connections.databases[connection])
        drivername = self.SQLALCHEMY_ENGINES[detail.ENGINE.rpartition(".")[-1]]
        return Url(drivername=drivername, database=detail.NAME, username=detail.USER or None, password=detail.PASSWORD or None, host=detail.HOST or None, port=detail.PORT or None)


class SqlHandlerConfig(AppConfig):
    name = "sqlhandler"

    def ready(self) -> None:
        for connection in connections.databases:
            con[connection] = sql = DjangoSql(connection)
            self.map_models(sql)

        sqlhandler.django.sql = con.default or None

    def map_models(self, sql: DjangoSql) -> None:
        for model in apps.get_models():
            if model._meta.db_table in sql.orm.default:
                sql_model = sql.orm.default[model._meta.db_table]
                model.sql, sql_model.django = sql_model, model
