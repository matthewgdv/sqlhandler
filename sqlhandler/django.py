from __future__ import annotations

from typing import Any, Type

from django.apps import AppConfig, apps
from django import db
from django.conf import settings

from maybe import Maybe
from subtypes import Str, Dict_, NameSpace

from .sql import Sql
from .config import Url
import sqlhandler

connections = Dict_()
sql = None
DjangoModel = None


class DjangoModelMixin:
    sql: Type[DjangoModel] = None

    def __call__(self) -> DjangoModel:
        return self.sql.query.get(getattr(self, self._meta.pk.name))

    @property
    def sql(self):
        return DjangoDatabase.django_mappings[self._meta.db_name].sql


class NullOp:
    pass


class SqlHandlerConfig(AppConfig):
    name, Sql = "sqlhandler", Sql
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
        self.settings.update(getattr(settings, "SQLHANDLER_SETTINGS", {}))
        DjangoDatabase.django_mappings = {model._meta.db_table: model for models in apps.all_models.values() for model in models.values()}

        for connection in db.connections.databases:
            connections[connection] = sql = DjangoSql(connection)
            self.map_models(sql)
            sql.database.django._hierarchize()

        sqlhandler.django.sql = connections.default or None
        self.create_model_cls()

    def map_models(self, sql: DjangoSql) -> None:
        for table, model in DjangoDatabase.django_mappings.items():
            for schema in self.settings.SCHEMAS:
                if table in sql.orm[schema]():
                    sql_model = sql.orm[schema][table]
                    model.sql, sql_model.django = sql_model, model
                    break

    def create_model_cls(self) -> None:
        from django.db.models import Model

        class DjangoModel(Model, DjangoModelMixin):
            pass

        sqlhandler.django.DjangoModel = DjangoModel


class SqlModel(SqlHandlerConfig.Sql.constructors.Model, SqlHandlerConfig.settings.MODEL_MIXIN):
    django: Type[DjangoModelMixin] = None

    def __call__(self) -> DjangoModelMixin:
        return self.django.objects.get(pk=getattr(self, list(self.__table__.primary_key)[0].name))

    @property
    def django(self):
        return DjangoApps.sqlhandler_mappings[self.__table__.name].django


class DjangoApp(SqlHandlerConfig.Sql.constructors.OrmSchema):
    pass


class DjangoApps(SqlHandlerConfig.Sql.constructors.OrmSchemas):
    schema_constructor = DjangoApp
    sqlhandler_mappings = {}

    def __repr__(self) -> str:
        return f"""{type(self).__name__}(num_apps={len(self)}, apps=[{", ".join([f"{type(schema).__name__}(name='{schema._name}', tables={len(schema) if schema._ready else '?'})" for name, schema in self])}])"""

    def _refresh(self) -> None:
        pass

    def _hierarchize(self) -> None:
        NameSpace.__call__(self)
        for app, models in apps.all_models.items():
            self[app] = schema = self.schema_constructor(database=self._database, name=app)
            schema._ready = True
            for name, model in models.items():
                schema[name] = model.sql
                self.sqlhandler_mappings[model._meta.db_table] = model.sql


class DjangoDatabase(SqlHandlerConfig.Sql.constructors.Database):
    django_mappings: dict = None

    def __init__(self, sql: DjangoSql) -> None:
        self.django = DjangoApps(database=self)
        super().__init__(sql=sql)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self.name)}, django={repr(self.django)})"

    @staticmethod
    def _table_name(base: Any, tablename: Any, table: Any) -> str:
        return tablename

    @staticmethod
    def _scalar_name(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
        return Maybe(DjangoDatabase.django_mappings)[referred_cls.__name__]._meta.model_name.else_(referred_cls.__name__)

    @staticmethod
    def _collection_name(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
        real_name = Maybe(DjangoDatabase.django_mappings)[referred_cls.__name__]._meta.model_name.else_(referred_cls.__name__)
        return Str(real_name).case.plural()


class DjangoSql(SqlHandlerConfig.Sql):
    CACHE_METADATA = False
    constructors = SqlHandlerConfig.Sql.Constructors()
    constructors.Model, constructors.Database = SqlModel, DjangoDatabase

    @property
    def django(self) -> DjangoApps:
        return self.database.django

    def _create_url(self, connection: str, **kwargs: Any) -> Url:
        detail = Dict_(db.connections.databases[connection])
        drivername = SqlHandlerConfig.settings.ENGINES[detail.ENGINE.rpartition(".")[-1]]
        return Url(drivername=drivername, database=detail.NAME, username=detail.USER or None, password=detail.PASSWORD or None, host=detail.HOST or None, port=detail.PORT or None)
