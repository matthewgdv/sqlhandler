from __future__ import annotations

from typing import Any, Callable, TYPE_CHECKING, Iterator, Tuple

from django.apps import apps

from maybe import Maybe
from subtypes import Str

from .config import SqlConfig

if TYPE_CHECKING:
    from .sql import DjangoSql


class DjangoApp(SqlConfig.Sql.Constructors.Schema):
    pass


class DjangoApps(SqlConfig.Sql.Constructors.Schemas):
    schema_constructor = DjangoApp

    def __init__(self, database: DjangoDatabase) -> None:
        super().__init__(database=database)
        self._table_mappings = {}
        self._hierarchize()

    def __repr__(self) -> str:
        return f"""{type(self).__name__}(num_apps={len(self)}, apps=[{", ".join([f"{type(schema).__name__}(name='{schema._name}', tables={len(schema) if schema._ready else '?'})" for name, schema in self])}])"""

    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        return super().__iter__()

    def _hierarchize(self) -> None:
        for app, models in apps.all_models.items():
            self[app] = schema = self.schema_constructor(name=app, parent=self)
            schema._ready = True
            for name, model in models.items():
                schema[name] = model = self._database.tables[None][(table_name := model._meta.db_table)]()
                self._table_mappings[table_name] = model


class DjangoDatabase(SqlConfig.Sql.Constructors.Database):
    def __init__(self, sql: DjangoSql) -> None:
        self.django_mappings = {model._meta.db_table: model for models in apps.all_models.values() for model in models.values()}
        super().__init__(sql=sql)

        self.django = DjangoApps(database=self)
        self.sqlhandler_mappings = self.django._table_mappings

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={repr(self.name)}, django={repr(self.django)})"

    def _scalar_name(self) -> Callable:
        def scalar_name(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
            return Maybe(self.django_mappings)[referred_cls.__name__]._meta.model_name.else_(referred_cls.__name__)

        return scalar_name

    def _collection_name(self) -> Callable:
        def collection_name(base: Any, local_cls: Any, referred_cls: Any, constraint: Any) -> str:
            real_name = Maybe(self.django_mappings)[referred_cls.__name__]._meta.model_name.else_(referred_cls.__name__)
            return Str(real_name).case.plural()

        return collection_name
