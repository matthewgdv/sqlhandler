from __future__ import annotations

from typing import Type, TYPE_CHECKING

from django.db.models.manager import Manager
from django.db.models.options import Options

from .config import SqlConfig

if TYPE_CHECKING:
    from .sql import DjangoSql


class SqlModel(SqlConfig.Sql.Constructors.ReflectedModel, SqlConfig.settings.MODEL_MIXIN):
    @classmethod
    def django(cls) -> Type[DjangoModel]:
        return SqlConfig.sql.database.django_mappings[cls.__table__.name]

    @classmethod
    def handler(cls) -> DjangoSql:
        return SqlConfig.sql

    def __call__(self) -> DjangoModel:
        return type(self).django().objects.get(pk=getattr(self, list(self.__table__.primary_key)[0].name))


class DjangoModel:
    objects: Manager
    _meta: Options

    @classmethod
    def sql(cls) -> Type[SqlModel]:
        return SqlConfig.sql.database.sqlhandler_mappings[cls._meta.db_table]

    def __call__(self) -> SqlModel:
        return type(self).sql().query.get(getattr(self, self._meta.pk.name))
