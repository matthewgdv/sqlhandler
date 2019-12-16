from __future__ import annotations

from typing import Type

from .config import SqlConfig


class SqlModel(SqlConfig.Sql.constructors.Model, SqlConfig.settings.MODEL_MIXIN):
    @classmethod
    def django(cls) -> Type[DjangoModel]:
        return SqlConfig.sql.database.django_mappings[cls.__table__.name]

    def __call__(self) -> DjangoModel:
        return type(self).django().objects.get(pk=getattr(self, list(self.__table__.primary_key)[0].name))


class DjangoModel:
    @classmethod
    def sql(cls) -> Type[SqlModel]:
        return SqlConfig.sql.database.sqlhandler_mappings[cls._meta.db_table]

    def __call__(self) -> SqlModel:
        return type(self).sql().query.get(getattr(self, self._meta.pk.name))
