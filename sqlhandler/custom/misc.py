from __future__ import annotations

from typing import Any, TYPE_CHECKING

import sqlalchemy as alch
from sqlalchemy.schema import CreateTable
from sqlalchemy.orm.attributes import InstrumentedAttribute

if TYPE_CHECKING:
    from .model import ModelMeta


class CreateTableAccessor:
    def __init__(self, model_cls: ModelMeta) -> None:
        self.model_cls = model_cls

    def __repr__(self) -> str:
        return str(CreateTable(self.model_cls.__table__)).strip()

    def __call__(self) -> str:
        return self.model_cls.metadata.sql.database.create_table(self.model_cls)


class ForeignKey(alch.ForeignKey):
    def __init__(self, column: Any, *args: Any, **kwargs: Any) -> None:
        super().__init__(column=column.comparator.table.c[column.comparator.key] if isinstance(column, InstrumentedAttribute) else column, *args, **kwargs)


def absolute_namespace(bases: tuple, namespace: dict) -> dict:
    abs_ns = {}
    for immediate_base in reversed(bases):
        for hierarchical_base in reversed(immediate_base.mro()):
            abs_ns.update(vars(hierarchical_base))

    abs_ns.update(namespace)

    return abs_ns
