from __future__ import annotations

from typing import Any, Callable, Optional, TYPE_CHECKING

from sqlalchemy import Column
from sqlalchemy import types
from sqlalchemy.orm import backref, relationship

from subtypes import Str, Dict, Enum

from .misc import ForeignKey, absolute_namespace

if TYPE_CHECKING:
    from .model import BaseModel, Table

# TODO: Find way to implement ONE_TO_MANY relationship by extending the previous model with a foreign key after the fact


class Relationship:
    class Settings:
        casing, fk_suffix, association_table_suffix = Str.Case.SNAKE, "id", "mapping"
        default_backref_kwargs = {
            "cascade": "all"
        }

    class Kind(Enum):
        ONE_TO_ONE, MANY_TO_ONE, MANY_TO_MANY = "one_to_one", "many_to_one", "many_to_many"

    class One:
        @classmethod
        def to_one(cls, target: BaseModel, backref_name: str = None, **backref_kwargs: Any) -> Relationship:
            return Relationship(target=target, kind=Relationship.Kind.ONE_TO_ONE, backref_name=backref_name, **backref_kwargs)

    class Many:
        @classmethod
        def to_one(cls, target: BaseModel, backref_name: str = None, **backref_kwargs: Any) -> Relationship:
            return Relationship(target=target, kind=Relationship.Kind.MANY_TO_ONE, backref_name=backref_name, **backref_kwargs)

        @classmethod
        def to_many(cls, target: BaseModel, backref_name: str = None, association: str = None, **backref_kwargs: Any) -> Relationship:
            return Relationship(target=target, kind=Relationship.Kind.MANY_TO_MANY, backref_name=backref_name, association=association, **backref_kwargs)

    class _TargetEntity:
        def __init__(self, model: BaseModel, rel: Relationship) -> None:
            self.relationship, self.model, self.name = rel, model, model.__table__.name
            self.pk, = list(self.model.__table__.primary_key)
            self.fk = self.relationship._casing(f"{self.name}_{rel.settings.fk_suffix}")

        def __repr__(self) -> str:
            return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

    class _FutureEntity:
        def __init__(self, table_name: str, bases: tuple, namespace: dict, rel: Relationship) -> None:
            self.relationship, self.name, self.bases, self.namespace = rel, table_name, bases, namespace
            self.plural = str(Str(self.name).case.plural())

            pk_key, = [key for key, val in absolute_namespace(bases=bases, namespace=namespace).items() if isinstance(val, Column) and val.primary_key]
            self.pk = f"{self.name}.{pk_key}"

        def __repr__(self) -> str:
            return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

    def __init__(self, target: BaseModel, kind: Relationship.Kind, backref_name: str = None, association: str = None, **backref_kwargs: Any) -> None:
        self.settings = self.Settings()

        self.target, self.kind, self.backref_name, self.association = Relationship._TargetEntity(model=target, rel=self), kind, backref_name, association
        self.backref_kwargs = Dict({**self.settings.default_backref_kwargs, **backref_kwargs})

        self.this: Optional[Relationship._FutureEntity] = None
        self.attribute: Optional[str] = None

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

    def build(self, table_name: str, bases: tuple, namespace: dict, attribute: str) -> None:
        self.this, self.attribute = Relationship._FutureEntity(table_name=table_name, bases=bases, namespace=namespace, rel=self), attribute
        self._build_fk_columns()
        self._build_relationship()

    def _build_fk_columns(self) -> None:
        if self.kind == self.Kind.MANY_TO_ONE:
            self.this.namespace[self.target.fk] = Column(types.Integer, ForeignKey(self.target.pk))
        elif self.kind == self.Kind.ONE_TO_ONE:
            self.this.namespace[self.target.fk] = Column(types.Integer, ForeignKey(self.target.pk), unique=True)
        else:
            self.Kind(self.kind)

    def _build_relationship(self) -> None:
        if self.backref_name is not None:
            backref_name = self.backref_name
        else:
            if self.kind == self.Kind.ONE_TO_ONE:
                backref_name = self.this.name
            elif self.kind in (self.Kind.MANY_TO_ONE, self.Kind.MANY_TO_MANY):
                backref_name = self.this.plural
            else:
                self.Kind(self.kind)
                backref_name = None

        if self.kind == Relationship.Kind.ONE_TO_ONE:
            self.backref_kwargs.uselist = False

        secondary = self._build_association_table() if self.kind == Relationship.Kind.MANY_TO_MANY else None

        self.this.namespace[self.attribute] = relationship(self.target.model, secondary=secondary, backref=backref(name=backref_name, **self.backref_kwargs))

    # noinspection PyArgumentList
    def _build_association_table(self) -> Callable[[], Table]:
        if self.association is not None:
            table = self.association
        else:
            from .model import Table

            name = self._casing(f"association_{self.this.name}_{self.target.name}")
            this_col = Column(self._casing(f"{self.this.name}_{self.settings.fk_suffix}"), types.Integer, ForeignKey(self.this.pk))
            target_col = Column(self._casing(f"{self.target.name}_{self.settings.fk_suffix}"), types.Integer, ForeignKey(self.target.pk))
            table = Table(name, self.target.model.metadata, Column(self._casing("id"), types.Integer, primary_key=True), this_col, target_col)

        self.this.namespace[self._casing(f"{self.target.name}_{self.settings.association_table_suffix}")] = table
        setattr(self.target.model, self._casing(f"{self.this.name}_{self.settings.association_table_suffix}"), table)

        def _defer_create_table() -> Table:
            table.create()
            return table

        return _defer_create_table

    def _casing(self, text: str) -> str:
        return str(Str(text).case.from_enum(self.settings.casing))
