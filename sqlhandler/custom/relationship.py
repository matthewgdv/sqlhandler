from __future__ import annotations

from typing import Any, Callable, Optional, TYPE_CHECKING

from sqlalchemy import Column
from sqlalchemy import types
from sqlalchemy.orm import backref, relationship

from subtypes import Str, Dict, Enum

from .misc import absolute_namespace
from sqlalchemy import ForeignKey

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
        ONE_TO_ONE, MANY_TO_ONE, MANY_TO_MANY, SELF_REFERENTIAL = "one_to_one", "many_to_one", "many_to_many", "self_referential"

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

        @classmethod
        def to_self(cls, backref_name: str = None, **backref_kwargs: Any) -> Relationship:
            return Relationship(target=None, kind=Relationship.Kind.SELF_REFERENTIAL, backref_name=backref_name, **backref_kwargs)

    class _TargetEntity:
        def __init__(self, rel: Relationship, name: str, pk: str, fk: str, model: BaseModel = None) -> None:
            self.relationship, self.name, self.pk, self.fk, self.model = rel, name, pk, fk, model

        def __repr__(self) -> str:
            return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

        @classmethod
        def from_model(cls, rel: Relationship, model: BaseModel):
            name = model.__table__.name
            pk, = list(model.__table__.primary_key)
            fk = rel._casing(f"{name}_{rel.settings.fk_suffix}")

            return cls(rel=rel, name=name, pk=pk, fk=fk, model=model)

        @classmethod
        def from_namespace(cls, rel: Relationship, name: str, namespace: dict):
            pk, = [val for key, val in namespace.items() if isinstance(val, Column) and val.primary_key]
            fk = rel._casing(f"{name}_{rel.settings.fk_suffix}")
            return cls(rel=rel, name=name, pk=pk, fk=fk, model=None)

    class _FutureEntity:
        def __init__(self, table_name: str, bases: tuple, namespace: dict, rel: Relationship) -> None:
            self.relationship, self.name, self.bases, self.namespace = rel, table_name, bases, namespace
            self.plural = str(Str(self.name).case.plural())

            pk, = [key for key, val in absolute_namespace(bases=bases, namespace=namespace).items() if isinstance(val, Column) and val.primary_key]
            self.pk = f"{self.name}.{pk}" if pk else None

        def __repr__(self) -> str:
            return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

    def __init__(self, target: BaseModel = None, kind: Relationship.Kind = None, backref_name: str = None, association: str = None, **backref_kwargs: Any) -> None:
        self.settings = self.Settings()

        self.target, self.kind, self.backref_name, self.association = target, kind, backref_name, association
        self.backref_kwargs = Dict({**self.settings.default_backref_kwargs, **backref_kwargs})

    def __repr__(self) -> str:
        return f"{type(self).__name__}({', '.join([f'{attr}={repr(val)}' for attr, val in self.__dict__.items() if not attr.startswith('_')])})"

    def build(self, table_name: str, bases: tuple, namespace: dict, attribute: str) -> None:
        self.this = Relationship._FutureEntity(table_name=table_name, bases=bases, namespace=namespace, rel=self)
        self.other = Relationship._TargetEntity.from_model(rel=self, model=self.target) if self.target else Relationship._TargetEntity.from_namespace(rel=self, name=table_name, namespace=absolute_namespace(bases=bases, namespace=namespace))
        self.attribute = attribute

        self._build_fk_columns()
        self._build_relationship()

    def _build_fk_columns(self) -> None:
        if self.kind == self.Kind.MANY_TO_ONE:
            self.this.namespace[self.other.fk] = Column(types.Integer, ForeignKey(self.other.pk))
        elif self.kind == self.Kind.ONE_TO_ONE:
            self.this.namespace[self.other.fk] = Column(types.Integer, ForeignKey(self.other.pk), unique=True)
        elif self.kind == self.Kind.SELF_REFERENTIAL:
            self.this.namespace[self.other.fk] = Column(types.Integer, ForeignKey(self.other.pk))
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
            elif self.kind == self.Kind.SELF_REFERENTIAL:
                backref_name = self.this.plural
            else:
                self.Kind(self.kind)
                backref_name = None

        if self.kind == Relationship.Kind.ONE_TO_ONE:
            self.backref_kwargs.uselist = False

        secondary = self._build_association_table() if self.kind == Relationship.Kind.MANY_TO_MANY else None

        self.this.namespace[self.attribute] = relationship(self.other.model or self.other.name, secondary=secondary, backref=backref(name=backref_name, **self.backref_kwargs))

    # noinspection PyArgumentList
    def _build_association_table(self) -> Callable[[], Table]:
        if self.association is not None:
            table = self.association
        else:
            from .model import Table

            if not self.this.pk:
                raise RuntimeError(f"Table '{self.this.name}' must have a primary key to build a many-to-many relationship.")

            name = self._casing(f"association_{self.this.name}_{self.other.name}")
            this_col = Column(self._casing(f"{self.this.name}_{self.settings.fk_suffix}"), types.Integer, ForeignKey(self.this.pk))
            target_col = Column(self._casing(f"{self.other.name}_{self.settings.fk_suffix}"), types.Integer, ForeignKey(self.other.pk))
            table = Table(name, self.other.model.metadata, Column(self._casing("id"), types.Integer, primary_key=True), this_col, target_col)

        self.this.namespace[self._casing(f"{self.other.name}_{self.settings.association_table_suffix}")] = table
        setattr(self.other.model, self._casing(f"{self.this.name}_{self.settings.association_table_suffix}"), table)

        def _defer_create_table() -> Table:
            table.create()
            return table

        return _defer_create_table

    def _casing(self, text: str) -> str:
        return str(Str(text).case.from_enum(self.settings.casing))
