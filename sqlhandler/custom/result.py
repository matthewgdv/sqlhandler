from __future__ import annotations

from functools import cached_property
from typing import Any, Optional, TYPE_CHECKING

from miscutils import ReprMixin

from sqlalchemy.engine import IteratorResult, Row, RowMapping

from sqlhandler.frame import Frame

if TYPE_CHECKING:
    from sqlhandler import Sql


class Result(ReprMixin):
    def __init__(self, raw_result: IteratorResult, sql: Sql) -> None:
        self.raw = raw_result
        self.sql = sql

        try:
            self.frozen = raw_result.freeze()
        except Exception:
            self.frozen = None

    @cached_property
    def rowcount(self) -> Optional[int]:
        try:
            return self.raw.raw.rowcount
        except AttributeError:
            return None

    @cached_property
    def columns(self) -> list[str]:
        return list(self.frozen().keys())

    @cached_property
    def scalar(self) -> ScalarAccessor:
        return ScalarAccessor(self)

    @cached_property
    def mapping(self) -> MappingAccessor:
        return MappingAccessor(self)

    @cached_property
    def first(self) -> Row:
        return self.frozen().first()

    @cached_property
    def one(self) -> Row:
        return self.frozen().one()

    @cached_property
    def one_or_none(self) -> Optional[Row]:
        return self.frozen().one_or_none()

    @cached_property
    def all(self) -> list[Row]:
        return self.frozen().all()

    @cached_property
    def frame(self) -> Frame:
        return self.sql.Constructors.Frame(self.mapping.all)


class ScalarAccessor(ReprMixin):
    def __init__(self, parent: Result) -> None:
        self.parent = parent

    @cached_property
    def first(self) -> Any:
        return self.parent.frozen().scalars().first()

    @cached_property
    def one(self) -> Any:
        return self.parent.frozen().scalars().one()

    @cached_property
    def one_or_none(self) -> Optional[Any]:
        return self.parent.frozen().scalars().one_or_none()

    @cached_property
    def all(self) -> list[Any]:
        return self.parent.frozen().scalars().all()


class MappingAccessor(ReprMixin):
    def __init__(self, parent: Result) -> None:
        self.parent = parent

    @cached_property
    def first(self) -> RowMapping:
        return self.parent.frozen().mappings().first()

    @cached_property
    def one(self) -> RowMapping:
        return self.parent.frozen().mappings().one()

    @cached_property
    def one_or_none(self) -> Optional[RowMapping]:
        return self.parent.frozen().mappings().one_or_none()

    @cached_property
    def all(self) -> list[RowMapping]:
        return self.parent.frozen().mappings().all()
