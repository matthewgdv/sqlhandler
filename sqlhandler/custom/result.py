from __future__ import annotations

from functools import cached_property
from typing import Any, Optional

from miscutils import ReprMixin

from sqlalchemy.engine import IteratorResult, Row, RowMapping


class Result(ReprMixin):
    def __init__(self, raw_result: IteratorResult) -> None:
        self.raw = raw_result.freeze()
        self.rowcount = raw_result.raw.rowcount

    @cached_property
    def columns(self) -> list[str]:
        return list(self.raw().keys())

    @cached_property
    def scalar(self) -> ScalarAccessor:
        return ScalarAccessor(self)

    @cached_property
    def mapping(self) -> MappingAccessor:
        return MappingAccessor(self)

    @cached_property
    def first(self) -> Row:
        return self.raw().first()

    @cached_property
    def one(self) -> Row:
        return self.raw().one()

    @cached_property
    def one_or_none(self) -> Optional[Row]:
        return self.raw().one_or_none()

    @cached_property
    def all(self) -> list[Row]:
        return self.raw().all()


class ScalarAccessor(ReprMixin):
    def __init__(self, parent: Result) -> None:
        self.parent = parent

    @cached_property
    def first(self) -> Any:
        return self.parent.raw().scalars().first()

    @cached_property
    def one(self) -> Any:
        return self.parent.raw().scalars().one()

    @cached_property
    def one_or_none(self) -> Optional[Any]:
        return self.parent.raw().scalars().one_or_none()

    @cached_property
    def all(self) -> list[Any]:
        return self.parent.raw().scalars().all()


class MappingAccessor(ReprMixin):
    def __init__(self, parent: Result) -> None:
        self.parent = parent

    @cached_property
    def first(self) -> RowMapping:
        return self.parent.raw().mappings().first()

    @cached_property
    def one(self) -> RowMapping:
        return self.parent.raw().mappings().one()

    @cached_property
    def one_or_none(self) -> Optional[RowMapping]:
        return self.parent.raw().mappings().one_or_none()

    @cached_property
    def all(self) -> list[RowMapping]:
        return self.parent.raw().mappings().all()
