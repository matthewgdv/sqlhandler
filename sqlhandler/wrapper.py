# Module imports
from __future__ import annotations

import contextlib
from typing import Any, Union, TYPE_CHECKING

import sqlalchemy as alch
from subtypes import Frame

from .utils import literalstatement

if TYPE_CHECKING:
    from .custom import Select, Update, Insert, Delete, SelectInto


class ExpressionWrapper:
    def __init__(self, expression: Union[Select, Update, Insert, Delete, SelectInto], silently: bool = False) -> None:
        from .custom import Select

        self.expression, self.alchemy, self.silently = expression, expression.alchemy, silently
        self.pre_select = self.pre_select_from_select = self.post_select = self.post_select_inserts = self.post_select_all = self.force_autocommit = False

        if isinstance(expression, (Select, alch.sql.Select)):
            self._select_wrapper()
        else:
            self._determine_attrs()
            self._prepare_tran()

            if self.pre_select and self.alchemy.printing and not self.silently:
                self._perform_pre_select()

            if self.pre_select_from_select and self.expression.select is not None:
                self._perform_pre_select_from_select()

            self.result = self.alchemy.session.execute(self.expression)
            self._determine_rowcount()

            if self.alchemy.printing or not self.alchemy.autocommit:
                self._print_literal_sql_statement()

            if not self.rowcount == -1:
                self._print_and_or_log_rowcount()

            if self.post_select and self.alchemy.printing and not self.silently:
                self._perform_post_select()

            if self.post_select_inserts and self.alchemy.printing and not self.silently:
                self._perform_post_select_inserts()

            if self.post_select_all:
                self._perform_post_select_all()

            self.alchemy.resolve_tran(force_autocommit=self.force_autocommit)

    def _determine_attrs(self) -> None:
        from .custom import Update, Insert, Delete, SelectInto

        expression_settings = {
            Update: ["pre_select", "post_select"],
            Insert: ["post_select_inserts", "pre_select_from_select"],
            Delete: ["pre_select"],
            SelectInto: ["post_select_all", "force_autocommit"]
        }

        for attr in expression_settings[type(self.expression)]:
            setattr(self, attr, True)

    def _select_wrapper(self) -> None:
        result = self.alchemy.session.execute(self.expression)
        cols = [col[0] for col in result.cursor.description]
        self.frame = Frame(result.fetchall(), columns=cols)

        if self.alchemy.printing:
            print(literalstatement(self.expression), end="\n\n")
            print(self.frame.to_ascii(), end="\n\n")

    def _prepare_tran(self) -> None:
        self.alchemy.session.rollback()
        if self.alchemy.printing:
            print(f"{'-' * 200}\n\nBEGIN TRAN;", end="\n\n")

    def _perform_pre_select(self) -> None:
        self.pre_select_object = self.alchemy.Select(["*"]).select_from(self.expression.table)
        if self.expression._whereclause is not None:
            self.pre_select_object = self.pre_select_object.where(self.expression._whereclause)
        self.pre_select_object.frame()

    def _perform_pre_select_from_select(self) -> None:
        with self.no_logging_or_printing_context() if (not self.alchemy.printing or self.silently) and self.alchemy.log is not None and self.alchemy.log.active else contextlib.nullcontext():
            self.rowcount = len(ExpressionWrapper(self.expression.select, silently=self.silently).frame.index)

    def _determine_rowcount(self) -> None:
        from .custom import Insert

        if not self.result.rowcount == -1:
            self.rowcount = self.result.rowcount
        elif isinstance(self.expression, Insert):
            if self.expression.select is None:
                self.rowcount = len(self.expression.parameters) if isinstance(self.expression.parameters, list) else 1
            else:
                pass
        else:
            self.rowcount = -1

    def _print_literal_sql_statement(self) -> None:
        print(literalstatement(self.expression), end="\n\n")

    def _print_and_or_log_rowcount(self) -> None:
        if self.alchemy.printing or not self.alchemy.autocommit:
            print(f"({self.rowcount} row(s) affected)", end="\n\n")
        if self.alchemy.log is not None:
            self.alchemy.log.write(f"-- ({self.rowcount} row(s) affected)")

    def _perform_post_select(self) -> None:
        self.pre_select_object.frame()

    def _perform_post_select_inserts(self) -> None:
        table = self.expression.table
        self.alchemy.Select(["*"]).select_from(table).order_by(getattr(table.columns, list(table.primary_key)[0].name).desc()).limit(self.rowcount).frame()

    def _perform_post_select_all(self) -> None:
        self.alchemy.Select(["*"]).select_from(self.alchemy.Text(f"{self.expression.into}")).frame()

    @contextlib.contextmanager
    def no_logging_or_printing_context(self) -> Any:
        printing = self.alchemy.printing
        self.alchemy.printing = self.alchemy.log.active = False

        try:
            yield None
        finally:
            self.alchemy.session.rollback()
            self.alchemy.log.active, self.alchemy.printing = True, printing
