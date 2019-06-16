from __future__ import annotations

import datetime
import logging
import os
import re as witchcraft
from typing import Any

import sqlparse
import tabulate

from subtypes import DateTime, Str
from miscutils import Log

assert datetime, DateTime


class SqlLog(Log):
    def __init__(self, logfile: os.PathLike, active: bool = True) -> None:
        self.logger: logging.Logger = None
        self.log_handler: logging.Handler = None
        super().__init__(logfile=logfile, active=active)

    def __enter__(self) -> Log:
        self.activate()
        return self

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        self.deactivate()

    def activate(self) -> None:
        super().activate()
        if self.logger is None:
            self.logger = logging.getLogger('sqlalchemy.engine')

        if self.log_handler is None:
            self.log_handler = logging.FileHandler(self.file.path)
            self.log_handler.setFormatter(logging.Formatter('- %(levelname)s - %(message)s'))
            self.log_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(self.log_handler)

        self.logger.setLevel(logging.DEBUG)

    def deactivate(self, openfile: bool = True) -> None:
        super().deactivate()
        SqlProcessor(self)

        self.logger.setLevel(logging.ERROR)
        self.logger.removeHandler(self.log_handler)
        self.log_handler.close()
        self.log_handler = None

        if openfile:
            self.open()

    def log_statements_only(self, statements_only: bool = False) -> None:
        if statements_only:
            self.log_handler.setLevel(logging.INFO)
        else:
            if self._active:
                self.log_handler.setLevel(logging.DEBUG)
            else:
                self.log_handler.setLevel(logging.ERROR)

    def write_comment(self, text: str, single_line_comment_cutoff: int = 5, add_newlines: int = 2) -> None:
        if self._active:
            if text.strip().count("\n") <= single_line_comment_cutoff:
                self.file.contents += "-- " + text.strip().replace("\n", "\n-- ")
            else:
                self.file.contents += "/*\n" + text.strip() + "\n*/"
            self.file.contents += "\n" * add_newlines

    @classmethod
    def from_details(cls, log_name: str, log_dir: str = None, active: bool = True, file_extension: str = "sql") -> SqlLog:
        return super().from_details(log_name=log_name, file_extension=file_extension, log_dir=log_dir, active=active)


class SqlProcessor:
    def __init__(self, log: SqlLog) -> None:
        self.log = log

        if any([line.startswith("- ") for line in self.log.file]):
            self.process_datetimes_nones_and_bools()
            self.collapse_multi_lines()
            self.process_trans()
            self.bind_params()
            self.beautify_queries_and_add_tables()
            self.final_formatting()

    def process_datetimes_nones_and_bools(self) -> None:
        stage1 = witchcraft.sub(r"[Dd]ate[Tt]ime(\.[a-z]{4,8})?\(([0-9]+, )+[0-9]+\)", lambda m: f"'{eval(m.group())}'", self.log.file.contents)
        stage2 = witchcraft.sub(r"- DEBUG - Row.*[( ]None[,)]", lambda m: m.group().replace("None", "'NULL'"), stage1)
        stage3 = witchcraft.sub(r"[( ]None[,)]", lambda m: m.group().replace("None", "NULL"), stage2)
        stage4 = witchcraft.sub(r"[( ]True[,)]", lambda m: m.group().replace("True", "1"), stage3)
        stage5 = witchcraft.sub(r"[( ]False[,)]", lambda m: m.group().replace("False", "0"), stage4)
        self.log.file.contents = stage5

    def collapse_multi_lines(self) -> None:
        invalid_indices = []
        for index, row in enumerate(self.log.file):
            if row.startswith("- DEBUG - Col"):
                col_row = f"- TABLE - (" + Str(row).after_first(r"\(")
                next_index = index + 1
                next_row = self.log.file[next_index]
                while next_row.startswith("- DEBUG - Row"):
                    col_row += f" - (" + Str(next_row).after_first(r"\(")
                    invalid_indices.append(next_index)
                    next_index += 1
                    next_row = self.log.file[next_index]
                self.log.file[index] = col_row
            elif row.startswith("- INFO - ") and not index + 1 == len(self.log.file) - 1 and not self.log.file[index + 1].startswith("-"):
                query = f"- INFO - {row[9:]}"
                next_index = index + 1
                next_row = self.log.file[next_index]
                while not (next_row.startswith("-") or next_row == ""):
                    query += next_row
                    invalid_indices.append(next_index)
                    next_index += 1
                    next_row = self.log.file[next_index]
                self.log.file[index] = query
            elif row == "- INFO - ()":
                invalid_indices.append(index)
        self.log.file.contents = "\n".join([line for index, line in enumerate(self.log.file.contents.split("\n")) if index not in invalid_indices])

    def process_trans(self) -> None:
        aslist = self.log.file.contents.split("\n")
        for index, line in enumerate(aslist):
            if line.startswith("- "):
                if line == "- INFO - BEGIN (implicit)":
                    aslist[index] = "\nBEGIN TRAN;\n"
                elif line == "- INFO - COMMIT":
                    aslist[index] = f"COMMIT;\n\n{'-' * 200}"
                elif line == "- INFO - ROLLBACK":
                    aslist[index] = f"ROLLBACK;\n\n{'-' * 200}"
        self.log.file.contents = "\n".join(aslist)

    def bind_params(self) -> None:
        invalid_indices = []
        for index, row in enumerate(self.log.file):
            if row.startswith("- INFO - ") and "?" in row:
                next_row = self.log.file[index + 1]
                unbound_vals = Str(next_row).after_first(r"\(")[:-1].split(", ")
                unbound_vals = [val[:-1] if len(unbound_vals) == 1 and val.endswith(",") else val for val in unbound_vals]
                self.log.file[index] = row.replace("?", r"{}").format(*unbound_vals)
                invalid_indices.append(index + 1)
        self.log.file.contents = "\n".join([line for index, line in enumerate(self.log.file.contents.split("\n")) if index not in invalid_indices])

    def beautify_queries_and_add_tables(self) -> None:
        lines = self.log.file.contents.split("\n")
        lines = [f"{sqlparse.format(line[9:], reindent=True, keyword_case='upper', wrap_after=500)};\n" if line.startswith("- INFO - ") else line for line in lines]

        reformatted = []
        for index, line in enumerate(lines):
            if line.count(";") > 1:
                reformatted.append(f"{Str(line).before_first(';')};\n")
            elif line == "- TABLE - ('',)":
                pass
            elif line.startswith("- TABLE - "):
                line_as_list = line.split(" - ")
                cols = eval(line_as_list[1])
                rows = [eval(row) for row in line_as_list[2:]]
                reformatted.append(f"/*\n{tabulate.tabulate(rows, headers=cols, tablefmt='grid')}\n*/\n")
            else:
                reformatted.append(line)

        self.log.file.contents = "\n".join(reformatted)

    def final_formatting(self) -> None:
        self.log.file.contents = witchcraft.sub(r"OVER \(\s*", lambda m: m.group().strip(), self.log.file.contents)
