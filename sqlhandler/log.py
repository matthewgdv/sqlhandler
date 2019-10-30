from __future__ import annotations

from typing import Any

from iotools import PrintLog


class SqlLog(PrintLog):
    """A logger designed to leave behind properly formatted SQL scripts from any SQL statements executed using the '.resolve()' method of the custom expression classes."""

    def __enter__(self, *args, **kwargs) -> PrintLog:
        if self._path is not None:
            self.activate()
            return self

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        self.deactivate()

    def deactivate(self, openfile: bool = True) -> None:
        """Deactivate this log and optionally open the log file."""
        if self.active:
            super().deactivate()
            if openfile:
                self.start()

    def write_comment(self, text: str, single_line_comment_cutoff: int = 5, add_newlines: int = 2) -> None:
        """Write a SQL comment to the log in either short or long-form depending on the 'single_line_comment_cutoff'."""
        if not self.active or self.to_console:
            super().write(text=text, to_console=True, to_file=False, add_newlines=add_newlines)

        if self.active and self.to_file:
            if text.strip().count("\n") <= single_line_comment_cutoff:
                text = "-- " + text.strip().replace("\n", "\n-- ")
            else:
                text = "/*\n" + text.strip() + "\n*/"

            super().write(text=text, to_console=False, to_file=True, add_newlines=add_newlines)

    @classmethod
    def from_details(cls, log_name: str, log_dir: str = None, active: bool = True, file_extension: str = "sql") -> SqlLog:
        """Create a SqlLog from several arguments."""
        return super().from_details(log_name=log_name, file_extension=file_extension, log_dir=log_dir, active=active)
