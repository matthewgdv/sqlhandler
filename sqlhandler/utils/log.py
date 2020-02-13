from __future__ import annotations

from typing import Any

from iotools import PrintLog


class SqlLog(PrintLog):
    """A logger designed to leave behind properly formatted SQL scripts from any SQL statements executed using the '.resolve()' method of the custom expression classes."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.to_file = False

    def write_sql(self, text: str, add_newlines: int = 2) -> None:
        """Write a SQL statement to the log if the log is active. Always write it to the stream."""
        self.write(text=text, to_stream=True, to_file=False, add_newlines=add_newlines)

        if self.active:
            self.write(text=text, to_stream=False, to_file=True, add_newlines=add_newlines)

    def write_comment(self, text: str, single_line_comment_cutoff: int = 5, add_newlines: int = 2) -> None:
        """Write a SQL comment to the log if the log is active in either short or long-form depending on the 'single_line_comment_cutoff'. Always write it to the stream."""
        self.write(text=text, to_stream=True, to_file=False, add_newlines=add_newlines)

        if self.active:
            if text.strip().count("\n") <= single_line_comment_cutoff:
                text = "-- " + text.strip().replace("\n", "\n-- ")
            else:
                text = "/*\n" + text.strip() + "\n*/"

            self.write(text=text, to_stream=False, to_file=True, add_newlines=add_newlines)

    @classmethod
    def from_details(cls, log_name: str, log_dir: str = None, active: bool = True, file_extension: str = "sql") -> SqlLog:
        """Create a SqlLog from several arguments."""
        return super().from_details(log_name=log_name, file_extension=file_extension, log_dir=log_dir, active=active)
