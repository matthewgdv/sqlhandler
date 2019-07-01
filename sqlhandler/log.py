from __future__ import annotations

from typing import Any

from miscutils import PrintLog


class SqlLog(PrintLog):
    def __enter__(self, *args, **kwargs) -> PrintLog:
        self.activate()
        return super().__enter__(*args, **kwargs)

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        self.deactivate()
        return super().__exit__(ex_type=ex_type, ex_value=ex_value, ex_traceback=ex_traceback)

    def deactivate(self, openfile: bool = True) -> None:
        super().deactivate()

        if openfile:
            self.open()

    def write_comment(self, text: str, single_line_comment_cutoff: int = 5, add_newlines: int = 2) -> None:
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
        return super().from_details(log_name=log_name, file_extension=file_extension, log_dir=log_dir, active=active)
