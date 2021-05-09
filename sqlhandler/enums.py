from subtypes import Enum


class Enums:
    class Dialect(Enum):
        """Enum of known dialect drivers."""
        MS_SQL = MY_SQL = SQLITE = POSTGRESQL = ORACLE = Enum.Auto()

    class IfExists(Enum):
        """Enum describing operation if a table being created already exists."""
        FAIL = REPLACE = APPEND = Enum.Auto()

    class InferRange(Enum):
        TRIM_SURROUNDING = STRIP_NULLS = SMALLEST_VALID = Enum.Auto()

    class PathType(Enum):
        PATHMAGIC = PATHLIB = STRING = Enum.Auto()
