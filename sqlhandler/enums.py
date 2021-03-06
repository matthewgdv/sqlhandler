from subtypes import Enum


class Dialect(Enum):
    """Enum of known dialect drivers."""
    MS_SQL = MY_SQL = SQLITE = POSTGRESQL = ORACLE = Enum.Auto()
