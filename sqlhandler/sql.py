from __future__ import annotations

import os

from typing import Any, Set, Dict, Union, TYPE_CHECKING


import numpy as np
import pandas as pd
import sqlalchemy as alch
from sqlalchemy.orm import backref, relationship
import pyodbc

from subtypes import Frame, Enum
from pathmagic import File
from miscutils.serializer import LostObject

from .custom import Model, Query, Session, StringLiteral, BitLiteral
from .expression import Select, Update, Insert, Delete, SelectInto
from .utils import StoredProcedure, Script
from .log import SqlLog
from .database import Database
from .config import Config

if TYPE_CHECKING:
    import alembic

assert pyodbc


class Sql:
    """
    Provides access to the complete sqlalchemy API, with custom functionality added for logging and pandas integration. Handles authentication through config settings and relects all schemas passed to the constructor.
    The custom expression classes provided have additional useful methods and are modified by the 'autocommit' attribute to facilitate human-supervised queries.
    The custom query class provided by the Alchemy object's 'session' attribute also has additional methods. Many commonly used sqlalchemy objects are bound to this object as attributes for easy access.
    """

    class IfExists(Enum):
        FAIL, REPLACE, APPEND = "fail", "replace", "append"

    def __init__(self, host: str = None, database: str = None, log: File = None, autocommit: bool = False, global_config: bool = False) -> None:
        self.engine = self._create_engine(host=host, database=database, global_config=global_config)
        self.session = Session(self.engine, sql=self)

        self.database = Database(self)

        self.log, self.autocommit = log, autocommit

        self.Select, self.SelectInto, self.Update = Select.from_sql(self), SelectInto.from_sql(self), Update.from_sql(self)
        self.Insert, self.Delete = Insert.from_sql(self), Delete.from_sql(self)
        self.StoredProcedure, self.Script = StoredProcedure.from_sql(self), Script.from_sql(self)

        self.text, self.literal = alch.text, alch.literal
        self.AND, self.OR, self.CAST, self.CASE, self.TRUE, self.FALSE = alch.and_, alch.or_, alch.cast, alch.case, alch.true(), alch.false()

        self.Table, self.Column, self.Relationship, self.Backref, self.ForeignKey, self.Index, self.CheckConstraint = alch.Table, alch.Column, relationship, backref, alch.ForeignKey, alch.Index, alch.CheckConstraint
        self.type, self.func, self.sqlalchemy = alch.types, alch.func, alch

    def __repr__(self) -> str:
        return f"{type(self).__name__}(engine={repr(self.engine)}, database={repr(self.database)})"

    def __len__(self) -> int:
        return len(self.database.meta.tables)

    def __enter__(self) -> Sql:
        self.session.rollback()
        return self

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        if ex_type is None:
            self.session.commit()
        else:
            self.session.rollback()

    def __getstate__(self) -> dict:
        return {"engine": LostObject(self.engine), "database": LostObject(self.database), "autocommit": self.autocommit, "_log": self.log}

    def __setstate__(self, attrs) -> None:
        self.__dict__ = attrs

    @property
    def Model(self) -> Model:
        return self.database.declaration

    @property
    def orm(self) -> Set[str]:
        return self.database.orm

    @property
    def objects(self) -> Set[str]:
        return self.database.objects

    @property
    def operations(self) -> alembic.operations.Operations:
        from alembic.migration import MigrationContext
        from alembic.operations import Operations

        return Operations(MigrationContext.configure(self.engine.connect()))

    @property
    def log(self) -> SqlLog:
        return self._log

    @log.setter
    def log(self, val: File) -> None:
        self._log = SqlLog(path=val, active=False)

    def initialize_log(self, logname: str, logdir: str = None) -> SqlLog:
        """Instantiates a matt.log.SqlLog object from a name and a dirpath, and binds it to this object's 'log' attribute. If 'active' argument is 'False', this method does nothing."""
        self._log = SqlLog.from_details(log_name=logname, log_dir=logdir, active=False)
        return self._log

    # Conversion Methods

    def query_to_frame(self, query: Query, labels: bool = False) -> Frame:
        """Convert sqlalchemy.orm.Query object to a pandas DataFrame. Optionally apply table labels to columns and/or print an ascii representation of the DataFrame."""
        query = query.with_labels() if labels else query

        result = self.session.execute(query.statement)
        cols = [col[0] for col in result.cursor.description]
        frame = Frame(result.fetchall(), columns=cols)

        return frame

    def plaintext_query_to_frame(self, query: str) -> Frame:
        """Convert plaintext SQL to a pandas DataFrame. The SQL statement must be a SELECT that returns rows."""
        return Frame(pd.read_sql_query(query, self.engine))

    def table_to_frame(self, table: str, schema: str = None) -> Frame:
        """Reads the target table or view (from the specified schema) into a pandas DataFrame."""
        return Frame(pd.read_sql_table(table, self.engine, schema=schema))

    def excel_to_table(self, filepath: os.PathLike, table: str = "temp", schema: str = None, if_exists: str = "fail", primary_key: str = "id", identity: bool = True, **kwargs: Any) -> Model:
        """Bulk insert the contents of the target '.xlsx' file to the specified table."""
        return self.frame_to_table(dataframe=Frame.from_excel(filepath, **kwargs), table=table, schema=schema, if_exists=if_exists, primary_key=primary_key, identity=identity)

    def frame_to_table(self, dataframe: pd.DataFrame, table: str, schema: str = None, if_exists: str = "fail", primary_key: str = "id") -> Model:
        """Bulk insert the contents of a pandas DataFrame to the specified table."""
        dataframe = Frame(dataframe)

        has_identity_pk = False
        if primary_key is None:
            dataframe.reset_index(inplace=True)
            primary_key = dataframe.iloc[:, 0].name
        else:
            if primary_key in dataframe.columns:
                dataframe.set_index(primary_key, inplace=True)
            else:
                has_identity_pk = True
                dataframe.reset_index(inplace=True, drop=True)
                dataframe.index.names = [primary_key]
                dataframe.index += 1

            dataframe.reset_index(inplace=True)

        dtypes = self._sql_dtype_dict_from_frame(dataframe)
        if has_identity_pk:
            dtypes.update({primary_key: alch.types.INT})

        dataframe.to_sql(engine=self.engine, name=table, if_exists=if_exists, index=False, index_label=None, primary_key=primary_key, schema=schema, dtype=dtypes)

        table_object = self.orm[schema][table]
        self.refresh_table(table=table_object)
        return self.orm[schema][table]

    @staticmethod
    def orm_to_frame(orm_objects: Any) -> Frame:
        """Convert a homogeneous list of sqlalchemy.orm instance objects (or a single one) to a pandas DataFrame."""
        if not isinstance(orm_objects, list):
            orm_objects = [orm_objects]

        if not all([type(orm_objects[0]) == type(item) for item in orm_objects]):
            raise TypeError("All sqlalchemy.orm mapped objects passed into this function must have the same type.")

        cols = [col.name for col in list(type(orm_objects[0]).__table__.columns)]
        vals = [[getattr(item, col) for col in cols] for item in orm_objects]

        return Frame(vals, columns=cols)

    def create_table(self, table: Union[Model, alch.schema.Table]) -> None:
        """Drop a table or the table belonging to an ORM class and remove it from the metadata."""
        self.database.create_table(table)

    def drop_table(self, table: Union[Model, alch.schema.Table]) -> None:
        """Drop a table or the table belonging to an ORM class and remove it from the metadata."""
        self.database.drop_table(table)

    def refresh_table(self, table: Union[Model, alch.schema.Table]) -> None:
        self.database.refresh_table(table=table)

    def clear_metadata(self) -> None:
        self.database.clear()

    # Private internal methods

    def _create_engine(self, host: str, database: str, global_config: bool) -> alch.engine.base.Engine:
        url = Config(global_config=global_config).generate_url(host=host, database=database)
        return alch.create_engine(str(url), echo=False, dialect=self._create_literal_dialect(url.get_dialect()))

    def _create_literal_dialect(self, dialect_class: alch.engine.default.DefaultDialect) -> alch.engine.default.DefaultDialect:
        class LiteralDialect(dialect_class):
            supports_multivalues_insert = True

            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                self.colspecs.update(
                    {
                        alch.sql.sqltypes.String: StringLiteral,
                        alch.sql.sqltypes.DateTime: StringLiteral,
                        alch.sql.sqltypes.Date: StringLiteral,
                        alch.sql.sqltypes.NullType: StringLiteral,
                        alch.dialects.mssql.BIT: BitLiteral
                    }
                )

        return LiteralDialect()

    @staticmethod
    def _sql_dtype_dict_from_frame(frame: Frame) -> Dict[str, Any]:
        def isnull(val: Any) -> bool:
            return val is None or np.isnan(val)

        def sqlalchemy_dtype_from_series(series: pd.code.series.Series) -> Any:
            if series.dtype.name in ["int64", "Int64"]:
                nums = [num for num in series if not isnull(num)]
                if not nums:
                    return alch.types.Integer
                else:
                    minimum, maximum = min(nums), max(nums)

                    if 0 <= minimum and maximum <= 255:
                        return alch.dialects.mssql.TINYINT
                    elif -2**15 <= minimum and maximum <= 2**15:
                        return alch.types.SmallInteger
                    elif -2**31 <= minimum and maximum <= 2**31:
                        return alch.types.Integer
                    else:
                        return alch.types.BigInteger
            elif series.dtype.name == "object":
                return alch.types.String(int((series.fillna("").astype(str).str.len().max()//50 + 1)*50))
            else:
                raise TypeError(f"Don't know how to process column type '{series.dtype}' of '{series.name}'.")

        return {name: sqlalchemy_dtype_from_series(col) for name, col in frame.infer_objects().iteritems() if col.dtype.name in ["int64", "Int64", "object"]}
