from __future__ import annotations

import os

from typing import Any, Set, Dict, TYPE_CHECKING


import numpy as np
import pandas as pd
import sqlalchemy as alch
from sqlalchemy.orm import aliased, backref, make_transient, relationship

from maybe import Maybe
from subtypes import Frame
from pathmagic import File
from miscutils import NullContext

from .custom import Base, Query, Session, Select, Update, Insert, Delete, SelectInto, StringLiteral, BitLiteral
from .utils import TempManager, StoredProcedure
from .log import SqlLog
from .database import DatabaseHandler
from .config import Config

if TYPE_CHECKING:
    import alembic


class Alchemy:
    """
    Provides access to the complete sqlalchemy API, with custom functionality added for logging and pandas integration. Handles authentication through config settings and relects all schemas passed to the constructor.
    The custom expression classes provided have additional useful methods and are modified by the 'autocommit' and 'printing' attributes (can be set at construction time) to facilitate human-supervised queries.
    The custom query class provided by the Alchemy object's 'session' attribute also has additional methods. Many commonly used sqlalchemy objects are bound to this object as attributes for easy access.
    """

    def __init__(self, host: str = None, database: str = None, log: File = None, printing: bool = False, autocommit: bool = False) -> None:
        self.engine = self._create_engine(host=host, database=database)
        self.session = Session.from_alchemy(self)(self.engine)

        self.database = DatabaseHandler(self)

        self.log, self.printing, self.autocommit = log, printing, autocommit

        self.Text, self.Literal, self.Case, self.Trans, self.Alias = alch.text, alch.literal, alch.case, make_transient, aliased
        self.AND, self.OR, self.CAST = alch.and_, alch.or_, alch.cast
        self.Select, self.SelectInto, self.Update = Select.from_alchemy(self), SelectInto.from_alchemy(self), Update.from_alchemy(self)
        self.Insert, self.Delete = Insert.from_alchemy(self), Delete.from_alchemy(self)
        self.StoredProcedure = StoredProcedure.from_alchemy(self)
        self.Table, self.Column, self.ForeignKey, self.Relationship, self.Backref = alch.Table, alch.Column, alch.ForeignKey, relationship, backref
        self.type, self.func, self.sqlalchemy = alch.types, alch.func, alch

        pd.set_option("max_columns", None)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(engine={repr(self.engine)}, num_tables={len(self)})"

    def __len__(self) -> int:
        return len(self.database.meta.tables)

    def __enter__(self) -> Alchemy:
        self.session.rollback()
        return self

    def __exit__(self, ex_type: Any, ex_value: Any, ex_traceback: Any) -> None:
        if ex_type is None:
            self.session.commit()
        else:
            self.session.rollback()

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
        self._log = SqlLog(logfile=val, active=False) if val is not None else NullContext()

    def initialize_log(self, logname: str, logdir: str = None) -> SqlLog:
        """Instantiates a matt.log.SqlLog object from a name and a dirpath, and binds it to this object's 'log' attribute. If 'active' argument is 'False', this method does nothing."""
        self._log = SqlLog.from_details(log_name=logname, log_dir=logdir, active=False)
        return self._log

    # Conversion Methods

    def query_to_frame(self, query: Query, labels: bool = False, print_ascii: bool = False) -> Frame:
        """Convert sqlalchemy.orm.Query object to a pandas DataFrame. Optionally apply table labels to columns and/or print an ascii representation of the DataFrame."""
        query = query.with_labels() if labels else query

        result = self.session.execute(query.statement)
        cols = [col[0] for col in result.cursor.description]
        frame = Frame(result.fetchall(), columns=cols)

        if print_ascii:
            print(frame.to_ascii())

        return frame

    def plaintext_query_to_frame(self, query: str) -> Frame:
        """Convert plaintext SQL to a pandas DataFrame. The SQL statement must be a SELECT that returns rows."""
        return Frame(pd.read_sql_query(query, self.engine))

    def table_to_frame(self, table: str, schema: str = None) -> Frame:
        """Reads the target table or view (from the specified schema) into a pandas DataFrame."""
        return Frame(pd.read_sql_table(table, self.engine, schema=schema))

    def excel_to_table(self, filepath: os.PathLike, tablename: str = "temp", schema: str = None, if_exists: str = "fail") -> Base:
        """Bulk insert the contents of the target '.xlsx' file to the specified table. The table is created with Primary Key 'id' field. Options for 'if_exists' are 'fail' (default), 'append', and 'replace'."""
        return self.frame_to_table(self.excel_to_frame(os.fspath(filepath)), table=tablename, schema=schema, if_exists=if_exists)

    def frame_to_table(self, dataframe: pd.DataFrame, table: str, schema: str = None, if_exists: str = "fail", primary_key: str = "id", identity: bool = True) -> Base:
        """Bulk insert the contents of a pandas DataFrame to the specified table. The table is created with a Primary Key 'id' field. Options for 'if_exists' are 'fail' (default), 'append', and 'replace'."""
        if primary_key is not None:
            if primary_key in dataframe.columns:
                raise ValueError(f"{type(dataframe).__name__} may not have a column named '{primary_key}'.")

            if identity:
                dataframe.reset_index(inplace=True, drop=True)
                dataframe.index += 1

        pk = Maybe(primary_key).else_(None)
        dtypes = self._sql_dtype_dict_from_frame(dataframe)
        dataframe.infer_dtypes().to_sql(engine=self.engine, name=table, if_exists=if_exists, index=primary_key is not None, index_label=pk, primary_key=pk, schema=schema, dtype=dtypes)

        self.refresh_table(table=table, schema=schema)
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

    @staticmethod
    def excel_to_frame(filepath: os.PathLike, sanitize_colnames: bool = True, **kwargs: Any) -> Frame:
        """Reads in the specified Excel spreadsheet into a pandas DataFrame. Passes on arguments to the pandas read_excel function. Optionally snake_cases column names and strips out non-ascii characters."""
        return Frame.from_excel(os.fspath(filepath), sanitize_colnames=sanitize_colnames, **kwargs)

    def resolve_tran(self, force_autocommit: bool = False) -> None:
        """Request user confirmation to resolve the ongoing transaction."""
        if self.autocommit or force_autocommit:
            self.session.commit()
            if self.printing:
                print("COMMIT;\n")
        else:
            user_confirmation = input("\nIf you are happy with the above Query/Queries please type COMMIT. Anything else will roll back the ongoing Transaction.\n\n")
            if user_confirmation.upper() == "COMMIT":
                self.session.commit()
                if self.printing:
                    print("\nCOMMIT;\n")
            else:
                self.session.rollback()
                if self.printing:
                    print("\nROLLBACK;\n")

    def refresh_table(self, table: alch.schema.Table, schema: str = None) -> None:
        self.database.refresh_table(table=table, schema=schema)

    def drop_table(self, table: alch.schema.Table) -> None:
        """Drop a table or the table belonging to an ORM class and remove it from the metadata."""
        self.database.drop_table(table)

    def clear_metadata(self) -> None:
        self.database.clear()

    # Private internal methods

    def _create_engine(self, host: str, database: str) -> alch.engine.base.Engine:
        url = Config().generate_url(host=host, database=database)
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
                minimum, maximum = min(nums), max(nums)

                if 0 <= minimum and maximum <= 255:
                    return alch.dialects.mssql.TINYINT
                elif -2**15 <= minimum and maximum <= 2**15:
                    return alch.types.SMALLINT
                elif -2**31 <= minimum and maximum <= 2**31:
                    return alch.types.INT
                else:
                    return alch.types.BIGINT
            elif series.dtype.name == "object":
                return alch.types.String(int((series.fillna("").astype(str).str.len().max()//50 + 1)*50))
            else:
                raise TypeError(f"Don't know how to process column type '{series.dtype}' of '{series.name}'.")

        return {name: sqlalchemy_dtype_from_series(col) for name, col in frame.infer_objects().iteritems() if col.dtype.name in ["int64", "Int64", "object"]}
