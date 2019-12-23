PLEASE NOTE:
====================

This library is currently still under development. The API will likely undergo significant changes that may break any code you write with it.
The documentation will fall out of sync with the updates regularly until development slows down. Use it at your own risk.

Overview
====================

Provides a thick wrapper around some SQLAlchemy operations with lots of conveniences:

The `Sql` class
--------------------

* Central handler class
* Automatically start connections based on prior configuration
* Preconfigured SQLAlchemy `Session` subclass bound which produces special `Query` objects with its `Session.query()` method.
* Unique `Database` object bound to its `Sql.database` attribute, which manages the SQLAlchemy `MetaData`, `DeclarativeBase`, and `AutomapBase` objects
* Produce a pre-configured alembic `Operations` object for simple programmatic migrations
* Create a table from a Pandas `DataFrame` or subtypes `Frame`, with an autoincrementing primary key
* Log class that captures raw SQL with inline literal binds from custom expression classes (`Select`, `Insert`, `Update`, `Delete`) when activated
* Create and drop table operations that work on mapped classes and raw tables, which update the metadata and database accessors to keep everything in sync
* Provides access to the most common parts of the SQLAlchemy API

The `Config` class
--------------------

* Simple API for configuring database URLs that can be easily reused
* When Providing a default host and database, `Sql` will no longer require arguments when connecting
* Add or remove config programatically as well as by importing/exporting JSON files

The `Model` class
--------------------

* A SQLAlchemy declarative base with a few utility methods
* `Model.insert()`, `Model.update()`, and `Model.delete()` will perform the respective operations within their bound session
* `Model.frame()` converts a record to a `subtypes.Frame` with a single row
* Other classmethods: `Model.create()`, `Model.drop()`, `Model.query()`, `Model.join()` (for cleaner joins in expression constructs), `Model.c()` (easier access to the table
  columns), and `Model.alias()`

The `Query` class
--------------------

* Alias methods `Query.from_()` (`Query.select_from()`), `Query.where()` (`Query.filter()`), `Query.set_()` (`Query.update()`, with automatic 'fetch' behaviour)
* `Query.frame()` method for conversion to `subtypes.Frame`
* `Query.scalar_col()` method for conversion of a single column to a `list`
* Implemented string magic method producing the query with inline literal binds

The `Database` class
--------------------

* `Database.orm` and `Database.objects` attributes can be used to access database objects via attribute access (eg `Database.orm.log.main`)
* `Database.orm` yields mapped classes, but only for database objects with a primary key
* `Database.objects` yields raw table objects, and should allow access to any database object, even those without a primary key, views, etc.
* Database reflection occurs automatically when attempting to access a schema or database object using these accessors
* The `MetaData` is automatically cached for 5 days after each reflection operation, causing the `Database` object to start with pre-populated schemas on subsequent instanciation

Custom expression classes
--------------------

* `Select`, `Update`, `Insert`, and `Delete` subclasses with various extra methods
* `.resolve()` method facilitates performing queries with user interaction



Installation
====================

To install use pip:

    $ pip install sqlhandler


Or clone the repo:

    $ git clone https://github.com/matthewgdv/sqlhandler.git
    $ python setup.py install


Usage
====================

Usage coming soon!

Contributing
====================

Contributions are welcome, and they are greatly appreciated! Every little bit helps, and credit will always be given.

You can contribute in many ways:

Report Bugs
--------------------

Report bugs at https://github.com/matthewgdv/sqlhandler/issues

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting.
* Detailed steps to reproduce the bug.

Fix Bugs
--------------------

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help wanted" is open to whoever wants to implement a fix for it.

Implement Features
--------------------

Look through the GitHub issues for features. Anything tagged with "enhancement" and "help wanted" is open to whoever wants to implement it.

Write Documentation
--------------------

The repository could always use more documentation, whether as part of the official docs, in docstrings, or even on the web in blog posts, articles, and such.

Submit Feedback
--------------------

The best way to send feedback is to file an issue at https://github.com/matthewgdv/sqlhandler/issues.

If you are proposing a new feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that contributions are welcome :)

Get Started!
--------------------

Before you submit a pull request, check that it meets these guidelines:

1.  If the pull request adds functionality, it should include tests and the docs should be updated. Write docstrings for any functions that are part of the external API, and add
    the feature to the README.md.

2.  If the pull request fixes a bug, tests should be added proving that the bug has been fixed. However, no update to the docs is necessary for bugfixes.

3.  The pull request should work for the newest version of Python (currently 3.7). Older versions may incidentally work, but are not officially supported.

4.  Inline type hints should be used, with an emphasis on ensuring that introspection and autocompletion tools such as Jedi are able to understand the code wherever possible.

5.  PEP8 guidelines should be followed where possible, but deviations from it where it makes sense and improves legibility are encouraged. The following PEP8 error codes can be
    safely ignored: E121, E123, E126, E226, E24, E704, W503

6.  This repository intentionally disallows the PEP8 79-character limit. Therefore, any contributions adhering to this convention will be rejected. As a rule of thumb you should
    endeavor to stay under 200 characters except where going over preserves alignment, or where the line is mostly non-algorythmic code, such as extremely long strings or function
    calls.
