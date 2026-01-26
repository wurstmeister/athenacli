# encoding: utf-8

import logging
import sqlparse

from athenacli.packages import special
from athenacli.packages.format_utils import format_status

logger = logging.getLogger(__name__)


class SQLExecute(object):
    """SQL execution wrapper that uses a database backend abstraction.

    This class provides a consistent interface for executing SQL queries
    regardless of the underlying database backend (Athena, Redshift, etc.).
    """

    def __init__(self, backend):
        """Initialize SQLExecute with a database backend.

        Args:
            backend: DatabaseBackend instance (AthenaBackend, RedshiftBackend, etc.)
        """
        self.backend = backend
        self.database = backend.database

    def connect(self, database=None):
        """Connect to database using the backend.

        Args:
            database: Optional database to connect to
        """
        self.backend.connect(database)
        self.database = self.backend.database

    @property
    def conn(self):
        """Get connection from backend for compatibility."""
        return self.backend.conn

    @property
    def region_name(self):
        """Get region name from backend if available."""
        return getattr(self.backend, 'region_name', None)

    def run(self, statement):
        '''Execute the sql in the database and return the results.

        The results are a list of tuples. Each tuple has 4 values
        (title, rows, headers, status).
        '''
        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            yield (None, None, None, None)

        # Split the sql into separate queries and run each one.
        components = sqlparse.split(statement)

        for sql in components:
            # Remove spaces, eol and semi-colons.
            sql = sql.rstrip(';')

            # \G is treated specially since we have to set the expanded output.
            if sql.endswith('\\G'):
                special.set_expanded_output(True)
                sql = sql[:-2].strip()

            cur = self.backend.get_cursor()

            try:
                for result in special.execute(cur, sql):
                    yield result
            except special.CommandNotFound:  # Regular SQL
                cur.execute(sql)
                yield self.get_result(cur)

    def get_result(self, cursor):
        '''Get the current result's data from the cursor.'''
        title = headers = None

        # Set output location if backend supports it (Athena-specific)
        if self.backend.supports_special_command('output_location') and hasattr(cursor, 'output_location'):
            special.set_output_location(cursor.output_location)

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT or SHOW.
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            rows = cursor.fetchall()
            status = format_status(rows_length=len(rows), cursor=cursor, backend=self.backend)
        else:
            logger.debug('No rows in result.')
            rows = None
            status = format_status(rows_length=None, cursor=cursor, backend=self.backend)
        return (title, rows, headers, status)

    def tables(self):
        '''Yields table names.'''
        return self.backend.tables()

    def table_columns(self):
        '''Yields column names.'''
        return self.backend.table_columns()

    def databases(self):
        '''Get list of database names.'''
        return self.backend.databases()
