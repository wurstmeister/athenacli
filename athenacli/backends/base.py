# encoding: utf-8
"""Abstract base class for database backends."""

from abc import ABC, abstractmethod


class DatabaseBackend(ABC):
    """Abstract base class for database backend implementations.

    This class defines the interface that all database backends must implement
    to work with the CLI framework.
    """

    # SQL queries for metadata operations
    DATABASES_QUERY = 'SHOW DATABASES'
    TABLES_QUERY = 'SHOW TABLES'
    TABLE_COLUMNS_QUERY = '''
        SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema = '%s'
        ORDER BY table_name, ordinal_position
    '''

    def __init__(self, database=None):
        """Initialize backend with optional initial database.

        Args:
            database: Initial database/schema to connect to
        """
        self.database = database
        self.conn = None

    @abstractmethod
    def connect(self, database=None):
        """Establish database connection.

        Args:
            database: Optional database/schema to connect to.
                     If None, uses self.database

        Raises:
            Exception: If connection fails
        """
        pass

    @abstractmethod
    def close(self):
        """Close database connection."""
        pass

    def get_cursor(self):
        """Get a cursor from the current connection.

        Returns:
            Database cursor object

        Raises:
            Exception: If not connected
        """
        if not self.conn:
            raise Exception("Not connected to database")
        return self.conn.cursor()

    def tables(self):
        """Yields table names from current database.

        Yields:
            tuple: Table information rows
        """
        with self.get_cursor() as cur:
            cur.execute(self.TABLES_QUERY)
            for row in cur:
                yield row

    def table_columns(self):
        """Yields (table_name, column_name) tuples for current database.

        Yields:
            tuple: (table_name, column_name)
        """
        with self.get_cursor() as cur:
            cur.execute(self.TABLE_COLUMNS_QUERY % self.database)
            for row in cur:
                yield row

    def databases(self):
        """Get list of available databases.

        Returns:
            list: Database names
        """
        with self.get_cursor() as cur:
            cur.execute(self.DATABASES_QUERY)
            return [x[0] for x in cur.fetchall()]

    @abstractmethod
    def format_statistics(self, cursor):
        """Format execution statistics from cursor.

        Args:
            cursor: Database cursor after query execution

        Returns:
            str: Formatted statistics string
        """
        pass

    def supports_special_command(self, command):
        """Check if backend supports a special command.

        Args:
            command: Special command name (e.g., 'output_location')

        Returns:
            bool: True if supported
        """
        # By default, backends don't support special commands
        # Override in subclasses as needed
        return False
