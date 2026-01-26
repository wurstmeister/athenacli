# encoding: utf-8
"""Amazon Redshift database backend."""

import logging
import os

try:
    import psycopg2
    from psycopg2 import extensions
except ImportError:
    psycopg2 = None

try:
    import boto3
except ImportError:
    boto3 = None

from athenacli.backends.base import DatabaseBackend

logger = logging.getLogger(__name__)


class RedshiftBackend(DatabaseBackend):
    """Amazon Redshift backend implementation using psycopg2."""

    # Redshift-specific queries (PostgreSQL compatible)
    DATABASES_QUERY = 'SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname'
    # Query all user schemas (exclude system schemas like pg_catalog, information_schema)
    # Return schema and table/view separately to avoid quoting issues
    # Union tables and views together
    TABLES_QUERY = """
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
        UNION ALL
        SELECT schemaname, viewname
        FROM pg_views
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
        ORDER BY 1, 2
    """

    def __init__(
        self,
        host=None,
        port=5439,
        database=None,
        user=None,
        password=None,
        sslmode='prefer',
        connect_timeout=None,
        aws_profile=None,
        region=None,
        **kwargs
    ):
        """Initialize Redshift backend.

        Args:
            host: Redshift cluster endpoint
            port: Port number (default: 5439)
            database: Initial database name
            user: Database user (can be prefixed with "IAM:" for IAM auth)
            password: Database password (if None, will attempt IAM auth)
            sslmode: SSL mode (prefer, require, disable)
            connect_timeout: Connection timeout in seconds
            aws_profile: AWS profile for IAM authentication
            region: AWS region (default: us-east-1)
            **kwargs: Additional psycopg2 connection parameters
        """
        if psycopg2 is None:
            raise ImportError(
                "psycopg2 is required for Redshift support. "
                "Install with: pip install psycopg2-binary"
            )

        super().__init__(database=database)

        self.host = host
        self.port = port
        self.sslmode = sslmode
        self.connect_timeout = connect_timeout
        self.extra_params = kwargs
        self.aws_profile = aws_profile or os.environ.get('AWS_PROFILE', 'default')
        self.region = region or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')

        # Parse user - strip "IAM:" prefix if present
        if user and user.startswith('IAM:'):
            self.user = user[4:]  # Remove "IAM:" prefix
            self.use_iam = True
        else:
            self.user = user
            self.use_iam = False

        # If no password provided, enable IAM authentication
        if password is None and self.user:
            self.use_iam = True
            self.password = None
        else:
            self.password = password

        self.connect()

    def _get_iam_credentials(self, database):
        """Get temporary IAM credentials for Redshift.

        Args:
            database: Database name for credentials

        Returns:
            tuple: (db_user, db_password) with IAM credentials

        Raises:
            ImportError: If boto3 is not available
            Exception: If unable to get credentials
        """
        if boto3 is None:
            raise ImportError(
                "boto3 is required for IAM authentication. "
                "Install with: pip install boto3"
            )

        # Extract cluster identifier from hostname
        # Format: cluster-name.region.redshift.amazonaws.com
        cluster_id = self.host.split('.')[0] if self.host else None
        if not cluster_id:
            raise ValueError("Cannot extract cluster identifier from host")

        logger.info("Getting IAM credentials for Redshift cluster: %s", cluster_id)

        try:
            session = boto3.Session(profile_name=self.aws_profile)
            redshift_client = session.client('redshift', region_name=self.region)

            response = redshift_client.get_cluster_credentials(
                DbUser=self.user,
                DbName=database or 'dev',
                ClusterIdentifier=cluster_id,
                DurationSeconds=3600,  # 1 hour
                AutoCreate=False
            )

            db_user = response['DbUser']
            db_password = response['DbPassword']

            logger.info("Successfully obtained IAM credentials for user: %s", db_user)
            return db_user, db_password

        except Exception as e:
            logger.error("Failed to get IAM credentials: %s", e)
            raise

    def connect(self, database=None):
        """Establish connection to Redshift.

        Args:
            database: Optional database to connect to
        """
        db_name = database or self.database or 'dev'
        db_user = self.user
        db_password = self.password

        # Get IAM credentials if needed
        if self.use_iam and not db_password:
            db_user, db_password = self._get_iam_credentials(db_name)

        conn_params = {
            'host': self.host,
            'port': self.port,
            'database': db_name,
            'user': db_user,
            'password': db_password,
            'sslmode': self.sslmode,
        }

        if self.connect_timeout:
            conn_params['connect_timeout'] = self.connect_timeout

        # Add any extra parameters
        conn_params.update(self.extra_params)

        # Remove None values
        conn_params = {k: v for k, v in conn_params.items() if v is not None}

        try:
            conn = psycopg2.connect(**conn_params)
            # Set autocommit mode for DDL statements
            conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            self.database = db_name

            if self.conn:
                self.conn.close()
            self.conn = conn

            logger.debug("Connected to Redshift: %s@%s:%s/%s",
                        db_user, self.host, self.port, self.database)
        except psycopg2.Error as e:
            logger.error("Failed to connect to Redshift: %s", e)
            raise

    def close(self):
        """Close Redshift connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def format_statistics(self, cursor):
        """Format Redshift execution statistics.

        Args:
            cursor: psycopg2 cursor after query execution

        Returns:
            str: Formatted statistics (Redshift provides limited statistics)
        """
        if not cursor:
            return ''

        # Redshift/PostgreSQL doesn't provide execution time or data scanned
        # directly from the cursor. For now, return minimal statistics.
        # Future enhancement: Query system tables for detailed stats
        return ''

    def tables(self):
        """Yields fully-qualified table names from current database.

        Overrides base implementation to concatenate schema.table in Python
        to avoid PostgreSQL quoting issues.

        Yields:
            tuple: Single-element tuple with schema-qualified table name
        """
        with self.get_cursor() as cur:
            cur.execute(self.TABLES_QUERY)
            for schema, table in cur:
                # Concatenate in Python to avoid PostgreSQL adding quotes
                yield (f"{schema}.{table}",)

    def table_columns(self):
        """Yields (table_name, column_name) tuples for current database.

        Overrides base implementation to query all user schemas.
        Returns schema-qualified table/view names concatenated in Python to avoid
        PostgreSQL quoting issues.
        Returns columns for both tables and views to match tables() output.
        """
        # Query all user schemas (exclude system schemas)
        # Join with both pg_tables and pg_views to get columns for tables and views
        query = '''
            SELECT c.table_schema, c.table_name, c.column_name
            FROM information_schema.columns c
            WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal')
              AND (
                -- Match tables
                EXISTS (
                    SELECT 1 FROM pg_tables t
                    WHERE c.table_schema = t.schemaname
                      AND c.table_name = t.tablename
                )
                OR
                -- Match views
                EXISTS (
                    SELECT 1 FROM pg_views v
                    WHERE c.table_schema = v.schemaname
                      AND c.table_name = v.viewname
                )
              )
            ORDER BY c.table_schema, c.table_name, c.ordinal_position
        '''
        with self.get_cursor() as cur:
            cur.execute(query)
            for schema, table, column in cur:
                # Concatenate in Python to avoid PostgreSQL adding quotes
                yield (f"{schema}.{table}", column)

    def supports_special_command(self, command):
        """Check if Redshift backend supports a special command.

        Args:
            command: Special command name

        Returns:
            bool: False for all commands (Redshift doesn't support Athena-specific features)
        """
        # Redshift doesn't support Athena-specific features like output_location
        return False
