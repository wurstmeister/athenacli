# encoding: utf-8
"""Amazon Athena database backend."""

import logging
import pyathena

from athenacli.backends.base import DatabaseBackend

logger = logging.getLogger(__name__)


class AthenaBackend(DatabaseBackend):
    """Amazon Athena backend implementation."""

    def __init__(
        self,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        region_name=None,
        s3_staging_dir=None,
        work_group=None,
        role_arn=None,
        database=None,
        result_reuse_enable=False,
        result_reuse_minutes=60,
        catalog_name=None
    ):
        """Initialize Athena backend.

        Args:
            aws_access_key_id: AWS access key ID
            aws_secret_access_key: AWS secret access key
            region_name: AWS region name
            s3_staging_dir: S3 location for query results
            work_group: Athena work group
            role_arn: IAM role ARN to assume
            database: Initial database/schema
            result_reuse_enable: Enable query result reuse
            result_reuse_minutes: Minutes to reuse query results
            catalog_name: Athena catalog name (default: AwsDataCatalog)
        """
        # Handle database parameter that may contain catalog.database format
        if database and '.' in database:
            catalog_name, database = database.split('.', 1)

        super().__init__(database=database)

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.s3_staging_dir = s3_staging_dir
        self.work_group = work_group
        self.role_arn = role_arn
        self.catalog_name = catalog_name or 'AwsDataCatalog'
        self.result_reuse_enable = result_reuse_enable
        self.result_reuse_minutes = result_reuse_minutes

        self.connect()

    def connect(self, database=None):
        """Establish connection to Athena.

        Args:
            database: Optional database to connect to. Can be in format
                     'catalog.database' to specify both catalog and database.
        """
        # Handle database parameter that may contain catalog.database format
        catalog_name = self.catalog_name
        if database and '.' in database:
            catalog_name, database = database.split('.', 1)

        conn_params = {
            'aws_access_key_id': self.aws_access_key_id,
            'aws_secret_access_key': self.aws_secret_access_key,
            'region_name': self.region_name,
            's3_staging_dir': self.s3_staging_dir,
            'work_group': self.work_group,
            'schema_name': database or self.database,
            'role_arn': self.role_arn,
            'poll_interval': 0.2,  # 200ms
            'catalog_name': catalog_name
        }

        # Add result reuse parameters if enabled
        if self.result_reuse_enable:
            conn_params['result_reuse_enable'] = True
            conn_params['result_reuse_minutes'] = self.result_reuse_minutes

        conn = pyathena.connect(**conn_params)
        self.database = database or self.database

        if self.conn:
            self.conn.close()
        self.conn = conn

    def close(self):
        """Close Athena connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def format_statistics(self, cursor):
        """Format Athena execution statistics.

        Args:
            cursor: PyAthena cursor after query execution

        Returns:
            str: Formatted statistics including execution time, data scanned, and cost
        """
        if not cursor:
            return ''

        # Most regions are $5 per TB: https://aws.amazon.com/athena/pricing/
        approx_cost = cursor.data_scanned_in_bytes / (1024 ** 4) * 5

        return '\nExecution time: %d ms, Data scanned: %s, Approximate cost: $%.2f' % (
            cursor.engine_execution_time_in_millis,
            self._humanize_size(cursor.data_scanned_in_bytes),
            approx_cost
        )

    def _humanize_size(self, num_bytes):
        """Convert bytes to human-readable format.

        Args:
            num_bytes: Number of bytes

        Returns:
            str: Human-readable size (e.g., '1.5 GB')
        """
        suffixes = ['B', 'KB', 'MB', 'GB', 'TB']

        suffix_index = 0
        while num_bytes >= 1024 and suffix_index < len(suffixes) - 1:
            num_bytes /= 1024.0
            suffix_index += 1

        num = ('%.2f' % num_bytes).rstrip('0').rstrip('.')
        return '%s %s' % (num, suffixes[suffix_index])

    def supports_special_command(self, command):
        """Check if Athena backend supports a special command.

        Args:
            command: Special command name

        Returns:
            bool: True if supported
        """
        # Athena supports output_location tracking
        return command == 'output_location'
