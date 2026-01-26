# encoding: utf-8
"""Redshift-specific configuration handling."""

import os


class RedshiftConfig:
    """Configuration for Redshift connection."""

    def __init__(self, host, port, database, user, password, sslmode, aws_profile, region, config):
        """Initialize Redshift configuration.

        Args:
            host: Redshift cluster endpoint (from CLI or config)
            port: Port number (from CLI or config)
            database: Database name (from CLI or config)
            user: Username (from CLI or config)
            password: Password (from CLI or config)
            sslmode: SSL mode (from CLI or config)
            aws_profile: AWS profile for IAM authentication (from CLI or config)
            region: AWS region (from CLI or config)
            config: ConfigObj instance from config file
        """
        # Priority: CLI args > config file > environment variables > defaults

        # Get profile from config or use 'default'
        profile_section = 'redshift_profile default'
        if profile_section not in config:
            profile_section = 'main'

        cfg = config.get(profile_section, config.get('main', {}))

        # Host
        self.host = (
            host or
            cfg.get('host') or
            os.environ.get('REDSHIFT_HOST') or
            os.environ.get('PGHOST')
        )

        # Port
        self.port = (
            port or
            cfg.get('port') or
            os.environ.get('REDSHIFT_PORT') or
            os.environ.get('PGPORT') or
            5439
        )
        if isinstance(self.port, str):
            self.port = int(self.port)

        # Database
        self.database = (
            database or
            cfg.get('database') or
            os.environ.get('REDSHIFT_DATABASE') or
            os.environ.get('PGDATABASE') or
            'dev'
        )

        # User
        self.user = (
            user or
            cfg.get('user') or
            os.environ.get('REDSHIFT_USER') or
            os.environ.get('PGUSER') or
            os.environ.get('USER')
        )

        # Password
        self.password = (
            password or
            cfg.get('password') or
            os.environ.get('REDSHIFT_PASSWORD') or
            os.environ.get('PGPASSWORD')
        )

        # SSL mode
        self.sslmode = (
            sslmode or
            cfg.get('sslmode') or
            os.environ.get('PGSSLMODE') or
            'prefer'
        )

        # AWS Profile (for IAM authentication)
        self.aws_profile = (
            aws_profile or
            cfg.get('aws_profile') or
            os.environ.get('AWS_PROFILE') or
            'default'
        )

        # AWS Region
        self.region = (
            region or
            cfg.get('region') or
            os.environ.get('AWS_DEFAULT_REGION') or
            'us-east-1'
        )

    def __repr__(self):
        return (
            f'RedshiftConfig(host={self.host}, port={self.port}, '
            f'database={self.database}, user={self.user}, '
            f'password={"***" if self.password else None}, sslmode={self.sslmode}, '
            f'aws_profile={self.aws_profile}, region={self.region})'
        )
