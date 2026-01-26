# -*- coding: utf-8 -*-
"""Main entry point for redshiftcli."""

import os
import sys
import select
import click

from athenacli.main import AthenaCli as BaseAthenaCli
from athenacli.sqlexecute import SQLExecute
from athenacli.backends import RedshiftBackend
from athenacli.config import read_config_files, write_default_config
from redshiftcli.config import RedshiftConfig

PACKAGE_ROOT = os.path.abspath(os.path.dirname(__file__))
REDSHIFTCLIRC = '~/.redshiftcli/redshiftclirc'
DEFAULT_CONFIG_FILE = os.path.join(PACKAGE_ROOT, 'redshiftclirc')


class RedshiftCli(BaseAthenaCli):
    """Redshift CLI - extends AthenaCli with Redshift-specific configuration."""

    DEFAULT_PROMPT = '\\d@\\h> '

    def __init__(self, host, port, database, user, password, sslmode,
                 aws_profile, region, redshiftclirc):
        """Initialize RedshiftCli.

        Args:
            host: Redshift cluster endpoint
            port: Port number
            database: Database name
            user: Username
            password: Password
            sslmode: SSL mode
            aws_profile: AWS profile for IAM authentication
            region: AWS region
            redshiftclirc: Path to config file
        """
        config_files = [DEFAULT_CONFIG_FILE]
        if os.path.exists(os.path.expanduser(redshiftclirc)):
            config_files.append(redshiftclirc)
        _cfg = self.config = read_config_files(config_files)

        self.init_logging(_cfg['main']['log_file'], _cfg['main']['log_level'])

        redshift_config = RedshiftConfig(
            host, port, database, user, password, sslmode, aws_profile, region, _cfg
        )

        try:
            self.connect(redshift_config, database)
        except Exception as e:
            self.echo(str(e), err=True, fg='red')
            err_msg = '''
There was an error while connecting to Redshift. It could be caused due to
missing/incomplete configuration. Please verify the configuration in %s
and run redshiftcli again.

For more details about the error, you can check the log file: %s''' % (
                redshiftclirc, _cfg['main']['log_file']
            )
            self.echo(err_msg)
            sys.exit(1)

        # Initialize other attributes from config
        from athenacli.packages import special
        special.set_timing_enabled(_cfg['main'].as_bool('timing'))
        self.multi_line = _cfg['main'].as_bool('multi_line')
        self.key_bindings = _cfg['main']['key_bindings']
        self.prompt = _cfg['main'].get('prompt', self.DEFAULT_PROMPT)
        self.destructive_warning = _cfg['main']['destructive_warning']
        self.syntax_style = _cfg['main']['syntax_style']
        self.prompt_continuation_format = _cfg['main']['prompt_continuation']

        from cli_helpers.tabular_output import TabularOutputFormatter
        from athenacli.packages.tabular_output import sql_format
        from athenacli.clistyle import style_factory_output
        from athenacli.completer import AthenaCompleter
        from athenacli.completion_refresher import CompletionRefresher
        import threading

        self.formatter = TabularOutputFormatter(_cfg['main']['table_format'])
        self.formatter.cli = self
        sql_format.register_new_formatter(self.formatter)

        self.cli_style = _cfg['colors']
        self.output_style = style_factory_output(self.syntax_style, self.cli_style)

        # Reuse AthenaCompleter as it's generic SQL
        self.completer = AthenaCompleter()
        self._completer_lock = threading.Lock()
        self.completion_refresher = CompletionRefresher()

        self.prompt_app = None
        self.query_history = []

        # Register custom special commands
        self.register_special_commands()

    def connect(self, redshift_config, database=None):
        """Connect to Redshift using configuration.

        Args:
            redshift_config: RedshiftConfig instance
            database: Optional database name (overrides config)
        """
        backend = RedshiftBackend(
            host=redshift_config.host,
            port=redshift_config.port,
            database=database or redshift_config.database,
            user=redshift_config.user,
            password=redshift_config.password,
            sslmode=redshift_config.sslmode,
            aws_profile=redshift_config.aws_profile,
            region=redshift_config.region
        )
        self.sqlexecute = SQLExecute(backend)

    def get_prompt(self, string):
        """Override to support Redshift-specific prompt placeholders.

        Adds support for:
            \\h - hostname
            \\p - port
            \\u - user
            \\t - time (24-hour format)
        """
        # First call parent to handle base placeholders (\d, \r, date/time)
        string = super().get_prompt(string)

        # Add Redshift-specific placeholders
        backend = self.sqlexecute.backend
        string = string.replace('\\h', backend.host or '(none)')
        string = string.replace('\\p', str(backend.port) if backend.port else '(none)')
        string = string.replace('\\u', backend.user or '(none)')

        from datetime import datetime
        now = datetime.now()
        string = string.replace('\\t', now.strftime('%H:%M:%S'))

        return string


@click.command()
@click.option('-e', '--execute', type=str,
              help='Execute a command (or a file) and quit.')
@click.option('-h', '--host', type=str,
              help='Redshift cluster endpoint.')
@click.option('-p', '--port', type=int,
              help='Port number (default: 5439).')
@click.option('-U', '--user', type=str,
              help='Database user.')
@click.option('-W', '--password', type=str,
              help='Database password (omit for IAM authentication).')
@click.option('--sslmode', type=str,
              help='SSL mode (prefer, require, disable).')
@click.option('--aws-profile', type=str,
              help='AWS profile for IAM authentication (defaults to AWS_PROFILE env var or "default").')
@click.option('--region', type=str,
              help='AWS region (defaults to us-east-1).')
@click.option('--redshiftclirc', default=REDSHIFTCLIRC,
              type=click.Path(dir_okay=False),
              help='Location of redshiftclirc file.')
@click.option('--table-format', type=str, default='csv',
              help='Table format used with -e option.')
@click.argument('database', default='dev', nargs=1)
def cli(execute, host, port, user, password, sslmode, aws_profile, region,
        redshiftclirc, table_format, database):
    """A Redshift terminal client with auto-completion and syntax highlighting.

    \b
    Examples:
      - redshiftcli -h mycluster.region.redshift.amazonaws.com -U myuser
      - redshiftcli -h mycluster.region.redshift.amazonaws.com -U myuser mydb
      - redshiftcli -h mycluster.region.redshift.amazonaws.com -U thombeck --aws-profile raptor_engineer integration_analytics
    """
    if (redshiftclirc == REDSHIFTCLIRC and
            not os.path.exists(os.path.expanduser(redshiftclirc))):
        err_msg = '''
        Welcome to redshiftcli!

        It seems this is your first time to run redshiftcli,
        we generated a default config file for you
            %s
        Please configure your Redshift connection details and run redshiftcli again.
        ''' % redshiftclirc
        print(err_msg)
        write_default_config(DEFAULT_CONFIG_FILE, redshiftclirc)
        sys.exit(1)

    redshiftcli = RedshiftCli(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        sslmode=sslmode,
        aws_profile=aws_profile,
        region=region,
        redshiftclirc=redshiftclirc
    )

    # Handle --execute argument
    if execute:
        if execute == '-':
            if select.select([sys.stdin, ], [], [], 0.0)[0]:
                query = sys.stdin.read()
            else:
                raise RuntimeError("No query to execute on stdin")
        elif os.path.exists(execute):
            with open(execute) as f:
                query = f.read()
        else:
            query = execute
        try:
            redshiftcli.formatter.format_name = table_format
            redshiftcli.run_query(query)
            exit(0)
        except Exception as e:
            click.secho(str(e), err=True, fg='red')
            exit(1)

    redshiftcli.run_cli()


if __name__ == '__main__':
    cli()
