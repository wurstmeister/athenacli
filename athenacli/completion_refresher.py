import threading
from collections import OrderedDict

from athenacli.completer import AthenaCompleter
from athenacli.sqlexecute import SQLExecute
from athenacli.packages.special.main import COMMANDS

import logging
LOGGER = logging.getLogger(__name__)


class CompletionRefresher(object):

    refreshers = OrderedDict()

    def __init__(self):
        self._completer_thread = None
        self._restart_refresh = threading.Event()

    def refresh(self, executor, callbacks, completer_options=None):
        """Creates a SQLCompleter object and populates it with the relevant
        completion suggestions in a background thread.

        executor - SQLExecute object, used to extract the credentials to connect
                   to the database.
        callbacks - A function or a list of functions to call after the thread
                    has completed the refresh. The newly created completion
                    object will be passed in as an argument to each callback.
        completer_options - dict of options to pass to SQLCompleter.
        """
        if completer_options is None:
            completer_options = {}

        if self.is_refreshing():
            self._restart_refresh.set()
            return [(None, None, None, 'Auto-completion refresh restarted.')]
        else:
            self._completer_thread = threading.Thread(
                target=self._bg_refresh,
                args=(executor, callbacks, completer_options),
                name='completion_refresh')
            self._completer_thread.setDaemon(True)
            self._completer_thread.start()
            return [(None, None, None,
                     'Auto-completion refresh started in the background.')]

    def is_refreshing(self):
        return self._completer_thread and self._completer_thread.is_alive()

    def _bg_refresh(self, sqlexecute, callbacks, completer_options):
        completer = AthenaCompleter(**completer_options)

        # Reuse the existing sqlexecute for background refresh
        # (Backend abstraction makes it simpler - no need to recreate)
        executor = sqlexecute

        # If callbacks is a single function then push it into a list.
        if callable(callbacks):
            callbacks = [callbacks]

        while 1:
            for refresher in self.refreshers.values():
                refresher(completer, executor)
                if self._restart_refresh.is_set():
                    self._restart_refresh.clear()
                    break
            else:
                # Break out of while loop if the for loop finishes natually
                # without hitting the break statement.
                break

            # Start over the refresh from the beginning if the for loop hit the
            # break statement.
            continue

        for callback in callbacks:
            callback(completer)


def refresher(name, refreshers=CompletionRefresher.refreshers):
    """Decorator to add the decorated function to the dictionary of
    refreshers. Any function decorated with a @refresher will be executed as
    part of the completion refresh routine."""
    def wrapper(wrapped):
        refreshers[name] = wrapped
        return wrapped
    return wrapper


@refresher('databases')
def refresh_databases(completer, executor):
    completer.extend_database_names(executor.databases())


@refresher('schemata')
def refresh_schemata(completer, executor):
    # schemata will be the name of the current database.
    completer.extend_schemata(executor.database)
    completer.set_dbname(executor.database)


@refresher('tables')
def refresh_tables(completer, executor):
    # For Redshift/PostgreSQL backends, don't escape identifiers
    # Schema-qualified names like "schema.table" would be incorrectly quoted as
    # a single identifier, when they need to be unquoted or quoted separately
    from athenacli.backends import RedshiftBackend

    if isinstance(executor.backend, RedshiftBackend):
        # Don't escape Redshift identifiers - they're case-insensitive
        # and schema.table should not be quoted as a single identifier
        tables_data = list(executor.tables())

        metadata = completer.dbmetadata['tables']
        for relname_tuple in tables_data:
            relname = relname_tuple[0]
            try:
                metadata[completer.dbname][relname] = ['*']
            except KeyError:
                pass
            completer.all_completions.add(relname)

        # For columns, also skip escaping
        try:
            column_data = list(executor.table_columns())
        except Exception:
            column_data = []

        for relname, column in column_data:
            metadata[completer.dbname][relname].append(column)
            completer.all_completions.add(column)
    else:
        # Default behavior for Athena and other backends
        completer.extend_relations(executor.tables(), kind='tables')
        completer.extend_columns(executor.table_columns(), kind='tables')


@refresher('special_commands')
def refresh_special(completer, executor):
    completer.extend_special_commands(COMMANDS.keys())
