"""PyBlazing API."""

import collections.abc

# import blazingdb.protocol


__all__ = ('Connection', 'IO')


class Artifact:
  """To represent orchestrator and interpreter artifacts.

  Do IPC calls between clients and artifacts.
  """

  def __init__(self, path):
    self._path = path

  def send(self, _bytes):
    """
    Raises: ConnectionError, TimeoutError
    """
    if not isinstance(_bytes, collections.abc.Iterable):
      raise(ValueError('Argument must be a iterable'))

    socket = blazingdb.protocol.UnixSocketConnection(self._path)
    with blazingdb.protocol.Client(socket) as client:
      client.send(_bytes)


class OrchestratorRepository:

  def openSession(self, credential):
    """
    Returns session object.
    Raises: CredentialError.
    """
    return NotImplemented

  def closeSession(self, session):
    return NotImplemented

  def createDatabase(self, name):
    return NotImplemented

  def createTable(self, name):
    return NotImplemented


class InterpreterRepository:

  def getResult(self, token):
    return NotImplemented


class Table:
  """Table object."""

  def __init__(self, name, cleaner):
    self._clean = cleaner
    self._name = name

  def __del__(self):
    self.drop()

  def drop(self):
    self._clean()


class Database:

  def __init__(self, name, orchestrator, interpreter, cleaner):
    self._clean = cleaner
    self._name = name
    self._tables = []

    self._orchestrator = orchestrator
    self._interpreter = interpreter

  def __del__(self):
    """Drop tables."""
    self.drop()

  def Table(self, name, dataframe):
    """
    Argsuments:
      dataframe: gdf dataframe

    Returns table object.
    """
    table = Table(name,
      lambda: self._tables.remove(table) if table in self._tables else None)
    self._tables.append(table)
    return table

  def removeTable(self, table):
    """
    Args:
      table: object.
    """
    return NotImplemented

  def runQuery(self, querystring, tables):
    """
    Returns token object.
    """
    return NotImplemented

  def getResult(self, token):
    return NotImplemented

  def drop(self):
    for table in self._tables:
      table.drop()
    self._clean()


class Connection:

  def __init__(self):
    """
    TODO(gcca): add configuration argument
    """
    self._interpreter = Artifact('/path/dummy/interpreter.socket')
    self._orchestrator = Artifact('/path/dummy/orchestrator.socket')

    self._databases = []
    self._session = None

  def __del__(self):
    """Drop database."""
    for db in self._databases:
      db.drop()

  def Database(self, name):
    """
    Returns database object.
    """
    db = Database(name, self._orchestrator, self._interpreter,
      lambda: self._databases.remove(db) if db in self._databases else None)
    self._databases.append(db)
    return db


def using_db(name):
  return DatabaseBuilder(name)


class DatabaseBuilder:

  def __init__(self, name):
    self._db = None  # create Database

  def with_tables(self, **tables):
    for name, gdf in tables.items():
      pass
    return RunQueryBuilder()


class RunQueryBuilder:

  def run_query(self, s):
    return NotImplemented
