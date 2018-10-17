from unittest import TestCase
from unittest.mock import Mock

import pyblazing


class TestAPI(TestCase):

  def test_for_objects(self):
    pygdf = Mock(**{
      'readParquet.return_value': None,
    })

    # start

    gdfA = pygdf.readParquet('/path/A.parquet')
    gdfB = pygdf.readParquet('/path/B.parquet')

    connection = pyblazing.Connection()

    db = connection.Database('pyblazing_test_db')

    tableA = db.Table('tableA', gdfA)
    tableB = db.Table('tableB', gdfB)

    q = '''
      select tableA.colA, tableB.colB
        from tableA inner join tableB on tableA.key = tableB.key;
    '''
    token = db.runQuery(q, [tableA, tableB])

    result = db.getResult(token)

  def test_for_chaining(self):
    pygdf = Mock(**{
      'readParquet.return_value': None,
    })

    result = (pyblazing
      .using_db('pyblazing_test_db')
      .with_tables(tableA=pygdf.readParquet('/path/A.parquet'),
                   tableB=pygdf.readParquet('/path/B.parquet'))
      .run_query('''
        select tableA.colA, tableB.colB
          from tableA inner join tableB on tableA.key = tableB.key;
      '''))


if '__main__' == __name__:
  unittest.main()
