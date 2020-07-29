import unittest
import sqlite3
from airflow.models import DagBag
from airflow.hooks.sqlite_hook import SqliteHook

from .. import dags
from dags.imbd_pull import db_to_file,csv_to_json
import tempfile
import csv
from os import path

class TestImdbPull(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag('imdb_data_pull')

    def test_expected_endpoints(self):
        tasks = self.dag.tasks

        source_tasks = [x for x in tasks if len(x.upstream_list) == 0]
        self.assertEqual(len(source_tasks), 1)

        sink_tasks = [x for x in tasks if len(x.downstream_list) == 0]
        self.assertEqual(len(sink_tasks), 2)

    def test_db_to_file(self):
        sqlite_conn = sqlite3.connect(':memory:')

        sqlite_conn.execute('create table person(name text, age integer)')
        data = [('Alice', 5),('Bob', 24), ('Carol', 33), ('Dave', 15)]
        sqlite_conn.executemany('insert into person values (?, ?)', data)

        class DBToFileHook(SqliteHook):
            def get_conn(self):
                return sqlite_conn

        hook=DBToFileHook()
        testfile=tempfile.NamedTemporaryFile()

        db_to_file(hook, "select name from person where age > 18", testfile.name)

        lines=testfile.readlines()

        self.assertEqual(len(lines), 3)

    def test_csv_to_json(self):
        testfile=tempfile.NamedTemporaryFile(mode='wt', newline='', suffix='.csv')

        writer = csv.writer(testfile)
        writer.writerows(
            [
                ['name', 'year', 'director_first_name', 'director_last_name'],
                ['Independence Day', 1996, 'Roland', 'Emmerich'],
                ['Dodgeball', 2004, 'Rawson', 'Thurber'],
                ['The Princess Bride', 1987, 'Rob', 'Reiner']
            ]
        )

        testfile.flush()

        csv_to_json(testfile.name)

        json_file_name=testfile.name.replace('.csv', '.json')
        self.assertTrue(path.exists(json_file_name))
        self.assertGreater(path.getsize(json_file_name), 0)
        


suite = unittest.TestLoader().loadTestsFromTestCase(TestImdbPull)
unittest.TextTestRunner(verbosity=2).run(suite)