import unittest
from airflow.models import DagBag

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


        


suite = unittest.TestLoader().loadTestsFromTestCase(TestImdbPull)
unittest.TextTestRunner(verbosity=2).run(suite)