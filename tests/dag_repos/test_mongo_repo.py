# __author__ = 'godq'
import unittest
import time

from dagflow.dag_repos.mongodb_dag_repo import MongodbDagRepo


class MongodbRepoTest(unittest.TestCase):
    def test_dag_def(self):
        dag = {
            "name": "dag_def_test",
            "steps": [
                {
                    "name": "step1",
                    "task_func": "hello_plugin1",
                    "args": {
                        "name": "hello world"
                    },
                    "upstreams": [],
                    "downstreams": ["step2"]
                },
                {
                    "name": "step2",
                    "task_func": "hello_plugin2",
                    "args": {
                        "name": "hello world"
                    }
                },
            ]
        }
        repo = MongodbDagRepo()
        dag_name = dag['name']

        repo.delete_dag(dag_name)
        repo.add_dag(dag_name, dag)
        dag_found = repo.find_dag(dag_name)
        print(dag_found)
        step = repo.find_step_def(dag_name, "step2")
        print(step)

    def test_dag_run(self):
        dag_name = "test"
        self.test_dag_def()
        repo = MongodbDagRepo()
        run_id1 = repo.add_dag_run(dag_name, time.time())
        print(run_id1)
        run_id2 = repo.add_dag_run(dag_name, time.time())
        print(run_id2)
        r = repo.list_dag_runs(dag_name)
        print(r)

    def test_dag_run_event(self):
        dag_name = "test"
        self.test_dag_def()
        repo = MongodbDagRepo()
        run_id = repo.add_dag_run(dag_name, time.time())
        print(run_id)
        event_status1 = {
            "time": time.time(),
            "type": "Created"
        }
        event_status2 = {
            "time": time.time(),
            "type": "Success"
        }
        repo.add_dag_run_event(dag_name, run_id, event_status1)
        repo.add_dag_run_event(dag_name, run_id, event_status2)
        r = repo.find_dag_run_events(dag_name, run_id)
        print(r)

