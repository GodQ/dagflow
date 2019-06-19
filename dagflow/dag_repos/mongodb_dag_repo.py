__author__ = 'godq'
import logging
import json
import time

from dagflow.dag_repos.base_dag_repo import BaseDagRepo
from dagflow.utils.mongodb_operator import get_mongodb_client
from dagflow.exceptions import DagNotFoundInRepo

logger = logging.getLogger('dagflow')
mongodb_client = get_mongodb_client()

'''
Every dag definition is a doc in collection dag_def: index is the dag name, value is a dict with a list of steps
fields:
{
    "name": "dag_name",
    "steps": [
        {
            "name": "step_name1"
            
        },
        {
            "name": "step_name2"
        },
    ]
}

Once a step starts to execute, it will be added to dag_run_event, index is random, 
fields:
     dag_name: ...
     run_id: 111
     step_name: ...
     status: step.StepStatus
     time: current_time
     message: ...
     result: return of this step
'''


class MongodbDagRepo(BaseDagRepo):
    def add_dag(self, dag_name, content):
        assert isinstance(content, dict)
        with mongodb_client as my_mongodb_client:
            db = my_mongodb_client.db
            content['_id'] = dag_name
            db.dag_def.insert_one(content)

    def update_dag(self, dag_name, content):
        assert isinstance(content, dict)
        with mongodb_client as my_mongodb_client:
            db = my_mongodb_client.db
            content['_id'] = dag_name
            filter_dict = {"_id": dag_name}
            db.dag_def.find_one_and_replace(
                filter=filter_dict,
                replacement=content,
                upsert=True
            )

    def delete_dag(self, dag_name, just_flag=False):
        if just_flag is False:
            with mongodb_client as my_mongodb_client:
                db = my_mongodb_client.db
                db.dag_def.delete_one({"_id": dag_name})
        else:
            with mongodb_client as my_mongodb_client:
                db = my_mongodb_client.db
                dag = db.dag_def.find_one({"name": dag_name})

                dag['deleted'] = True
                dag['deleted_time'] = time.time()
                filter_dict = {"_id": dag_name}
                db.dag_def.find_one_and_replace(
                    filter=filter_dict,
                    replacement=dag,
                    upsert=True
                )

    def find_dag(self, dag_name):
        with mongodb_client as my_mongodb_client:
            db = my_mongodb_client.db
            res = db.dag_def.find({"name": dag_name})
            for dag in res:
                if isinstance(dag, bytes):
                    dag = dag.decode()
                if isinstance(dag, str):
                    dag = json.loads(dag)
                return dag
            raise DagNotFoundInRepo("Dag {} not found in mongodb".format(dag_name))

    def find_step_def(self, dag_name, step_name):
        dag = self.find_dag(dag_name)
        for step in dag['steps']:
            if step['name'] == step_name:
                return step

    def add_dag_run(self, dag_name, start_time):
        dag_run = dict()
        if not start_time:
            start_time = time.time()
        run_id = start_time
        with mongodb_client as my_mongodb_client:
            db = my_mongodb_client.db
            dag_run['dag_name'] = dag_name
            dag_run['start_time'] = start_time
            dag_run['run_id'] = run_id
            db.dag_run.insert_one(dag_run)
            return run_id

    def find_dag_runs(self, dag_name, max_count=5):
        dag_runs = list()
        with mongodb_client as my_mongodb_client:
            db = my_mongodb_client.db
            res = db.dag_run.find({"dag_name": dag_name})
            for dag in res:
                dag_runs.append(dag)
                if len(dag_runs) == max_count:
                    return dag_runs
            return dag_runs

    def add_dag_run_event(self, dag_name, run_id, status):
        assert isinstance(status, dict)
        with mongodb_client as my_mongodb_client:
            db = my_mongodb_client.db
            status['dag_name'] = dag_name
            status['run_id'] = run_id
            if 'time' not in status or not status['time']:
                status['time'] = time.time()
            db.dag_run_event.insert_one(status)

    def find_dag_run_events(self, dag_name, run_id):
        dag_run_events = list()
        with mongodb_client as my_mongodb_client:
            db = my_mongodb_client.db
            res = db.dag_run_event.find({"dag_name": dag_name, "run_id": run_id})
            for dag in res:
                dag_run_events.append(dag)
            return dag_run_events

