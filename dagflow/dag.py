__author__ = 'godq'

import json

from dagflow.loader import get_DagRepo_Object
from dagflow.utils.cache_manager import CacheManager
from dagflow.step import StepStatus
from dagflow.exceptions import StepFailed
from dagflow.utils.dlock import DLock

dag_repo_obj = get_DagRepo_Object()


'''
Graph def:
    step1 -> step2, step3 -> step4
    
    ==> 
    
    step_downstreams:
    {
        step1: [step2, step3]
        step2: [step4]
        step3: [step4]
        step4: []
    }
    step_upstreams:
    {
        step1: []
        step2: [step1]
        step3: [step1]
        step4: [step2, step3]
    }

event fields:
             dag_name: ...
             run_id: 111
             step_name: ...
             operation: start_flow or finish_step(default) or continue_flow
             time: current_time
             status: step.StepStatus
             message: ...
             result: return of this step
'''


class Dag:
    def __init__(self, dag_name, run_id=None, event=None):
        self.dag_name = dag_name
        self.dag_run_id = str(run_id)
        self.event = event
        self.dag = None
        self.step_upstreams = None  # dict, step dependency
        self.step_downstreams = None  # dict
        self.steps = dict()
        self.first_flag = False
        self.ready_steps = None
        self.dag_run_events = None
        self.dlock = DLock("dag_loading")

    @property
    def dag_graph_cache_id(self):
        cache_id = "dag_graph_to_do_{}_{}".format(
            self.dag_name,
            self.dag_run_id
        )
        return cache_id

    def set_current_event(self, event):
        self.event = event

    def fetch_steps_to_run(self, max_count=1):
        self.dlock.lock(expire_time=60 * 2, block=True)
        self.load_current_step_graph()
        self.update_step_graph(self.event)
        ready_steps = self.select_ready_steps()
        ret_steps = ready_steps[0:max_count]
        self.mark_ready_steps(ret_steps)
        self.save_step_graph()
        self.dlock.unlock()
        return ret_steps

    def fetch_one_step_to_run(self):
        ret_steps = self.fetch_steps_to_run(max_count=1)
        if len(ret_steps) == 1:
            return ret_steps[0]
        else:
            return None

    def fetch_step_info(self, step_name):
        if not self.dag:
            self.load_dag()
        dag_steps = self.dag['steps']
        for step in dag_steps:
            if step_name == step['name']:
                return step
        return None

    def load_dag(self):
        cache_id = "dag_def_{}".format(self.dag_name)
        dag = CacheManager.get_cache(cache_id, decode=True)
        if not dag:
            dag = dag_repo_obj.find_dag(self.dag_name)
            CacheManager.set_cache(cache_id, dag)
        self.dag = dag
        return dag

    def load_current_step_graph(self):
        dag_step_deps = CacheManager.get_cache(self.dag_graph_cache_id, decode=True)
        if dag_step_deps:
            self.step_upstreams = dag_step_deps["step_upstreams"]
            self.step_downstreams = dag_step_deps["step_downstreams"]
            self.steps = dag_step_deps["steps"]
            self.first_flag = False
        else:
            if not self.dag:
                self.load_dag()
            self.init_step_graph()
            self.first_flag = True

    def init_step_graph(self, dag_def=None):
        if not dag_def:
            dag_def = self.dag
        dag_steps = dag_def['steps']
        # init all up/down
        CacheManager.delete_cache(self.dag_graph_cache_id)
        self.step_upstreams = dict()
        self.step_downstreams = dict()
        for step in dag_steps:
            step_name = step['name']
            self.step_upstreams[step_name] = list()
            self.step_downstreams[step_name] = list()

        # calculate every up/down
        for step in dag_steps:
            step_name = step['name']
            upstreams = step.get('upstreams', None)
            downstreams = step.get('downstreams', None)
            step['scheduled'] = False
            self.steps[step_name] = step
            if upstreams:
                for s in upstreams:
                    self.step_upstreams[step_name].append(s)
                    self.step_downstreams[s].append(step_name)
            if downstreams:
                for s in downstreams:
                    self.step_downstreams[step_name].append(s)
                    self.step_upstreams[s].append(step_name)

    def load_dag_run_events(self):
        self.dag_run_events = dag_repo_obj.find_dag_run_events(self.dag_name, self.dag_run_id)
        return self.dag_run_events

    def update_step_graph(self, dag_run_events=None, first_flag=False):
        '''
        if step finished,
        remove this step in its downstream_steps' upstreams,
        delete this step in self.steps
        '''

        # make sure dag_run_events is a list
        if dag_run_events and not isinstance(dag_run_events, list):
            dag_run_events = [dag_run_events]
        if not dag_run_events:
            dag_run_events = list()

        # if this is the first time, analyze all events
        if first_flag is True:
            es = self.load_dag_run_events()
            dag_run_events.extend(es)

        for event in dag_run_events:
            step_name = event['step_name']
            status = event['status']
            self.steps[step_name]['status'] = status
            if status == StepStatus.Succeeded:
                for s in self.step_downstreams[step_name]:
                    self.step_upstreams[s].remove(step_name)
                del self.steps[step_name]
            elif status == StepStatus.Failed:
                msg = json.dumps(event)
                raise StepFailed(msg)

    def select_ready_steps(self):
        ready_steps = list()
        for s in self.steps.keys():
            scheduled = self.steps[s].get('scheduled', False)
            if len(self.step_upstreams[s]) == 0 and scheduled is False:
                ready_steps.append(s)
        return ready_steps

    def mark_ready_steps(self, selected_steps):
        for s in selected_steps:
            self.steps[s]['scheduled'] = True

    def save_step_graph(self):
        cache_id = self.dag_graph_cache_id
        dag_step_deps = {
            "steps": self.steps,
            "step_upstreams": self.step_upstreams,
            "step_downstreams": self.step_downstreams
        }
        CacheManager.set_cache(cache_id, dag_step_deps)

    def is_finished(self):
        self.load_current_step_graph()
        return len(self.steps) == 0


if __name__ == "__main__":
    from dagflow.dag_repos.mongodb_dag_repo import MongodbDagRepo
    dag_def = {
        "name": "dag_def_test",
        "steps": [
            {
                "name": "step1",
                "upstreams": [],
                "downstreams": ["step2", "step3"]
            },
            {
                "name": "step2",
                "upstreams": [],
                "downstreams": ["step4"]
            },
            {
                "name": "step3",
                "upstreams": [],
                "downstreams": ["step4"]
            },
            {
                "name": "step4",
                "upstreams": [],
                "downstreams": []
            },
            {
                "name": "step5",
                "upstreams": [],
                "downstreams": ["step6"]
            },
            {
                "name": "step6",
                "upstreams": [],
                "downstreams": []
            },
        ]
    }
    repo = MongodbDagRepo()
    dag_name = dag_def['name']

    repo.delete_dag(dag_name)
    repo.add_dag(dag_name, dag_def)
    run_id = repo.add_dag_run(dag_name)
    print("create dag run id: " + run_id)
    dag = Dag(dag_name, run_id)

    # step = dag.fetch_one_step_to_run()
    # print("Select step {} to run".format(step))
    # step = dag.fetch_one_step_to_run()
    # print("Select step {} to run".format(step))
    # step = dag.fetch_one_step_to_run()
    # print("Select step {} to run".format(step))

    steps = dag.fetch_steps_to_run(max_count=10)
    print("Select steps {} to run".format(steps))
