__author__ = 'godq'
from dagflow.utils.class_loader import ClassLoader
from dagflow.config import Config
from dagflow.utils.cache_manager import CacheManager

dag_repo_class = ClassLoader.load(Config.dag_repo_class)
dag_repo_obj = dag_repo_class()
# step_executor = ClassLoader.load(Config.executor_class)


class Dag:
    def __init__(self, dag_name, run_id):
        self.dag_name = dag_name
        self.dag_run_id = run_id
        self.step_status_map = dict()
        self.dag = None
        self.step_upstreams = dict()  # dict, step dependency
        self.step_downstreams = dict()  # dict
        self.steps = dict()
        self.dag_run_events = None

        self.load_dag()
        self.load_step_dependency()
        self.load_dag_run_events()

    def load_dag(self):
        cache_id = "dag_def_{}".format(self.dag_name)
        dag = CacheManager.get_cache(cache_id)
        if not dag:
            dag = dag_repo_obj.find_dag(self.dag_name)
            CacheManager.set_cache(cache_id, dag)
        self.dag = dag

        return dag

    def load_step_dependency(self):
        cache_id = "dag_step_deps_{}".format(self.dag_name)
        dag_step_deps = CacheManager.get_cache(cache_id, decode=True)
        if dag_step_deps:
            self.step_upstreams = dag_step_deps["step_upstreams"]
            self.step_downstreams = dag_step_deps["step_downstreams"]
            self.steps = dag_step_deps["steps"]
        else:
            self.build_step_dependency()
            dag_step_deps = {
                "steps": self.steps,
                "step_upstreams": self.step_upstreams,
                "step_downstreams": self.step_downstreams
            }
            CacheManager.set_cache(cache_id, dag_step_deps)

    def build_step_dependency(self):
        dag_steps = self.dag['steps']
        # init all up/down
        for step in dag_steps:
            step_name = step['name']
            self.step_upstreams[step_name] = list()
            self.step_downstreams[step_name] = list()

        # calculate every up/down
        for step in dag_steps:
            step_name = step['name']
            upstreams = step.get('upstreams', None)
            downstreams = step.get('downstreams', None)
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

    def update_step_graph(self):
    '''
    event fields:
         dag_name: ...
         run_id: 111
         step_name: ...
         status: step.StepStatus
         time: current_time
         message: ...
         result: return of this step
    '''
        for event in self.dag_run_events:
            step_name = event['step_name']
            status = event['status']
        #TODO


    def next_step(self, current_step):
        return None

