__author__ = 'godq'


class Config:
    dag_repo_class = "dagflow.dag_repos.mongodb_dag_repo.MongodbDagRepo"
    # Executor = "dagflow.executors.celery_executor.CeleryExecutor"
    executor_class = "dagflow.executors.celery_executor.SequentialExecutor"

    base_url = "10.241.3.229"
    celery_app_name = "dagflow"
    celery_configs = {
        "broker_url": "redis://@{}:6379/0".format(base_url),
        "result_backend": "redis://@{}:6379/0".format(base_url),
        "task_serializer": 'json',
        "result_serializer": 'json',
        "accept_content": ['json'],
        "timezone": 'Asia/Shanghai',
    }

    redis_url = "redis://@{}:6379/0".format(base_url)
    repo_mongodb_url = {
        "url": "mongodb://{}:27017/".format(base_url),
        "db_name": "dagflow"}


