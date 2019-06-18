# __author__ = 'godq'


class Config:
    BASE_URL = "127.0.0.1"
    CELERY_APP_NAME = "dagflow"
    CELERY_CONFIGS = {
        "CELERY_BROKER_URL": "redis://@{}:6379/0".format(BASE_URL),
        "CELERY_RESULT_BACKEND": "redis://@{}:6379/0".format(BASE_URL),
        "CELERY_TASK_SERIALIZER": 'json',
        "CELERY_RESULT_SERIALIZER": 'json',
        "CELERY_ACCEPT_CONTENT": ['json'],
        "CELERY_TIMEZONE": 'Asia/Shanghai',
    }
    DAG_REPO = "dagflow.dag_repos.redis_dag_repo.RedisDagRepo"
    EXECUTOR = "dagflow.executors.celery_executor.CeleryExecutor"

