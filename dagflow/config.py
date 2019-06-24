__author__ = 'godq'


import logging.config
from dagflow.configs.logging_config import LoggingConfig
logging.config.dictConfig(LoggingConfig)


class Config:
    dag_repo_class = "dagflow.dag_repos.mongodb_dag_repo.MongodbDagRepo"
    # executor_class = "dagflow.executors.celery_executor.CeleryExecutor"
    executor_class = "dagflow.executors.sequential_executor.SequentialExecutor"

    base_url = "10.241.0.146"
    celery_app_name = "dagflow"
    celery_configs = {
        "broker_url": "redis://@{}:6379/0".format(base_url),
        "result_backend": "redis://@{}:6379/0".format(base_url),
        "task_serializer": 'json',
        "result_serializer": 'json',
        "accept_content": ['json'],
        "timezone": 'Asia/Shanghai',
    }

    # Redis for step dependency calculation and session storage
    redis_url = "redis://@{}:6379/0".format(base_url)

    # MongoDB for persistent storage of dag/run/event
    repo_mongodb_url = {
        "url": "mongodb://{}:27017/".format(base_url),
        "db_name": "dagflow"}

    # Dagflow Broker Config
    event_mq_url = "amqp://qau:qau-@@@-12345@mq1.dev.bkjk.cn:5672/qau"
    event_mq_exchange = 'dagflow-broker-local'
    event_mq_queue = 'dagflow-broker-local'
    event_mq_delay_queue = 'dagflow-broker-delay-local'


