__author__ = 'godq'

import os
import sys

root_dir = os.path.dirname(__file__)
sys.path.append(os.path.dirname(root_dir))
sys.path.append(root_dir)
sys.path.append(os.path.join(root_dir, "executors"))
sys.path.append(os.path.join(root_dir, "dag_repos"))
sys.path.append(os.path.join(root_dir, "plugins"))

from dagflow.config import Config
from dagflow.utils.class_loader import ClassLoader
from dagflow.message_queue.mq_broker import MQ_Broker


def get_DagRepo_Class():
    DagRepoClass = ClassLoader.load(Config.dag_repo_class)
    return DagRepoClass


dag_repo = None


def get_DagRepo_Object():
    global dag_repo
    if dag_repo:
        return dag_repo
    DagRepoClass = ClassLoader.load(Config.dag_repo_class)
    dag_repo = DagRepoClass()
    return dag_repo


def get_StepExecutor_Class():
    StepExecutor = ClassLoader.load(Config.executor_class)
    return StepExecutor


mq_broker = None


def get_MQ_Broker_Object():
    global mq_broker
    if mq_broker:
        return mq_broker
    mq_broker = MQ_Broker(
        url=Config.event_mq_url,
        exchange=Config.event_mq_exchange,
        queue=Config.event_mq_queue,
        delay_queue=Config.event_mq_delay_queue
    )
    return mq_broker

