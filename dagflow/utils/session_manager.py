__author__ = 'godq'

import logging
import time
import json

from dagflow.utils.redis_operator import get_redis_client
from dagflow.utils.dlock import DLock

logger = logging.getLogger('dagflow')
redis_client = get_redis_client()

EXPIRE_TIME = 60 * 60 * 2


class SessionManager:
    def __init__(self, dag_name, dag_run_id):
        name = "session_{}_{}".format(dag_name, dag_run_id)
        self.dlock = DLock(name=name)
        self.session_id = name
        self.key = "dagflow_" + name

    def set_session(self, data, expire_time=EXPIRE_TIME):
        # self.dlock.lock(block=True)
        if isinstance(data, dict):
            data = json.dumps(data)
        redis_client.set(name=self.key, value=data, ex=expire_time)
        # self.dlock.unlock()

    def get_session(self, decode=False):
        d = redis_client.get(name=self.key)
        if decode is True and isinstance(d, bytes):
            d = d.decode()
            d = json.loads(d)
        return d

    def delete_session(self):
        redis_client.delete(self.key)


if __name__ == '__main__':
    pass
