__author__ = 'godq'
from dagflow.utils.redis_operator import get_redis_client
import logging
import time
import json

logger = logging.getLogger('dagflow')
redis_client = get_redis_client()

SESSION_PREFIX = "dagflow_session_"
EXPIRE_TIME = 60 * 60 * 2


class SessionManager:

    @staticmethod
    def set_session(session_id, data, expire_time=EXPIRE_TIME):
        key = SESSION_PREFIX + str(session_id)
        if isinstance(data, dict):
            data = json.dumps(data)
        redis_client.set(name=key, value=data, ex=expire_time)

    @staticmethod
    def get_session(session_id, decode=False):
        key = SESSION_PREFIX + str(session_id)
        d = redis_client.get(name=key)
        if decode is True and isinstance(d, bytes):
            d = d.decode()
            d = json.loads(d)
        return d

    @staticmethod
    def list_sessions():
        key_pattern = SESSION_PREFIX + "*"
        keys_iter = redis_client.scan_iter(match=key_pattern, count=10)
        return list(keys_iter)

    @staticmethod
    def delete_session(session_id):
        key = SESSION_PREFIX + str(session_id)
        redis_client.delete(key)


if __name__ == '__main__':
    l = SessionManager.list_sessions()
    print(l)
    SessionManager.set_session("111", {"1": 1})
    d = SessionManager.get_session("111")
    print(d)
    l = SessionManager.list_sessions()
    print(l)
