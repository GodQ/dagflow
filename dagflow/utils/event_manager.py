__author__ = 'godq'
from dagflow.utils.redis_operator import get_redis_client
import logging
import time
import json

logger = logging.getLogger('dagflow')
redis_client = get_redis_client()

EVENT_PREFIX = "dagflow_event_"
EXPIRE_TIME = 60 * 60 * 2


class EventManager:

    @staticmethod
    def add_event(event_id, data, expire_time=EXPIRE_TIME):
        key = EVENT_PREFIX + str(event_id)
        if isinstance(data, dict):
            data = json.dumps(data)
        redis_client.set(name=key, value=data, ex=expire_time)

    @staticmethod
    def get_event(event_id):
        key = EVENT_PREFIX + str(event_id)
        d = redis_client.get(name=key)
        if not d:
            return None
        if isinstance(d, bytes):
            d = d.decode()
        d = json.loads(d)
        return d

    @staticmethod
    def list_events(max_count=10):
        key_pattern = EVENT_PREFIX + "*"
        keys_iter = redis_client.scan_iter(match=key_pattern, count=max_count)
        return list(keys_iter)

    @staticmethod
    def delete_event(event_id):
        redis_client.delete(event_id)


if __name__ == '__main__':
    l = EventManager.list_events()
    print(l)
    o_d = {"1": 1}
    EventManager.add_event("111", o_d)
    d = EventManager.get_event("111")
    print(d)
    l = EventManager.list_events()
    print(l)
