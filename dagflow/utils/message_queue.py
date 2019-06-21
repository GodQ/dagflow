__author__ = 'godq'
import json
from dagflow.utils.redis_operator import get_redis_client


class MessageQueue:
    def __init__(self, queue):
        self.redis_client = get_redis_client()
        self.queue = queue

    def put_message(self, message):
        self.redis_client.lpush(self.queue, message)

    def get_message(self, decode=False, timeout=5):
        msg = self.redis_client.brpop(self.queue, timeout=timeout)
        if decode is True:
            if isinstance(msg, bytes):
                msg = msg.decode()
            msg = json.loads(msg)
        return msg