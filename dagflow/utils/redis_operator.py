# -*- coding: utf-8 -*-
__author__ = 'godq'
import redis


class RedisOperator:

    def __init__(self, url):

        self._url = url
        self._pool = redis.ConnectionPool.from_url(self._url)
        self._client = redis.StrictRedis(connection_pool=self._pool)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    @property
    def client(self):
        return self._client

    def list_iter(self, key):
        list_count = self._client.llen(key)
        for index in range(list_count):
            yield self._client.lindex(key, index)


redis_client = None


def get_redis_client():
    global redis_client
    if redis_client:
        return redis_client

    from dagflow.config import Config
    redis_client = RedisOperator(Config.redis_url).client
    return redis_client


if __name__ == "__main__":
    pass
