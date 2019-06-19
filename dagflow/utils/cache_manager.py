__author__ = 'godq'
from dagflow.utils.redis_operator import get_redis_client
import logging
import time
import json

logger = logging.getLogger('dagflow')
redis_client = get_redis_client()

CACHE_PREFIX = "dagflow_cache_"
EXPIRE_TIME = 60 * 60 * 2


class CacheManager:

    @staticmethod
    def set_cache(cache_id, data, expire_time=EXPIRE_TIME):
        key = CACHE_PREFIX + str(cache_id)
        if isinstance(data, dict):
            data = json.dumps(data)
        redis_client.set(name=key, value=data, ex=expire_time)

    @staticmethod
    def get_cache(cache_id, decode=False):
        key = CACHE_PREFIX + str(cache_id)
        d = redis_client.get(name=key)
        if decode is True and isinstance(d, bytes):
            d = d.decode()
            d = json.loads(d)
        return d

    @staticmethod
    def list_cache():
        key_pattern = CACHE_PREFIX + "*"
        keys_iter = redis_client.scan_iter(match=key_pattern, count=10)
        return list(keys_iter)

    @staticmethod
    def delete_cache(cache_id):
        key = CACHE_PREFIX + str(cache_id)
        redis_client.delete(key)


if __name__ == '__main__':
    cid = "key"
    value = {
        "name": "key",
        "num": 111
    }
    CacheManager.set_cache(cid, value)
    print(CacheManager.get_cache(cid, decode=True))
    print(CacheManager.list_cache())
    CacheManager.delete_cache(cid)
    print(CacheManager.get_cache(cid))
