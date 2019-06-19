__author__ = 'godq'
from dagflow.utils.redis_operator import get_redis_client
import logging
import time

logger = logging.getLogger('dagflow')
redis_client = get_redis_client()

DLOCK_PREFIX = "dagflow_dlock_"


class DLock:
    def __init__(self, name, u_id=None, block=False, block_check_interval=2):
        self.name = "{}{}".format(DLOCK_PREFIX, name)
        if not u_id:
            u_id = str(time.time())
        self.u_id = str(u_id)
        self.block = block
        self.block_check_interval = block_check_interval

    def lock(self, expire_time=30, block=False):
        if not block:
            block = self.block

        if not block:
            ret = redis_client.set(self.name, self.u_id, ex=expire_time, nx=True)
            if ret:
                return True
            else:
                return False
        else:
            start_time = time.time()
            while True:
                ret = redis_client.set(self.name, self.u_id, ex=expire_time, nx=True)
                if ret:
                    return True
                else:
                    if time.time() - start_time > expire_time:
                        return False
                    time.sleep(self.block_check_interval)

    def refresh(self, expire_time=30):
        v = redis_client.get(self.name)
        if v == self.u_id:
            redis_client.expire(self.name, expire_time)
            return True
        else:
            return False

    def unlock(self):
        v = redis_client.get(self.name)
        if isinstance(v, bytes):
            v = v.decode()
        if v == self.u_id:
            redis_client.delete(self.name)
            return True
        else:
            return False

