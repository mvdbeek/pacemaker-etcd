import etcd


def with_etcd_lock(func):
    def _decorator(self, *args, **kwargs):
        try:
            lock = etcd.Lock(self.client, lock_name=func.__name__)
            lock.acquire(lock_ttl=120, blocking=False)
            if lock.is_acquired:
                return func(self, *args, **kwargs)
            else:
                return False
        finally:
            lock.release()
    return _decorator
