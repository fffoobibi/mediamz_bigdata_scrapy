import time
import weakref

from typing import Optional
from redis import StrictRedis
from scrapy_selenium.globals import g


class IpPoolBase(object):

    refs = weakref.WeakValueDictionary()
    duration = 300

    @classmethod
    def getClass(cls, cls_name):
        if cls_name == cls.__name__:
            return cls
        else:
            return cls.refs.get(cls_name)

    def random(self, url=None):
        return self._get(url)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        IpPoolBase.refs[cls.__name__] = cls

    def __init__(self):
        self._current_value = None

    def _get(self, url=None, **kw):
        if self._current_value is None:
            value, t = self._request(url, **kw), time.time()
            self._current_value = (value, t)
            return value
        else:
            v, t = self._current_value
            if time.time() - t <= self.duration:
                return v
            v, t = self._request(url, **kw), time.time()
            self._current_value = (v, t)
            return v

    def _request(self, url, **kw) -> str:
        return None


class RedisIpPool(IpPoolBase):
    
    """use redis"""
    save_key = 'IP_POOL_VALUE'    

    def __init__(self):
        self.redis = StrictRedis(host=g.redis.get(
            'host'), port=g.redis['port'], db=g.redis['db'], password=g.redis.get('pwd'))

    def __del__(self):
        try:
            self.redis.close()
        except:
            ...

    def _get(self, url, **kw) -> Optional[str]:
        ip_value = self.redis.get(self.save_key)
        if ip_value is not None:
            if isinstance(ip_value, bytes):
                return ip_value.decode()
        else:
            save_value = self._request(url, **kw)
            if save_value is not None:
                self.redis.setex(self.save_key, self.duration, save_value)
            return save_value
