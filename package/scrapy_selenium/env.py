# - coding:utf-8 -#
# 该模块管理全局的环境变量

from typing import Any, Union
from copy import deepcopy
from fake_useragent import UserAgent
from .defaults import fake_useragent_content
from .globals import _global_ctx_stack, g

import tempfile
import json
import os

directory = tempfile.gettempdir()
json_file = os.path.join(directory, 'fake_useragent_0.1.11.json')

if not os.path.exists(json_file):
    fp = open(json_file, 'w+', encoding='utf8')
    json.dump(fake_useragent_content, fp, ensure_ascii=False)
    del fp

del json, tempfile, os, directory


__all__ = ('env',)


class Env(dict):
    _dl_caches_ = {}

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setitem__(name, value)

    def __getattr__(self, key) -> '_dict':
        return self[key]

    def __setitem__(self, k, v) -> None:
        raise TypeError('use register')

    def __getitem__(self, k) -> Union[Any, '_dict']:
        v = super().__getitem__(k)
        predict = isinstance(v, (list, dict))
        if not predict:
            return v
        cache = self._dl_caches_.get(k)
        if cache is None:
            if isinstance(v, dict):
                copyed = _dict(**deepcopy(v))
            elif isinstance(v, list):
                copyed = deepcopy(v)
            else:
                copyed = v
            self._dl_caches_[k] = deepcopy(v)
            return copyed
        return cache

    def register(self, key: str, value: Any):
        register_key = key.replace('-', '_')
        super().__setitem__(register_key, value)

    def flush(self):
        self._dl_caches_.clear()

    def propertys(self) -> list:
        return list(self.keys())

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.has_init = False


class _dict(dict):

    def getint(self, k, default=0):
        return int(self.get(k, default))

    def getfloat(self, k, default=0.0):
        return float(self.get(k, default))

    def getboolean(self, k, default=False):
        return bool(self.get(k, default))

    def getlist(self, k, separator: str):
        return self.get(k).split(separator)


env = Env()
env.register('ua', UserAgent())
env.register('flask_urls', {'tiktok': 'http://127.0.0.1:5000/tiktok'})
_global_ctx_stack.push(env)

