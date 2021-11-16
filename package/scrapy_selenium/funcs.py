from typing import Callable

class _Obj:

    def __setattr__(self, name, value) -> None:
        self.__dict__[name] = value

    def __getattr__(self, name):
        return self.__dict__[name]
        
def dict_to_obj(item: dict) -> _Obj:
    obj = _Obj()
    for key in item:
        obj.__setattr__(key, item[key])
    return obj


def lzip(*iterables, convert: Callable=None):

    def _all_done(lis):
        flag = 0
        l = len(lis)
        for f, v in lis:
            if isinstance(f, StopIteration):
                flag += 1
        return flag == l

    iters = [iter(i) for i in iterables]

    while True:
        iter_v = []
        for i in iters:
            try:
                iter_v.append((None, next(i)))
            except StopIteration as e:
                iter_v.append((e, None))

        if _all_done(iter_v):
            break
        else:
            ret = []
            for f ,v in iter_v:
                if convert is None:
                    ret.append(v)
                else:
                    ret.append(convert(v))
            yield ret
