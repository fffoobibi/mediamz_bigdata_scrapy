# - coding=utf-8 -#
# 该模块定义了 装饰器

from functools import wraps
from typing import Any, Callable, Tuple, List, Dict, Union

__all__ = ('lasyproperty', 'final_property', 'ncalls', 'trigger', 'emit',
           'Trigger', 'action', 'method_caches')


# 延迟属性装饰器
class lasyproperty(object):
    def __init__(self, func):
        self.func = func
        self.__doc__ = func.__doc__

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        value = self.func(instance)
        instance.__dict__[self.func.__name__] = value
        return value


# final 常量装饰器
class final_property(object):
    def __init__(self, initial) -> None:
        self.dft = initial
        self.func = None
        self.setted = False

    def __get__(self, ins, owner=None):
        if ins is None:
            return self
        if self.setted:
            return ins.__dict__[self.func.__name__]
        return self.dft

    def __set__(self, ins, value):
        if self.setted:
            return
        ins.__dict__[self.func.__name__] = value
        self.setted = True

    def __call__(self, func):
        self.func = func
        return self


# 记录函数调用次数的装饰器
def ncalls(func):
    ncalls = 0

    @wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal ncalls
        ncalls += 1
        return func(*args, **kwargs)

    def clear():
        nonlocal ncalls
        ncalls = 0

    wrapper.ncalls = lambda: ncalls
    wrapper.clear = lambda: clear()
    return wrapper


# 参数广播运算
def match_params_to_list(*params) -> List[List]:
    results = []
    for param in params:
        if isinstance(param, str):
            p = [param]
        else:
            p = param if isinstance(param, (tuple, list)) else [param]
        results.append(p)

    max_length = max([len(l) for l in results])
    return_result = []
    for param in results:
        return_result.append(_broadcast_seqence(param, max_length))
    return return_result

# 广播
def _broadcast_seqence(lis: List, count: int) -> List:
    result = []
    if len(lis) < count:
        s2 = [lis[-1]] * (count - len(lis))
        result.extend(lis)
        result.extend(s2)
        return result
    result.extend(lis)
    return result

class Trigger(Exception):
    def __init__(self, *events: Tuple[str], args: Union[Tuple, List[Tuple]] = None,
                 kwargs: Union[Dict, List[Dict]] = None,
                 msg: Union[str, List[str]] = ''):
        self.events, self.call_a, self.call_kws, self.msg = match_params_to_list(events, args, kwargs,
                                                                                  msg)


    def __contains__(self, item: str):
        return item in self.events

    def __str__(self):
        return f'{self.msg}'


def emit(*events, args: Union[Tuple, List[Tuple]] = None, kwargs: Union[Dict, List[Dict]] = None,
         msg: Union[str, List[str]] = ""):
    # 广播运算
    raise Trigger(*events, args=args, kwargs=kwargs, msg=msg)


# 函数触发装饰器
def trigger(frequencys: Union[int, List[int]],
            calls: Union[Callable, List[Callable], List[Tuple[Callable, Tuple]], List[Tuple[Callable, Tuple, Dict]]],
            events: Union[str, List[str]] = '',
            raise_as: Union[Exception, List[Exception]] = None,
            is_series: Union[bool, List[bool]] = True,
            states: Dict[str, Tuple[int, Any, Any]] = None,
            ):
    '''
    触发器
    frequencys: 触发器执行频率
    calls: 触发器回调函数
    triggers: 触发事件名称
    is_series: 触发器类型,连续式触发,总计触发
    raise_as: 触发器是否抛出触发异常
    states: 触发器状态值
    '''

    ncalls = 0  # 函数调用次数
    trigger_logs = {}  # 触发器记录
    length_record = [0, ]  # 总计触发缓存表
    max_length = 0  # 广播运算

    # 计算连续触发数
    def series_count(lis) -> int:
        i = 0
        for log in reversed(lis):
            if log == True:
                i += 1
            else:
                return i
        return i

    fs, trigger_names, raises, series, length_record = match_params_to_list(frequencys, events, raise_as, is_series,
                                                                             length_record)
    if callable(calls):
        call_funcs = [(calls, (), {})]
    elif isinstance(calls, (tuple, list)):
        res = []
        for u in calls:
            if callable(u):
                res.append((u, (), {}))
            else:
                if len(u) == 2:
                    temp = list(u)
                    temp.append({})
                res.append(tuple(u))
        call_funcs = res
    else:
        call_funcs = [lambda: None]
        
    call_funcs = _broadcast_seqence(call_funcs, len(fs))

    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):

            nonlocal ncalls, trigger_logs, length_record, max_length, call_funcs, fs, raises, series, trigger_names
            ncalls += 1

            if ncalls == 100000:  # 触发器每10万次状态重置
                ncalls = 0
                for key_trigger in trigger_logs:
                    trigger_logs[key_trigger].clear()
                length_record = [0 for i in range(max_length)]

            try:
                return func(*args, **kwargs)
            except Trigger as e:
                for index, event in enumerate(e.events):
                    try:
                        trigger_logs.setdefault(event, [])
                        trigger_logs[event].append(True)
                        # 连续触发
                        if series[index]:
                            if series_count(trigger_logs[event]) >= fs[index]:
                                arg = call_funcs[index][1]
                                kws = call_funcs[index][2]
                                a = e.call_a[index]
                                kw = e.call_kws[index]
                                execute_arg = a if a is not None else arg
                                execute_kw = kw if kw is not None else kws
                                call_funcs[index][0](e.msg[index], *execute_arg, **execute_kw)
                                trigger_logs[event] = [True]
                        # 总计触发
                        else:
                            if length_record[index] == 0:
                                total_count = len(list(filter(lambda ele: ele is True, trigger_logs[event])))
                                length_record[index] = total_count
                            else:
                                length_record[index] += 1
                                total_count = length_record[index]

                            if total_count >= fs[index]:
                                arg = call_funcs[index][1]
                                kws = call_funcs[index][2]
                                a = e.call_a[index]
                                kw = e.call_kws[index]
                                execute_arg = a if a is not None else arg
                                execute_kw = kw if kw is not None else kws
                                call_funcs[index][0](e.msg[index], *execute_arg, **execute_kw)
                                trigger_logs[event] = []
                                length_record[index] = 0

                        if raises[index] is not None:
                            raise raises[index](e.msg[index])

                    except Exception:
                        trigger_logs[event].append(False)
                        raise


        # 设置状态值
        if states:
            for key in states.keys():
                setattr(inner, key, lambda: states[key][1] if ncalls <= states[key][0] else states[key][2])
        return inner

    return wrapper


@trigger(frequencys=[3, 2], calls=print,
         events=['test', 'test2'], is_series=True, states={'valid': (3, True, False)}, raise_as=None)
def trigger_demo(i, toggle=True):
    print('valid: ', trigger_demo.valid())
    if toggle:
        print('i=%s' % i)
        emit('test', 'test2', args=[('hellow',), ('xxx',)], msg='附加信息')
        # raise Trigger('test', 'test2', msg="测试1")
        # raise Trigger('test', 'test2', msg="测试2")


def test_tri():
    for i in range(15):
        flag = True if i % 2 == 0 else False
        flag = True
        try:
            res = trigger_demo(i, flag)
            # print('i=%s' % i, 'trigger test test2 ' if flag else '')
        except RuntimeError:
            pass


class UniqueKeyDict(dict):
    def __setitem__(self, k, v) -> None:
        if k in self.keys():
            raise TypeError('键已存在, 请重新设置')
        return super().__setitem__(k, v)

    def __deepcopy__(self, memo) -> 'UniqueKeyDict':
        return UniqueKeyDict(**self)


method_caches = UniqueKeyDict()


def action(name: Any):
    '''
    缓存装饰的方法至 method_caches字典中,key为 f'{class.__qualname__}.{name}'的形式

    for example:

    >>> class C:
    ...     class D:
                @aciton(0)
    ...         def meth(self):
    ...             pass
    ...
    >>> C.__qualname__
    'C'
    >>> C.D.__qualname__
    'C.D'
    >>> C.D.meth.__qualname__
    'C.D.meth'
    >>> list(method_caches.keys())
    ['C.D.0']
    '''

    def wrapper(func):
        sp = func.__qualname__.split('.')
        ls = len(sp)
        if ls > 1:
            tt = ''.join(sp[:-1])
        else:
            tt = ''

        method_caches[f'{tt}.{name}'] = func

        @wraps(func)
        def inner(*args, **kwargs):
            return func(*args, **kwargs)

        return inner

    return wrapper


if __name__ == '__main__':
    test_tri()
