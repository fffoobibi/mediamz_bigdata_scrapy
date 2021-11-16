import weakref
import platform
import subprocess

from enum import IntEnum
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from typing import List
from copy import deepcopy
from multiprocessing import Process
from datetime import datetime, timedelta

from .globals import g
from .sche import GLOBAL_SCHED

spider_refs = weakref.WeakValueDictionary()

class ProcessTask:  # 进程任务
    def __init__(self, process_settings) -> None:
        self.process_settings = process_settings

    def run(self):  # 启动进程任务
        process_count = self.process_settings[0][-2]  # crawl_process
        for i in range(process_count):
            process = Process(target=SpidersControler._schedule_ex,
                              args=(self.process_settings, 'p%s' % i))
            process.start()

    @property
    def job_kws(self) -> dict:  # 返回调度参数
        name, crawl_mode, *_ = self.process_settings[0]
        return SpidersControler.from_int(crawl_mode).job_kws()


schedul_params = {}  # 缓存自定义的调度任务信息


# 枚举类,控制爬虫的调度行为,爬虫启动类
class SpidersControler(IntEnum):
    test_mode = -1  # 测试模式
    undefined = 0  # 未定义模式
    every_8 = 1  # 每天早8点抓取
    every_3_day = 2  # 每三天抓取
    every_7_day = 3  # 每7天抓取
    every_15_day = 4  # 每15日抓取
    every_30_day = 5  # 每30天抓取

    @classmethod
    def from_int(cls, value) -> 'SpidersControler':
        lis = cls.__members__.values()
        for e in lis:
            if e == value:
                return e
        return cls.undefined

    # 注册新增加的任务调度
    @classmethod
    def register_schedule(cls, mode: int, trigger: str, **kw):
        schedul_params[mode] = [trigger, kw]

    # 返回调度参数
    def job_kws(self) -> dict:
        mode = self
        if mode == 1:
            return {'trigger': 'cron', 'hour': 8, 'minute': 8}
        elif mode == 2:
            return {'trigger': 'interval', 'days': 3}
        elif mode == 3:
            return {'trigger': 'interval', 'days': 7}
        elif mode == 4:
            return {'trigger': 'interval', 'days': 15}
        elif mode == 5:
            return {'trigger': 'interval', 'days': 30}
        else:
            paramters = schedul_params.get(mode.value, None)
            if paramters:
                trigger, kw = paramters
                return {'trigger': trigger, **kw}
        return

    @staticmethod
    def schedule_ex():
        # [name, crawl_mode, count, crawl_time, crawl_item, crawl_process, urls]
        for process_settings in g.crawl_settings['enable_spiders']:
            task = ProcessTask(process_settings)
            if task.job_kws is not None:
                GLOBAL_SCHED.add_job(task.run, **task.job_kws)
            else:
                GLOBAL_SCHED.add_job(task.job_kws,
                                     trigger='date',
                                     run_date=str(datetime.now() +
                                                  timedelta(seconds=30))[:-7])
        try:
            GLOBAL_SCHED.start()
        except (KeyboardInterrupt, SystemExit) as e:
            print('Schedule Exit: %s' % e)
            GLOBAL_SCHED.shutdown(False)

    @staticmethod
    def _schedule_ex(spider_settings: List[List], p_name: str):  # 执行单个进程内任务
        # [name, crawl_mode, count, crawl_time, crawl_item, crawl_process, start_urls, chrome_settings]
        process = CrawlerProcess(settings=get_project_settings())
        spider_clss = []
        for spider_setting in spider_settings:
            name, crawl_mode, crawl_count, crawl_time, crawl_item, crawl_process, start_urls = spider_setting
            for i in range(crawl_count):
                temp = {}
                temp['pid'] = p_name
                temp['run_time'] = crawl_time
                temp['max_items'] = crawl_item
                spider_cls = spider_refs.get(name)
                if start_urls:
                    spider_cls.set_start_urls(start_urls)
                spider_clss.append((spider_cls, temp))

        for spider, info in spider_clss:
            spider.set_log_file(info['pid'])  # 设置爬虫log文件
            spider.set_running_time(info['run_time'])  # 设置爬虫运行时间
            spider.set_max_items(info['max_items'])
            spider.set_browser_settings(info['pid'])
            process.crawl(spider,
                          custom_settings=deepcopy(spider.custom_settings))
        process.start()

    @staticmethod
    def simple():  # 无调度版本运行

        if platform.system() == 'Linux':
            p = subprocess.Popen('killall -9 -r chrome', shell=True)
            p.wait()

        for process_settings in g.crawl_settings['enable_spiders']:
            task = ProcessTask(process_settings)
            task.run()
