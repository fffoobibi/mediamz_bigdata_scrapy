import os
import time
import scrapy
import logging
import yagmail
import requests
import pymysql

from copy import deepcopy
from datetime import datetime
from typing import List, Tuple, Union

from scrapy.spiders import CrawlSpider
from scrapy_redis.spiders import RedisCrawlSpider, RedisSpider

from .globals import g
from .utils import time_now
from .request import BrowserRequest
from .decorators import lasyproperty
from .controllers import spider_refs
from .parseconfig import create_g_from_cfg

_Logger = logging.getLoggerClass()

create_g_from_cfg()

_dft_settings = {
    'DOWNLOADER_MIDDLEWARES': {
        'scrapy_selenium.middlewares.BrowserDownloadMiddle': 543  # 下载中间件
    },
    # 'ITEM_PIPELINES': {
    #     'scrapy_selenium.pipelines.SaveToSqlPipesline': 543  # 储存数据值mysql数据库
    # },
    'EXTENSIONS': {
        'scrapy_selenium.extensions.EMailExensions': 500  # 邮件通知
    },
}

if g.dbdefault.get('enable', False) == False:
    _dft_settings['ITEM_PIPELINES']['scrapy_selenium.pipelines.SaveToSqlPipesline'] = None
if g.mail_settings.get('enable', False) == False:
    _dft_settings['EXTENSIONS']['scrapy_selenium.extensions.EMailExensions'] = None


class SpiderRef(type):
    def __new__(mtcls, name, bases, attrs):
        cls = super().__new__(mtcls, name, bases, attrs)
        spider_refs.setdefault(cls.name, cls)
        return cls


class ChromeSpiderBase(object):

    # [name, crawl_mode, count, crawl_time, crawl_item, crasqlwl_process, urls]
    crawl_settings = []

    pid_name: int = None  # 运行时动态设置的参数

    def create_db(self, db_settings: dict) -> pymysql.Connect:
        settings = db_settings.copy()
        settings.pop('table', None)
        settings.pop('enable', None)
        data_base = pymysql.connect(**settings)
        db_name = str(data_base.db, encoding='utf8') if isinstance(
            data_base.db, bytes) else data_base.db
        self.dbs.setdefault(db_name, data_base)
        return data_base

    def get_cursor(self, data_base: str, cursor_type=pymysql.cursors.SSDictCursor):
        db = self.dbs.get(data_base)
        cursor = db.cursor(cursor_type)
        self.cursors.append(cursor)
        return cursor

    def get_table(self, cursor, table: str):
        try:
            cursor.execute(f'select * from {table}')
            return cursor.fetchall()
        except:
            return (None, )

    @classmethod
    def add_method(cls, func):
        setattr(cls, func.__name__, func)

    def looping_email(self):
        messages = f'{self.run_info}正在运行\n抓取时间:{self.crawl_time}\n当前时间:{datetime.now()}\n已抓取:{self.crawl_counts}条数据'
        self.send_by_mail_settings(messages)

    def __init_subclass__(cls, *args, **kwargs) -> None:
        super().__init_subclass__(*args, **kwargs)

        dft = deepcopy(_dft_settings)

        # if cls.custom_settings.get('ITEM_PIPELINES') is not None:
        #     dft['ITEM_PIPELINES'].update(cls.custom_settings['ITEM_PIPELINES'])
        # cls.custom_settings['ITEM_PIPELINES'] = dft['ITEM_PIPELINES']

        if cls.custom_settings.get('DOWNLOADER_MIDDLEWARES') is not None:
            dft['DOWNLOADER_MIDDLEWARES'].update(
                cls.custom_settings['DOWNLOADER_MIDDLEWARES'])

        cls.custom_settings['DOWNLOADER_MIDDLEWARES'] = dft['DOWNLOADER_MIDDLEWARES']

        if cls.custom_settings.get('EXTENSIONS') is not None:
            dft['EXTENSIONS'].update(cls.custom_settings['EXTENSIONS'])

        cls.custom_settings['EXTENSIONS'] = dft['EXTENSIONS']
        cls.custom_settings['ROBOTSTXT_OBEY'] = False

        cls.set_max_items(g.crawl_settings['crawl_item'])
        cls.set_running_time(g.crawl_settings['crawl_time'])

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.start_time = datetime.now()
        self.crawl_time = datetime.now()
        self.crawl_counts = 0
        self.middle = None
        self.dbs = {}
        self.cursors = []
        self.logger: _Logger
        self.logger.info('Spider opened: %s' % self.name)
        if g.info_settings.get('verbose', False):
            print(self.run_info + ' run at %s' % self.start_time)

    def closed(self, reason):
        for cursor in self.cursors:
            try:
                cursor.close()
            except:
                ...
        for db in self.dbs:
            try:
                self.dbs[db].close()
            except:
                ...

    def set_middle(self, middle):
        self.middle = middle

    @classmethod  # 获取验证码信息
    def flask_captcha_info(cls,
                           src: str = None,
                           b64_image: bytes = None,
                           type: str = 'tiktok',
                           show: bool = False) -> dict:
        url = g.flask_urls.get(type, '')
        if url:
            post_data = {'image': src, 'b64': b64_image, 'show': show}
            resp = requests.post(url, data=post_data)
            return resp.json()
        return {}

    @lasyproperty
    def computer_info(self):
        return g.info_settings.get('computer', 'unknow')

    @lasyproperty
    def run_info(self) -> str:  # 运行时信息
        return self.computer_info + '-' + self.name + '-' + f'{self.pid_name}'

    def message(self) -> None:
        print(f'[{self.run_info} {time_now()}] 已抓取: ', self.crawl_counts + 1)

    @lasyproperty
    def mail_client(self):
        copyed = deepcopy(g.mail_settings)
        copyed.pop('enable', None)
        copyed.pop('notice_frequency', None)
        self.sends = copyed.pop('sends', None)
        return yagmail.SMTP(**copyed)

    def send_by_mail_settings(self, msg: str):
        if g.mail_settings.get('enable', True):
            try:
                self.mail_client.send(self.sends, f'SPider<{self.run_info}>爬虫',
                                      msg)
                self.logger.info(f'{self.run_info} Send Email At {time_now()}')
            except Exception as e:
                print('Send mail Error: %s' % e)

    def browser_request(self,
                        url,
                        callback=None,
                        cont_flag=None,
                        method="GET",
                        headers=None,
                        body=None,
                        cookies=None,
                        meta=None,
                        encoding='utf-8',
                        priority=0,
                        dont_filter=False,
                        errback=None,
                        flags=None,
                        cb_kwargs=None,
                        **kw) -> scrapy.Request:
        return BrowserRequest(url, callback, cont_flag, method, headers, body, cookies, meta, encoding, priority,
                              dont_filter, errback, flags, cb_kwargs, **kw)

    def push_urls(self,
                  urls: List[str],
                  cont_flag,
                  callback=None,
                  dont_filter=False):
        for url in urls:
            yield self.browser_request(url,
                                       callback=callback,
                                       cont_flag=cont_flag,
                                       dont_filter=dont_filter)

    # 设置brower相关设置
    @classmethod
    def set_browser_settings(cls, pid_name):
        user_data_dir = g.chrome_settings.get('user_data_dir')
        disk_cache_dir = g.chrome_settings.get('disk_cache_dir')
        if user_data_dir and disk_cache_dir:
            user_data_dir = os.path.join(
                user_data_dir, f'{cls.name}-{cls.pid_name}')
            disk_cache_dir = os.path.join(
                disk_cache_dir, f'{cls.name}-{cls.pid_name}')
            g.chrome_settings['user_data_dir'] = user_data_dir
            g.chrome_settings['disk_cache_dir'] = disk_cache_dir

    # 设置进程信息
    @classmethod
    def set_pid_name(cls, name):
        cls.pid_name = name

    # 设置日志文件
    @classmethod
    def set_log_file(cls, pid) -> None:
        path = g.crawl_settings.get('log_dir', './logs')
        if not os.path.exists(path):
            os.mkdir(path)
        file = os.path.abspath(os.path.join(
            path, f'{cls.name}-{pid}-{time_now()}.log'))
        if os.path.exists(file):
            try:
                os.remove(file)
            except:
                ...
        cls.pid_name = pid
        cls.custom_settings['LOG_FILE'] = file

    # 设置爬虫运行时间
    @classmethod
    def set_running_time(cls, seconds: int) -> None:
        cls.custom_settings['CLOSESPIDER_TIMEOUT'] = seconds

    # 设置单次最大爬取数量
    @classmethod
    def set_max_items(cls, count: int):
        cls.custom_settings['CLOSESPIDER_ITEMCOUNT'] = count

    # 设置start_urls
    @classmethod
    def set_start_urls(cls, urls):
        cls.start_urls = list(urls)

    # 设置queue
    @classmethod
    def set_main_q(cls, queue):
        cls.queue = queue

    # 阻塞指定的时间
    def sleep(self, time_out=0.5):
        time.sleep(time_out)

    # # 字符串转换为数字类型
    # def str2value(
    #         self,
    #         value_str: str,
    #         *,
    #         pattern: str = None,
    #         strip: bool = True,
    #         replace: Tuple[str, str] = None,  # (old, new)
    #         dft=0  # 转换失败则返回默认值
    # ) -> Union[int, float, None]:

    #     if pattern:  # 正则处理
    #         re_result = re.findall(pattern, value_str)
    #         if re_result:
    #             value_str = re_result[0]

    #     value_str = str(value_str).strip() if strip else value_str  # 去除空白字符

    #     value_str = value_str.replace(
    #         *replace) if replace else value_str  # replace操作

    #     idxOfYi = value_str.find("亿")

    #     idxOfWan = value_str.find("万")

    #     try:
    #         if idxOfYi != -1 and idxOfWan != -1:
    #             return int(
    #                 float(value_str[:idxOfYi]) * 1e8 +
    #                 float(value_str[idxOfYi + 1:idxOfWan]) * 1e4)
    #         elif idxOfYi != -1 and idxOfWan == -1:
    #             return int(float(value_str[:idxOfYi]) * 1e8)
    #         elif idxOfYi == -1 and idxOfWan != -1:
    #             return int(float(value_str[idxOfYi + 1:idxOfWan]) * 1e4)
    #         elif idxOfYi == -1 and idxOfWan == -1:
    #             return float(value_str)
    #     except:
    #         return dft


class ChromeSpider(ChromeSpiderBase, scrapy.Spider, metaclass=SpiderRef):
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_selenium.middlewares.BrowserDownloadMiddle": 543
        }
    }

    def __init_subclass__(cls, *args, **kwargs) -> None:
        super().__init_subclass__(*args, **kwargs)


class ChromeCrawlSpider(ChromeSpiderBase, CrawlSpider, metaclass=SpiderRef):
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_selenium.middlewares.BrowserDownloadMiddle": 543
        }
    }

    def __init_subclass__(cls, *args, **kwargs) -> None:
        super().__init_subclass__(*args, **kwargs)


class RedisChromeSpider(ChromeSpiderBase, RedisSpider, metaclass=SpiderRef):

    custom_settings = {
        'LOG_LEVEL': g.crawl_settings.get('log_level', 'info').upper(),
        'REDIS_HOST': g.redis.get('host'),  # 指定Redis的主机名和端口
        'REDIS_PORT': g.redis.get('port'),
        'SCHEDULER': 'scrapy_redis.scheduler.Scheduler',  # 调度器启用Redis存储Requests队列
        'DUPEFILTER_CLASS': 'scrapy_redis.dupefilter.RFPDupeFilter',  # 确保所有的爬虫实例使用Redis进行重复过滤
        'SCHEDULER_PERSIST': True,  # 将Requests队列持久化到Redis，可支持暂停或重启爬虫
        'SCHEDULER_QUEUE_CLASS': 'scrapy_redis.queue.PriorityQueue',  # Requests的调度策略，默认优先级队列
    }

    def __init_subclass__(cls, *args, **kwargs) -> None:
        super().__init_subclass__(*args, **kwargs)
        


class RedisChromeCrawlSpider(ChromeSpiderBase,
                             RedisCrawlSpider,
                             metaclass=SpiderRef):
    custom_settings = {
        'LOG_LEVEL': g.crawl_settings.get('log_level', 'info').upper(),
        'REDIS_HOST': g.redis.get('host'),  # 指定Redis的主机名和端口
        'REDIS_PORT': g.redis.get('port'),
        'SCHEDULER':
            'scrapy_redis.scheduler.Scheduler',  # 调度器启用Redis存储Requests队列
        'DUPEFILTER_CLASS':
            'scrapy_redis.dupefilter.RFPDupeFilter',  # 确保所有的爬虫实例使用Redis进行重复过滤
        'SCHEDULER_PERSIST': True,  # 将Requests队列持久化到Redis，可支持暂停或重启爬虫
        'SCHEDULER_QUEUE_CLASS':
            'scrapy_redis.queue.PriorityQueue',  # Requests的调度策略，默认优先级队列,
    }

    def __init_subclass__(cls, *args, **kwargs) -> None:
        super().__init_subclass__(*args, **kwargs)
