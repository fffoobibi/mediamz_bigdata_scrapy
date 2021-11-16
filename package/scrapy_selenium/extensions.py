from datetime import datetime
from scrapy import signals

from .globals import g
from .selenium_spiders import ChromeSpiderBase


class EMailExensions(object):


    @classmethod
    def from_crawler(cls, crawler):
        ext= cls()
        # 将扩展对象连接到信号， 将signals.spider_idle 与 spider_idle() 方法关联起来。
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext
       
    def spider_opened(self, spider: ChromeSpiderBase):
        from twisted.internet.task import LoopingCall
        if isinstance(spider, ChromeSpiderBase) and g.mail_settings['enable'] and g.mail_settings['notice_frequency']:
            loop = LoopingCall(spider.looping_email)
            self.task1 = loop.start(g.mail_settings['notice_frequency'], False)
        # if isinstance(spider, ChromeSpiderBase) and g.fail_settings['enable'] and g.fail_settings['repush_frequency']:
        #     loop = LoopingCall(spider.flush_fail_urls)
        #     self.task2 = loop.start(g.fail_settings['repush_frequency'], False)

    def spider_closed(self, spider:ChromeSpiderBase):
        spider.send_by_mail_settings(f'{spider.run_info}爬虫已关闭\n抓取时间:{spider.crawl_time}\n当前时间:{datetime.now()}\n已抓取:{spider.crawl_counts}条数据')


