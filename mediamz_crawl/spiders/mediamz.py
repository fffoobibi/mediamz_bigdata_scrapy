import logging

from itertools import zip_longest
from functools import partial
from typing import List, Iterator, Tuple


from pymysql.cursors import Cursor

from scrapy_selenium.pipelines import SaveToSqlPipesline
from scrapy_selenium import (g, pipe_lines, BrowserRequest, ChromeSpider)

from mediamz_crawl.spiders import should_crawl, Tbl, UrlType, Crawl
from mediamz_crawl.spiders.ins_account import get_ins_other_account


class MediamzCrawl:

    @staticmethod
    def crawl_promoter_detail(type, item, dm, **kw):
        return Crawl.crawl_promoter_detail(type, item, dm, **kw)

    @staticmethod
    def crawl_analysisn(type, item, dm, **kw):
        return Crawl.crawl_analysisn(type, item, dm, **kw)

    @staticmethod
    def crawl_mcn(type, item, dm, **kw):
        return Crawl.crawl_mcn(type, item, dm, **kw)

    @staticmethod
    def crawl_total_resource(type, item, dm, **kw):
        return Crawl.crawl_total_resource(type, item, dm, **kw)

    @staticmethod
    def crawl_partner(type, item, dm, **kw):
        return Crawl.crawl_partner(type, item, dm, **kw)


class MediamzSpider(ChromeSpider):
    name = 'mediamz'

    # allowed_domains = ['www.youtube.com']

    def url_type(self, url: str) -> int:
        _url = url.lower()
        if 'instagram' in _url:
            return UrlType.ins
        elif 'tiktok' in _url:
            return UrlType.tiktok
        elif 'youtube' in _url:
            return UrlType.ytb
        else:
            return UrlType.unknow

    def create_cursors(self) -> List[Tuple[Cursor, SaveToSqlPipesline]]:
        cursors = []
        name = g.db_mediamz.get('db')
        try:
            settings = g.db_mediamz
            self.create_db(settings)
        except:
            self.log(f'--- connect db fail ---',
                     level=logging.ERROR, exc_info=True)
        try:
            cursor = self.get_cursor(settings['db'])
        except:
            cursor = None
            self.log(f'--- get cursor fail ---',
                     level=logging.ERROR, exc_info=True)
        if cursor is not None:
            cursors.append((cursor, pipe_lines.__getattr__(name)))
        return cursors

    def request_from_cursor(self, cursor: Cursor, db: SaveToSqlPipesline) -> Iterator[BrowserRequest]:
        analysisn = self.get_table(cursor, Tbl.tbl_marketing_analysisn)
        mcn = self.get_table(cursor, Tbl.tbl_marketing_resource_expanded_mcn)
        resource = self.get_table(cursor, Tbl.tbl_marketing_total_resource)
        celebrity = self.get_table(cursor, Tbl.tbl_celebrity)
        partner = self.get_table(cursor, Tbl.tbl_partners)
        detail = self.get_table(cursor, Tbl.tbl_promoter_task_accept_detail)
        for a, m, r, c, p, d in zip_longest(analysisn, mcn, resource, celebrity, partner, detail):
            if a is not None:
                if should_crawl(a['last_crawler_time'], a['crawler_mode']):
                    url_type = self.url_type(a['channel_url'])
                    kw = get_ins_other_account() if url_type == UrlType.ins else {}
                    yield BrowserRequest(a['channel_url'], self.parse_channel, cont_flag=partial(MediamzCrawl.crawl_analysisn, url_type, a, db, **kw), dont_filter=True)
            if m is not None:
                if m.get('is_delete', 0) == 0:
                    url_type = self.url_type(m['resource_url'])
                    kw = get_ins_other_account() if url_type == UrlType.ins else {}
                    yield BrowserRequest(m['resource_url'], self.parse_mcn, cont_flag=partial(MediamzCrawl.crawl_mcn, url_type, m, db, **kw), dont_filter=True)
            if r is not None:
                if should_crawl(r['last_crawler_time'], r['crawler_mode']):
                    url_type = self.url_type(r['channel_url'])
                    kw = get_ins_other_account() if url_type == UrlType.ins else {}
                    yield BrowserRequest(r['channel_url'], self.parse_resource, cont_flag=partial(MediamzCrawl.crawl_total_resource, url_type, r, db, **kw), dont_filter=True)
            if c is not None:
                if c.get('is_delete', 0) == 0:
                    url_type = self.url_type(c['channel_url'])
                    kw = get_ins_other_account() if url_type == UrlType.ins else {}
                    yield BrowserRequest(c['channel_url'], self.parse_log, cont_flag=partial(MediamzCrawl.crawl_log, url_type, c, db, **kw), dont_filter=True)
            if p is not None:
                url_type = self.url_type(p['spider_url'])
                kw = get_ins_other_account() if url_type == UrlType.ins else {}
                yield BrowserRequest(p['spider_url'], self.parse_partner, cont_flag=partial(MediamzCrawl.crawl_partner, url_type, p, db, **kw), dont_filter=True)
            if d is not None:
                url_type = self.url_type(d['channel_url'])
                kw = get_ins_other_account() if url_type == UrlType.ins else {}
                yield BrowserRequest(d['channel_url'], self.parse_detail, cont_flag=partial(MediamzCrawl.crawl_promoter_detail, url_type, d, db, **kw), dont_filter=True)

    def start_requests(self):
        cursors = self.create_cursors()
        datas = [self.request_from_cursor(cursor, db)
                 for cursor, db in cursors]
        
        for tr in zip_longest(*datas):
            yield from tr

    def parse_detail(seflf, resp): ...

    def parse_analysisn(self, response): ...

    def parse_mcn(self, response): ...

    def parse_resource(self, response): ...

    def parse_log(self, response): ...

    def parse_partner(self, response): ...
