import time
import logging

from itertools import zip_longest
from functools import partial
from typing import List, Iterator, Tuple

from pymysql.cursors import Cursor

from scrapy_selenium.pipelines import SaveToSqlPipesline
from scrapy_selenium import (g, pipe_lines, BrowserRequest, ChromeSpider)

from mediamz_crawl.spiders import Crawl, Tbl, UrlType
from mediamz_crawl.spiders.ins_account import get_ins_other_account


class CelebrityCrawl:

    @staticmethod
    def crawl_log(type, item, dm, **kw):
        return Crawl.crawl_log(type, item, dm, **kw)

class CelebritySpider(ChromeSpider):
    name = 'celebrity'

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
        name = 'db_mediamz'
        try:
            settings = g.dbs[name]
            self.create_db(settings)
        except:
            self.log(f'---connect db fail---',
                        level=logging.ERROR, exc_info=True)
        try:
            cursor = self.get_cursor(settings['db'])
        except:
            cursor = None
            self.log(f'---get cursor fail---',
                        level=logging.ERROR, exc_info=True)
        if cursor is not None:
            cursors.append((cursor, pipe_lines.__getattr__(name)))
        return cursors

    def request_from_cursor(self, cursor: Cursor, db: SaveToSqlPipesline) -> Iterator[BrowserRequest]:
        detail = self.get_table(cursor, Tbl.tbl_promoter_task_accept_detail)
        for d in detail:
            if d is not None:
                url_type = self.url_type(d['channel_url'])
                if url_type == UrlType.ins:
                    kw = {'account': get_ins_other_account()}
                else:
                    kw = {}
                t = d.get('last_crawler_time', 0) 
                if (time.time() - t) >= Tbl.spider_limit:
                    yield BrowserRequest(d['channel_url'], self.parse_detail,
                                        cont_flag=partial(CelebrityCrawl.crawl_promoter_detail, url_type, d, db, **kw), dont_filter=True)

    def start_requests(self):
        cursors = self.create_cursors()
        datas = [self.request_from_cursor(cursor, db) for cursor, db in cursors]
        if datas:
            yield from zip_longest(*datas)

    def parse_detail(seflf, resp):
        ...