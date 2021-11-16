import re
import time
import scrapy
import logging
import dateutil.parser

from itertools import zip_longest
from functools import reduce, partial
from typing import List, Iterator, Tuple
from urllib.parse import urlparse


from pymysql.cursors import Cursor

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from scrapy_selenium.pipelines import SaveToSqlPipesline
from scrapy_selenium import (spider, driver, g, dict_to_obj,
                             pipe_lines, BrowserRequest, ChromeSpider)

from mediamz_crawl.spiders import should_crawl, Tbl, UrlType, YtbTools, TiktokTools, InsTools
from mediamz_crawl.spiders.ins_account import get_ins_other_account


class MediamzCnCrawl:

    @staticmethod
    def crawl_promoter_detail(type, item, dm, **kw):
        """
        type: 0 ins, 1.tiktok, 2 youtube
        """
        funcs = [MediamzCnCrawl._ins_promoter_detail, MediamzCnCrawl._tiktok_promoter_detail,
                 MediamzCnCrawl._ytb_promoter_detail]
        try:
            return funcs[type](item, dm, **kw)
        except:
            raise

    @staticmethod
    def _ins_promoter_detail(item, dm, **kw):
        row = dict_to_obj(item)
        logger = spider.logger
        account = kw.pop('account')
        try:
            InsTools.get_with_cookies(account, row.channel_url)
            driver.get(row.channel_url)
            cur_url = driver.current_url
            parsed = urlparse(row.channel_url)

            if re.search(r'/accounts/login', cur_url):
                logger.info('帐号可能被封？')
                raise Exception('account has be verify')

            cur = urlparse(cur_url)
            if parsed.path != cur.path:
                try:
                    driver.get_screenshot_as_file('ins.png')
                except:
                    logger.info('截图失败')

                raise Exception('触发验证规则？')

            action = driver.action_chains()
            fans = driver.find_element_by_xpath(
                '//*[@id="react-root"]/section/main/div/header/section/ul/li[2]/a/span')
            fans = fans.get_attribute('title').replace(',', '')
            post_list = driver.find_elements_by_xpath(
                '//*[@id="react-root"]/section/main/div/div[3]/article/div[1]/div/div')
            store = []

            _data = dict(crawl_platform='instagram', is_crawl=1)

            for lst in post_list:
                a = lst.find_elements_by_tag_name('a')
                logger.info(a)
                for ai in a:
                    action.move_to_element(ai).perform()
                    time.sleep(1)
                    t = ai.find_element_by_css_selector('.qn-0x')
                    store.append(t.text)
                    if len(store) == 6:
                        break
                if len(store) == 6:
                    break

            if fans and store:
                fans = InsTools.parse_ins_number(fans)
                _data.update(crawl_fans=fans)
                _list = list(map(InsTools.parse_ins_number, store))
                if len(_list) > 0:
                    avg = reduce(lambda x, y: x + y, _list) / len(_list)
                    _data.update(crawl_avg_plays=avg)

            try:
                chain = driver.action_chains()
                a = post_list[0].find_element_by_tag_name('a')
                chain.move_to_element(a).click().perform()
                wait = driver.create_waiter(5)
                t = wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, '/html/body/div[6]/div[2]/div/article/div/div[2]/div/div/div[2]/div[2]/a/time')))
                tt = t.get_attribute('datetime')
                logger.info(f'----------ins tt is: {tt}---------------------')
                tt = dateutil.parser.parse(tt)
                _data.update(crawl_last_update_time=tt.timestamp())

            except Exception as e:
                logger.info('get first crawl_last_update_time error')
                logger.info(e, exc_info=True)
            finally:
                dm.update(Tbl.tbl_promoter_task_accept_detail,
                          ('id', row.id), _data, logger)

        except Exception as e:
            logger.error(e, exc_info=True)

    @staticmethod
    def _tiktok_promoter_detail(item, dm, **kw):
        row = dict_to_obj(item)
        logger = spider.logger
        try:
            logger.info(
                f'-------------------promoter url {row.channel_url}------------')
            url = row.channel_url
            driver.get(url)
            try:
                driver.find_element_by_xpath(
                    '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            except Exception as e:
                logger.error(e, exc_info=True)
                logger.info('refresh now .....')
                driver.refresh()
                time.sleep(3)

            items = driver.find_elements_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/main/div[2]/div[1]/div')
            fans = driver.find_element_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            fans = TiktokTools.parse_tiktok_number(fans.text)
            store = []
            for i in items:
                store.append(i.text)
            store = store[:6]
            store = list(map(TiktokTools.parse_tiktok_number, store))
            _data = dict(crawl_platform='tiktok', is_crawl=1, crawl_fans=fans)
            if len(store) > 0:
                avg = reduce(lambda x, y: x + y, store) / len(store)
                if avg:
                    _data.update(crawl_avg_plays=avg)
            try:
                a = items[0].find_element_by_tag_name('a')
                _div = a.find_element_by_tag_name('div')
                _url = _div.get_attribute('style')
                logger.info(f'-----style is: {_url}------------------')
                dt = TiktokTools.parse_update_time(_url)
                logger.info(f'------parsed url raw dt is: {dt}-----------')
                if dt:
                    _data.update(crawl_last_update_time=dt)
            except Exception as e:
                logger.info(e, exc_info=True)
            dm.update(Tbl.tbl_promoter_task_accept_detail,
                      ('id', row.id), _data, logger)
        except Exception as e:
            logger.error(e, exc_info=True)

    @staticmethod
    def _ytb_promoter_detail(item, dm, **kw):
        row = dict_to_obj(item)
        logger = spider.logger
        try:
            driver.get(row.channel_url)
            logger.info(
                f'----------mcn channel url: {row.channel_url}---------')
            wait = driver.create_waiter(10)
            subscriber = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#subscriber-count',)))
            subscribe_count = YtbTools.parse_ytb_number(subscriber.text)
            items = driver.find_elements_by_css_selector(
                '#items > ytd-grid-video-renderer')
            store = []
            if len(items) > 0:
                for i in items[:6]:
                    watch_times = i.find_element_by_css_selector(
                        '#metadata-line > span:nth-child(1)').text
                    store.append(YtbTools.parse_ytb_number(watch_times))
            avg = 0
            _data = dict(crawl_platform='youtube',
                         crawl_fans=subscribe_count, is_crawl=1)
            if len(store) > 0:
                avg = reduce(lambda x, y: x + y, store)
                _data.update(crawl_avg_plays=avg)
            logger.info('-' * 40)
            logger.info(_data)
            logger.info('-' * 40)
            # dm.update_promoter_task(row.id, _data)
            try:
                chain = driver.action_chains()
                chain.move_to_element(items[0]).click().perform()
                ss = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '#info-strings > yt-formatted-string')))
                dt = ss.text
                logger.info(f'-------------------dt is: {dt}------------')
                dt = str(dt).replace(
                    '年', '-').replace('月', '-').replace('日', '')
                dt = dateutil.parser.parse(dt)
                driver.back()
                if dt:
                    _data.update(crawl_last_update_time=dt.timestamp())
                    # dm.update_promoter_task(row.id, {'crawl_last_update_time': dt.timestamp()})
            except Exception as e:
                logger.info(e, exc_info=True)
            try:
                about = wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, '#tabsContent > tp-yt-paper-tab:nth-child(12) > div')))
                chain = driver.action_chains()
                chain.move_to_element(about).click().perform()

                position = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR,
                                                                      '#details-container > table > tbody > tr:nth-child(2) > td:nth-child(2) > yt-formatted-string')))
                _data.update(crawl_country=position.text)
                # dm.update_promoter_task(row.id, {'crawl_country': position.text})
            except Exception as e:
                logger.info(e, exc_info=True)

            dm.update(Tbl.tbl_promoter_task_accept_detail,
                      ('id', row.id), _data, logger)

        except Exception as e:
            logger.error(e, exc_info=True)


class MediamzCnSpider(ChromeSpider):
    name = 'mediamzcn'

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
        name = 'db_mediamzcn'
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
                yield BrowserRequest(d['channel_url'], self.parse_detail,
                                     cont_flag=partial(MediamzCnCrawl.crawl_promoter_detail, url_type, d, db, **kw), dont_filter=True)

    def start_requests(self):
        cursors = self.create_cursors()
        datas = [self.request_from_cursor(cursor, db) for cursor, db in cursors]
        if datas:
            yield from zip_longest(*datas)

    def parse_detail(seflf, resp):
        ...
