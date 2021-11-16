import re
import time
import scrapy
import logging
import dateutil.parser

from typing import List, Iterator, Tuple
from itertools import zip_longest
from functools import reduce, partial
from datetime import datetime, timedelta

from pymysql.cursors import Cursor

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from scrapy_selenium.pipelines import SaveToSqlPipesline
from scrapy_selenium import (spider, driver, g, dict_to_obj,
                             pipe_lines, BrowserRequest, ChromeSpider)

from mediamz_crawl.spiders import should_crawl, Tbl


class YtbCrawl:

    @staticmethod
    def parse_ytb_number(num_str: str) -> int:
        if type(num_str) is int or type(num_str) is float:
            return num_str
        num_str = str(num_str)

        pat = re.compile(r'\d+(,?\.?\d*)*')
        m = pat.match(num_str)
        if m:
            try:
                unit = num_str[m.end()].lower()
            except IndexError:
                unit = None
            rate = 1
            if unit == '万' or unit == '萬':
                rate = 10000
            if unit == 'k':
                rate = 1000
            if unit == 'm':
                rate = 1000 * 1000
            number = pat.match(num_str).group()
            number = number.replace(',', '')
            if number.count('.') > 0:
                number = int(float(number) * rate)
            else:
                number = int(number) * rate
            return number
        return 0

    @staticmethod
    def parse_ytb_dt(dt: str) -> int:
        match = re.search(r'\d+', dt)
        dtf = None
        now = datetime.now()
        if match:
            number = int(match.group())
            _rect = match.span()
            unit = dt[_rect[1]:_rect[1] + 1]
            if unit == '分':
                dtf = now - timedelta(minutes=number)
            if unit == '小':
                dtf = now - timedelta(hours=number)
            if unit == '天':
                dtf = now - timedelta(days=number)
            if unit == '个':
                dtf = now - timedelta(days=30 * number)
        if dtf:
            return dtf.timestamp()
        return 0

    @staticmethod
    def crawl_analysisn(item: dict, dm: SaveToSqlPipesline):
        row = dict_to_obj(item)
        logger = spider.logger

        channel_url = row.channel_url
        store = []
        subscribe_count = 0
        try:
            driver.get(channel_url)
            subscriber = driver.find_element_by_css_selector(
                '#subscriber-count')  # 订阅数量
            subscribe_count = YtbCrawl.parse_ytb_number(subscriber.text)
            items = driver.find_elements_by_css_selector(
                '#items > ytd-grid-video-renderer')
            href_list = []  # 最近6个视屏的点赞量
            _data = dict(channel_subscription=subscribe_count)
            avg_watch = 0
            if len(items) > 0:
                for i in items[:6]:
                    a = i.find_element_by_tag_name('a')
                    href_list.append(a.get_attribute('href'))
                    watch_times = i.find_element_by_css_selector(
                        '#metadata-line > span:nth-child(1)').text
                    store.append(YtbCrawl.parse_ytb_number(watch_times))
            _avg = 0
            if len(store) > 0:
                _avg = reduce(lambda x, y: x + y, store) / len(store)
                avg_watch = _avg
                _data.update(channel_avg_watchs=_avg)

            wait = driver.create_waiter(4)
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
                    _data.update(last_update_time=dt.timestamp())
                    # dm.update_data(row.id, {'last_update_time': dt.timestamp()})
            except Exception as e:
                logger.info(e, exc_info=True)

            _like_list = []
            if len(href_list):
                for _url in href_list:
                    driver.get(_url)
                    time.sleep(5)
                    container = driver.find_element_by_xpath(
                        '//*[@id="menu-container"]')
                    like = container.find_element_by_css_selector('#text').text
                    logger.info(f'get like is : {like}')
                    _like_list.append(like)

            if len(_like_list) > 0:
                _like_list = list(map(YtbCrawl.parse_ytb_number, _like_list))
                _avg = reduce(lambda x, y: x + y, _like_list) / len(_like_list)
                _data.update(channel_avg_likes=_avg)
                if _avg and avg_watch:
                    interact_rate = _avg / avg_watch * 100
                    logger.info(
                        f'-------------avg watch {avg_watch} avg like {_avg} rate is: {interact_rate}-----------')
                    _data.update(channel_interact_rate=interact_rate)

            logger.info('-----youtube-------')
            logger.info(_data)
            logger.info('------------------')

            # if len(_data.keys()):
            #     dm.update_data(row.id, _data)

            # if dm.calculate_time(row):
            adverts_url = row.adverts_url
            driver.get(adverts_url)

            wait = driver.create_waiter(10)
            watch = wait.until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="count"]/ytd-video-view-count-renderer/span[1]')))
            _watch = YtbCrawl.parse_ytb_number(watch.text)
            container = driver.find_element_by_xpath(
                '//*[@id="menu-container"]')
            like = container.find_element_by_css_selector('#text').text
            _like = YtbCrawl.parse_ytb_number(like)
            driver.execute_script("window.scrollBy(0,200)")  # 往下滚动
            reply = wait.until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="count"]/yt-formatted-string/span[1]')))
            reply = reply.text
            _reply = YtbCrawl.parse_ytb_number(reply)
            if _watch is not None and _like is not None and _reply is not None:
                _like_rate = _watch > 0 and _like / _watch or 0
                _reply_rate = _watch > 0 and _reply / _watch or 0
                last_time = time.time()
                avg_watch = row.channel_avg_watchs or 0
                _data.update(adverts_plays=_watch,
                             adverts_likes=_like,
                             adverts_likes_rate=_like_rate * 100,
                             adverts_comment=_reply,
                             adverts_comment_rate=_reply_rate * 100,
                             plays_difference=_watch - avg_watch,
                             last_crawler_time=last_time)
                # dm.update_data(row.id, dict(adverts_plays=_watch,
                #                             adverts_likes=_like,
                #                             adverts_likes_rate=_like_rate * 100,
                #                             adverts_comment=_reply,
                #                             adverts_comment_rate=_reply_rate * 100,
                #                             plays_difference=_watch - avg_watch,
                #                             last_crawler_time=last_time))
            dm.update(Tbl.tbl_marketing_analysisn, ('id', row.id), _data, logger)

        except Exception as e:
            logger.error(e, exc_info=True)

    @staticmethod
    def crawl_mcn(item: dict, dm: SaveToSqlPipesline):
        row = dict_to_obj(item)
        logger = spider.logger
        try:
            driver.get(row.resource_url)
            logger.info(f'----------mcn url: {row.resource_url}---------')
            wait = driver.create_waiter(10)
            subscriber = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#subscriber-count',)))
            subscribe_count = YtbCrawl.parse_ytb_number(subscriber.text)
            items = driver.find_elements_by_css_selector(
                '#items > ytd-grid-video-renderer')
            store = []
            if len(items) > 0:
                for i in items[:6]:
                    watch_times = i.find_element_by_css_selector(
                        '#metadata-line > span:nth-child(1)').text
                    store.append(YtbCrawl.parse_ytb_number(watch_times))
            avg = 0
            if len(store) > 0:
                avg = reduce(lambda x, y: x + y, store)
            _data = dict(subscription=subscribe_count, avg_watchs=avg)
            logger.info('-' * 40)
            logger.info(_data)
            logger.info('-' * 40)
            # dm.update_mcn_task(row.id, _data)
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
                    # dm.update_mcn_task(row.id, {'last_update_time': dt.timestamp()})
                    _data.update(last_update_time=dt.timestamp())
            except Exception as e:
                logger.info(e, exc_info=True)

            dm.update(Tbl.tbl_marketing_resource_expanded_mcn,
                      ('id', row.id), _data, logger)

        except Exception as e:
            logger.error(e, exc_info=True)

    @staticmethod
    def crawl_log(item: dict, dm: SaveToSqlPipesline):
        row = dict_to_obj(item)
        logger = spider.logger
        logger.info('----- crawl spider log ------')
        t = 0 if row.last_crawler_time is None else row.last_crawler_time
        if (time.time() - t) >= Tbl.spider_limit:
            channel_url = row.channel_url
            store = []
            try:
                logger.info(
                    f"-------log crawl channel url is: {channel_url}-----------")
                driver.get(channel_url)
                subscriber = driver.find_element_by_css_selector(
                    '#subscriber-count')
                subscribe_count = YtbCrawl.parse_ytb_number(
                    subscriber.text)  # 订阅数量
                items = driver.find_elements_by_css_selector(
                    '#items > ytd-grid-video-renderer')  # 首页最近6个视频

                href_list = []
                _data = dict(now_followers=subscribe_count, last_crawler_time=time.time(), create_time=time.time(),
                             celebrity_id=row.id)
                if len(items) > 0:
                    for i in items[:6]:
                        a = i.find_element_by_tag_name('a')
                        href_list.append(a.get_attribute('href'))  # 视屏具体地址
                        watch_times = i.find_element_by_css_selector(
                            '#metadata-line > span:nth-child(1)').text
                        store.append(YtbCrawl.parse_ytb_number(
                            watch_times))  # 视屏观看次数

                wait = driver.create_waiter(5)
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
                    if dt:
                        _data.update(last_update_time=dt.timestamp())
                except Exception as e:
                    logger.info(e, exc_info=True)

                # 平均观看量
                _avg = 0
                if len(store) > 0:
                    _sum = reduce(lambda x, y: x + y, store)
                    _avg = _sum / len(store)
                    _data.update(now_avg_watchs=_avg, watchs=_sum)
                else:
                    _data.update(now_avg_watchs=row.now_avg_watchs)

                _celebrity_data = _data.copy()
                _celebrity_data.pop('create_time')
                _celebrity_data.pop('celebrity_id')
                _celebrity_data.pop('watchs', None)
                _celebrity_data.update(id=row.id)

                if _avg > row.now_avg_watchs and subscribe_count > row.now_followers:
                    channel_status = 2
                elif _avg < row.now_avg_watchs and subscribe_count < row.now_followers:
                    channel_status = 4
                else:
                    channel_status = row.channel_status or 1
                _celebrity_data.update(channel_status=channel_status)

                dm.insert(Tbl.tbl_celebrity_spider_log, _data, logger)
                dm.update(Tbl.tbl_celebrity, ('id', row.id),
                          _celebrity_data, logger)

                logger.info('-----youtube celebrity log-------')
                logger.info(_data)
                logger.info('------------------')

            except Exception as e:
                logger.info(e, exc_info=True)
        else:
            logger.info(f'-------log crawl {row.channel_url} cancel -----')

    @staticmethod
    def crawl_total_resource(item: dict, dm: SaveToSqlPipesline):
        row = dict_to_obj(item)
        logger = spider.logger
        dm = pipe_lines.mediamz
        channel_url = row.channel_url
        store = []
        try:
            logger.info(
                f"-------row id is: {row.id} channel url is: {channel_url}-----------")
            driver.get(channel_url)
            subscriber = driver.find_element_by_css_selector(
                '#subscriber-count')
            subscribe_count = YtbCrawl.parse_ytb_number(
                subscriber.text)  # 订阅数量
            items = driver.find_elements_by_css_selector(
                '#items > ytd-grid-video-renderer')  # 首页最近6个视频
            href_list = []
            _data = dict(channel_subscription=subscribe_count,
                         last_crawler_time=time.time())
            avg_watch = 0
            if len(items) > 0:
                for i in items[:6]:
                    a = i.find_element_by_tag_name('a')
                    href_list.append(a.get_attribute('href'))  # 视屏具体地址
                    watch_times = i.find_element_by_css_selector(
                        '#metadata-line > span:nth-child(1)').text
                    store.append(YtbCrawl.parse_ytb_number(
                        watch_times))  # 视屏观看次数

            wait = driver.create_waiter(5)
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
                if dt:
                    _data.update(last_update_time=dt.timestamp())
                    # dm.update_total_resource(row.id, {'last_update_time': dt.timestamp()})
            except Exception as e:
                logger.info(e, exc_info=True)

            # 平均观看量
            _avg = 0
            if len(store) > 0:
                _avg = reduce(lambda x, y: x + y, store) / len(store)
                avg_watch = _avg
                _data.update(channel_avg_watchs=_avg)

            _like_list = []
            try:
                if len(href_list):
                    for _url in href_list:
                        driver.get(_url)
                        time.sleep(5)
                        container = driver.find_element_by_xpath(
                            '//*[@id="menu-container"]')
                        like = container.find_element_by_css_selector(
                            '#text').text
                        logger.info(f'get like is : {like}')
                        _like_list.append(like)

                if len(_like_list) > 0:
                    _like_list = list(
                        map(YtbCrawl.parse_ytb_number, _like_list))
                    _avg = reduce(lambda x, y: x + y,
                                  _like_list) / len(_like_list)
                    _data.update(channel_avg_likes=_avg)
                    if _avg and avg_watch:
                        interact_rate = _avg / avg_watch * 100
                        logger.info(
                            f'-------------av {avg_watch} al {_avg} rate: {interact_rate}-----------')
                        _data.update(channel_interact_rate=interact_rate)
            except Exception as e:
                logger.info(e)

            logger.info('-----youtube-------')
            logger.info(_data)
            logger.info('------------------')

            dm.update(Tbl.tbl_marketing_total_resource,
                      ('id', row.id), _data, logger)
            # dm.update_total_resource(row.id, _data)
        except Exception as e:
            logger.info(e, exc_info=True)

    @staticmethod
    def crawl_partner(item: dict, dm: SaveToSqlPipesline):
        row = dict_to_obj(item)
        logger = spider.logger
        if not row.spider_url:
            return
        try:
            driver.get(row.spider_url)
            logger.info(f'-------spider url is: {row.spider_url}-----')
            wait = driver.create_waiter(10)
            subscribe = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#subscriber-count',)))
            subscribe = YtbCrawl.parse_ytb_number(subscribe.text)
            _data = dict(subscribes=subscribe, last_crawler_time=time.time())
            logger.info('-' * 40)
            logger.info('youtube partner crawler')
            # logger.info(_data)
            logger.info('-' * 40)

            dm.update(Tbl.tbl_partners, ('id', row.id), _data, logger)
            # dm.update_partners(row.id, _data)

        except Exception as e:
            logger.error(e, exc_info=True)
            try:
                driver.get_screenshot_as_file('partner_exc.png')
            except Exception as e:
                logger.info('get screen timeout')

    @staticmethod
    def crawl_promoter_detail(item: dict, dm: SaveToSqlPipesline):
        row = dict_to_obj(item)
        logger = spider.logger
        try:
            driver.get(row.channel_url)
            logger.info(
                f'----------mcn channel url: {row.channel_url}---------')
            wait = driver.create_waiter(10)
            subscriber = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#subscriber-count',)))
            subscribe_count = YtbCrawl.parse_ytb_number(subscriber.text)
            items = driver.find_elements_by_css_selector(
                '#items > ytd-grid-video-renderer')
            store = []
            if len(items) > 0:
                for i in items[:6]:
                    watch_times = i.find_element_by_css_selector(
                        '#metadata-line > span:nth-child(1)').text
                    store.append(YtbCrawl.parse_ytb_number(watch_times))
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
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#tabsContent > tp-yt-paper-tab:nth-child(12) > div')))
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


class YtbCrawlerSpider(ChromeSpider):

    name = 'youtube'

    allowed_domains = ['www.youtube.com']


    def is_ytb(self, url: str):
        return bool(re.search(r'youtube', url, re.I))

    def create_cursors(self) -> List[Tuple[Cursor, SaveToSqlPipesline]]:
        cursors = []
        for k in g.dbs:
            try:
                settings = g.dbs[k]
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
                cursors.append((cursor, pipe_lines.__getattr__(k)))
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
                if self.is_ytb(a['channel_url']) and should_crawl(a['last_crawler_time'], a['crawler_mode']):
                    yield BrowserRequest(a['channel_url'], self.parse_analysisn, cont_flag=partial(YtbCrawl.crawl_analysisn, a, db), dont_filter=True)
            if m is not None:
                if self.is_ytb(m['resource_url']) and m.get('is_delete', 0) == 0:
                    yield BrowserRequest(m['resource_url'], self.parse_mcn, cont_flag=partial(YtbCrawl.crawl_mcn, m, db), dont_filter=True)
            if r is not None:
                if self.is_ytb(r['channel_url']) and should_crawl(a['last_crawler_time'], a['crawler_mode']):
                    yield BrowserRequest(r['channel_url'], self.parse_resource, cont_flag=partial(YtbCrawl.crawl_total_resource, r, db), dont_filter=True)
            if c is not None:
                if self.is_ytb(c['channel_url']) and c.get('is_delete', 0) == 0:
                    yield BrowserRequest(c['channel_url'], self.parse_log, cont_flag=partial(YtbCrawl.crawl_log, c, db), dont_filter=True)
            if p is not None:
                if self.is_ytb(p['spider_url']):
                    yield BrowserRequest(p['spider_url'], self.parse_partner, cont_flag=partial(YtbCrawl.crawl_partner, p, db), dont_filter=True)
            if d is not None:
                if self.is_ytb(d['channel_url']):
                    yield BrowserRequest(d['channel_url'], self.parse_detail, cont_flag=partial(YtbCrawl.crawl_promoter_detail, d, db), dont_filter=True)

    def start_requests(self):
        cursors = self.create_cursors()
        datas = [self.request_from_cursor(cursor, db) for cursor, db in cursors]
        if datas:
            yield from zip_longest(*datas)

    def parse_analysisn(self, resp):
        ...

    def parse_mcn(self, resp):
        ...

    def parse_resource(self, resp):
        ...

    def parse_log(self, resp):
        ...

    def parse_partner(self, resp):
        ...

    def parse_detail(seflf, resp):
        ...


# region
# class YoutuBeRedisControl(BrowserController):
#     parse_watch = 1  # 控制watch观看页面的浏览器的行为

#     parse_end = 2  # 处理博主详情页面的浏览器的行为

#     video_page = 3  #

#     @action(parse_watch)
#     def from_watchs(self, request, spider: 'YoutubeSpider', driver: SeleniumChrome, waiter: 'WebDriverWait'):
#         print('222222============================================>')
#         driver.get(request.url)
#         for i in range(1):  # 滑动鼠标滚轮，获取更多推荐链接
#             driver.scroll_window(0, 1000)
#         request.meta['channel_url'] = request.url
#         print('gggggggg =======================>')
#         return self.make_response(driver.current_url, driver.page_source, request)

#     @action(parse_end)
#     def scrape_info(self, request, spider: 'YoutubeSpider', driver: SeleniumChrome, waiter: 'WebDriverWait'):
#         try:
#             create_time = self.time_stamp()
#             print('xxxxxxx=====================>')
#             driver.get(request.url)

#             # 视频按钮
#             video_selector = "#tabsContent > tp-yt-paper-tab:nth-child(4)"
#             video = waiter.until(
#                 EC.presence_of_element_located(
#                     (By.CSS_SELECTOR, video_selector))
#             )

#             # 正常可爬取数据为: 首页-视屏-播放列表-社区-频道-简介的形式; 过滤掉不可爬取的页面
#             video_flag_selector = 'div[class="tab-content style-scope tp-yt-paper-tab"]'
#             elements = driver.find_elements_by_css_selector(
#                 video_flag_selector)
#             if elements[1].text.strip() == '视屏':
#                 raise IgnoreRequest(driver.current_url)  # 过滤掉请求

#             self.click(driver, video)  # 切换至视频栏
#             self.sleep(0.5)

#             counts_selector = "#metadata-line > span:nth-child(1)"  # 播放次数选择器

#             waiter.until(
#                 EC.presence_of_element_located(
#                     (By.CSS_SELECTOR, counts_selector))
#             )
#             last_crawler_time = self.time_stamp()
#             request.meta['create_time'] = create_time
#             request.meta['last_crawler_time'] = last_crawler_time
#             request.meta['channel_url'] = driver.current_url
#             return self.make_response(driver.current_url, driver.page_source, request)

#         except TimeoutException as e:
#             if g.info_settings['verbose']:
#                 print(f'{spider.run_info} crawl fail: {str(e).strip()} [页面结构可能发生变化]')
#             spider.logger.error(f'{spider.run_info} crawl fail: {e} [页面结构可能发生变化] {request.url}')
#             raise IgnoreRequest()

#         except Exception as e:
#             if g.info_settings['verbose']:
#                 print(f'{spider.run_info} unknow error: {str(e).strip()} [尝试更换ip,排查浏览器是否崩溃,或重启浏览器]')
#             spider.logger.error(f'{spider.run_info} unknow error: {e} [{request.url}]')
#             raise IgnoreRequest()


# class YoutubeSpider(RedisChromeSpider):
#     name = 'youtube'

#     redis_key = 'youtube:start_urls'

#     start_urls = ["https://www.youtube.com/watch?v=AGWBLv4Rr9I"]

#     def make_requests_from_url(self, url):
#         return self.browser_request(url, self.parse_watch, cont_flag=YoutuBeRedisControl.parse_watch, dont_filter=True)

#     def parse_watch(self, response):
#         youtuber_links = LinkExtractor(restrict_css=("yt-formatted-string#text")).extract_links(response)

#         for link in youtuber_links:
#             yield self.browser_request(link.url, self.parse_item, cont_flag=YoutuBeRedisControl.parse_end, priority=500)

#         all_links = response.css("#dismissible > ytd-thumbnail a::attr(href)").getall()
#         no_need_links = response.css(
#             'a[class="yt-simple-endpoint style-scope ytd-compact-movie-renderer"]::attr(href)').getall()
#         for no_need in no_need_links:
#             if no_need in all_links:
#                 all_links.remove(no_need)
#         all_links = [response.urljoin(link) for link in all_links]
#         for url in all_links:
#             yield self.browser_request(url, self.parse_watch, cont_flag=YoutuBeRedisControl.parse_watch, priority=0)

#     def parse_item(self, response):
#         item = MediamzCrawlItem()

#         latests = response.css(
#             "#metadata-line > span:nth-child(1)::text").getall()  # 最近视频观看量

#         if latests and len(latests) >= 6:  # 字符串转换数字
#             length = 6
#         else:
#             length = len(latests)

#         avg = 0
#         if length > 0:
#             # 4331次观看 pattern: r'\d+'  123万次  replace: ('次', ''）  123,12次  replace: (',', '')
#             res = (
#                 self.str2value(v, pattern="\d+\,?\d+万?(?=次)",
#                                replace=(",", ""))
#                 for v in latests[:length]
#             )
#             avg = sum(res) / length

#         subs_count = response.css("#subscriber-count::text").get(0)  # 订阅量

#         if subs_count != 0:
#             subs_count = self.str2value(
#                 subs_count, replace=("位订阅者", ""))  # 字符串转数字

#         item["platform"] = "Youtube"
#         item["channel_name"] = response.css(
#             "#channel-header-container #text::text").get('').strip()
#         item["channel_subscription"] = subs_count  # 订阅数量
#         item["channel_url"] = response.meta.get('channel_url')
#         item["channel_avg_watchs"] = avg  # 最近6个视频的平均订阅量
#         item["create_time"] = response.meta.get('create_time')
#         item["last_crawler_time"] = response.meta.get('last_crawler_time')
#         item["creator"] = 'fuqingkun'

#         # 保存博主链接至本地数据库
#         # self.adb_save_ignore_to_sql('platforms', platform='Youtube',
#         #                             channel_url=item['channel_url'], account_name=item['channel_name'])
#         return item
# endregion
