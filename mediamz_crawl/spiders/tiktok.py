# - coding=utf8 -#
import re
import time
import random
import logging

from scrapy.http import HtmlResponse
from scrapy.exceptions import IgnoreRequest
from typing import Iterator, List, Optional, Tuple

from scrapy_selenium import (pipe_lines, driver, request, waiter,
                             spider, g, dict_to_obj, restart_browser, make_response)
                             
from scrapy_selenium.pipelines import SaveToSqlPipesline

from scrapy_selenium.utils import time_now
from scrapy_selenium.request import BrowserRequest
from scrapy_selenium.vpnconnect import connect_vpn
from scrapy_selenium.decorators import Trigger, trigger
from scrapy_selenium.selenium_spiders import ChromeSpider
from scrapy_selenium.exceptions import TimeoutException

from mediamz_crawl.items import MediamzCrawlItem
from mediamz_crawl.spiders import should_crawl, Tbl

from functools import reduce, partial
from itertools import zip_longest

from pymysql.cursors import Cursor


# 处理验证码过程
def parse_captcha():
    recommend_selector = 'a[class*="user-item"]'
    verify_selector = '#captcha-verify-image'
    reg_count = 0
    refresh_count = 0
    max_count = 3
    while True:
        try:
            reg_count += 1
            verify = None
            while True:
                try:
                    refresh_count += 1
                    # print('refresh_count: ', refresh_count)
                    if refresh_count >= max_count:
                        print(f'{driver.current_url}: 刷新{max_count}次')
                        break
                    try:
                        recomand = driver.find_element_by_css_selector(
                            recommend_selector)
                        if recomand:
                            return  # 验证码问题解决,返回
                    except:
                        pass
                    verify = waiter.until(
                        lambda e: driver.find_element_by_css_selector(verify_selector))
                    break
                except TimeoutException:
                    driver.refresh()
                    print(f'{driver.current_url}: 第{refresh_count}次刷新')
                    time.sleep(3)
            if verify is not None:
                b64_image = driver.get_element_image_b64(verify)
                res = spider.flask_captcha_info('', b64_image)
                x1, y1, x2, y2 = res['image']
                button_selector = '#secsdk-captcha-drag-wrapper > div.secsdk-captcha-drag-icon.sc-kEYyzF.fiQtnm'
                button = driver.find_element_by_css_selector(
                    button_selector)
                driver.move_element(button, x1 - 2)
                try:
                    waiter.until_not(lambda e: driver.find_element_by_css_selector(
                        verify_selector), '验证码滑动失败')  # 验证码框消失,则验证码处理成功
                    if g.info_settings['verbose']:
                        print(
                            f'[{spider.run_info} {time_now()}] [{request.url}] 验证码处理成功+')
                    break
                except TimeoutException:
                    if g.info_settings['verbose']:
                        print(
                            f'[{spider.run_info} {time_now()}] [{request.url}] 验证码处理失败！')
                    raise IgnoreRequest()
            else:
                raise IgnoreRequest()
        except TimeoutException:
            # 验证码滑动失败,重新识别
            if reg_count >= max_count:
                print('验证码识别失败')
                break
            time.sleep(3)
        except Exception as e:
            # driver.get_screenshot_as_file('.logs/2.png')
            if g.info_settings['verbose']:
                print(
                    f'[{spider.run_info} {time_now()}] parse captcha error: {e}')
            raise IgnoreRequest()

# 单次抓取过程
def single_crawl() -> Optional[str]:
    try:
        if spider.url_filter.add_url(url):
            extra_message = spider.extra['channel_type']
            driver.get(url)
            recommend_selector = 'a[class*="user-item"]'  # 推荐列表
            account_selector = 'p[class*="user-title"] h4'
            user_selector = 'p[class*="user-desc"]'
            see_all_selector = 'div[class*="see-all"]'  # 查看全部

            waiter.until(
                lambda e: driver.find_element_by_css_selector(see_all_selector))
            hrefs = driver.find_elements_by_css_selector(
                recommend_selector)
            insert_values = []
            recommend_urls = []
            for href in hrefs:
                account_name = href.find_element_by_css_selector(
                    account_selector)
                user_name = href.find_element_by_css_selector(
                    user_selector)
                url = href.get_attribute('href')
                if url:
                    recommend_urls.append(url)
                    insert_values.append(
                        ('Tiktok', url, account_name.text, user_name.text, extra_message))
            if insert_values:
                spider.adb_save_ignore_many_to_sql('platforms',
                                                   ['platform', 'channel_url', 'account_name', 'user_name',
                                                    'channel_type'],
                                                   insert_values)
            if recommend_urls:
                return random.choice(recommend_urls)
            return
    except TimeoutException:
        message = '%s: 验证码识别 -- %s' % (time_now(), request.url)
        print(message)
        spider.logger.info('验证码识别 -- %s' % request.url)
        self.parse_captcha(request, spider, driver, waiter)  # 处理验证码识别问题
        waiter.until(
            lambda e: driver.find_element_by_css_selector(see_all_selector))
        hrefs = driver.find_elements_by_css_selector(recommend_selector)
        insert_values = []
        recommend_urls = []
        for href in hrefs:
            account_name = href.find_element_by_css_selector(
                account_selector)
            user_name = href.find_element_by_css_selector(
                user_selector)
            url = href.get_attribute('href')
            if url:
                recommend_urls.append(url)
                insert_values.append(
                    ('Tiktok', url, account_name.text, user_name.text, extra_message))
        if insert_values:
            spider.adb_save_ignore_many_to_sql('platforms', ['platform', 'channel_url', 'account_name', 'user_name',
                                                             'channel_type'],
                                               insert_values)
        if recommend_urls:
            return random.choice(recommend_urls)
        return
    except Exception as e:
        connect_vpn(spider)
        raise IgnoreRequest()

# 抓取channel
def parse_start():
    next_url = single_crawl(
        request.url, request, spider, driver, waiter)
    while True:
        try:
            next_url = single_crawl(
                next_url, request, spider, driver, waiter)
        except IgnoreRequest:
            ...

# 解析数字
def parse_tiktok_number(num_str):
    pat = re.compile(r'\d+\.?\d*[KMkm]?')
    if pat.match(num_str):
        number = pat.match(num_str).group()
        u = number[-1]
        rate = 1
        if u.lower() in 'km':
            if u.lower() == 'k':
                rate = 1000
            if u.lower() == 'm':
                rate = 1000 * 1000
            number = number[:-1]

        if number.count('.') > 0:
            number = int(float(number) * rate)
        else:
            number = int(number) * rate
        return number
    return 0


class TiktokCrawl:

    @staticmethod
    def parse_update_time(styles: str) -> int:
        if styles:
            try:
                update_time = int(re.findall(r'(\d+)\?x-expires', styles)[0])
            except:
                update_time = 0
            return update_time
        return 0

    @staticmethod
    def crawl_log(row: dict, dm: SaveToSqlPipesline):
        logger = spider.logger
        row = dict_to_obj(row)
        channel_url = row.channel_url
        db = dm
        logger.info(f'----- crawl spider log {channel_url} ------')
        try:
            driver.get(channel_url)
            try:
                driver.find_element_by_xpath(
                    '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            except Exception as e:
                logger.info(e, exc_info=True)
                logger.info('refresh now!!!')
                driver.refresh()
                time.sleep(3)

            fans = driver.find_element_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            like = driver.find_element_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[3]/strong')
            items = driver.find_elements_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/main/div[2]/div[1]/div')
            store = []

            _data = dict(now_followers=row.now_followers, last_crawler_time=time.time(), create_time=time.time(),
                         celebrity_id=row.id)

            for i in items[:6]:
                store.append(i.text)
            _a_list = []
            for i in items[:6]:
                a = i.find_element_by_tag_name('a')
                _a_list.append(a.get_attribute('href'))
            try:
                a = items[0].find_element_by_tag_name('a')
                _div = a.find_element_by_tag_name('div')
                _url = _div.get_attribute('style')
                logger.info(f'-----style is: {_url}------------------')
                try:
                    dt = TiktokCrawl.parse_update_time(_url)
                    logger.info(f'------parsed url raw dt is: {dt}-----------')
                    if dt:
                        _data.update(last_update_time=dt)
                except:
                    ...
            except Exception as e:
                logger.info(e, exc_info=True)
            if fans is not None:
                fans = parse_tiktok_number(fans.text)
                _data.update(now_followers=fans)

            channel_status = row.channel_status

            if store is not None:
                store = store[:6]
                store = list(map(parse_tiktok_number, store))
                count = len(store)
                if count > 0:
                    _sum = reduce(lambda x, y: x + y, store)
                    avg = _sum / count
                    _data.update(now_avg_watchs=avg, watchs=_sum)
                    if avg > row.now_avg_watchs and _data.get('now_followers') > row.now_followers:
                        channel_status = 2
                    elif avg < row.now_avg_watchs and _data.get('now_followers') < row.now_followers:
                        channel_status = 4
                    else:
                        channel_status = row.channel_status or 1
            _celebrity_data = _data.copy()
            _celebrity_data.pop('create_time')
            _celebrity_data.pop('celebrity_id')
            _celebrity_data.pop('watchs', None)
            _celebrity_data.update(channel_status=channel_status, id=row.id)
            db.insert(Tbl.tbl_celebrity_spider_log, _data, logger)
            db.update(Tbl.tbl_celebrity, ('id', row.id),
                      _celebrity_data, logger)
            logger.info('-----tiktok celebrity log-------')
            logger.info(_data)
            logger.info('------------------')
        except Exception as e:
            logger.error(e, exc_info=True)

    # 根据频道url, 抓取数据
    @staticmethod
    @trigger(frequencys=5, calls=restart_browser, events="restart", raise_as=IgnoreRequest,
             states={'valid': (1, True, False)})
    def crawl_analysisn():
        try:
            create_time = int(time.time())
            driver.get(request.url)  # 注意页面超时
            verify_selector = '#captcha-verify-image'
            try:
                # 访问前两次假定 会出现 验证码 监测, 超时等待
                if TiktokCrawl.crawl_analysisn.valid():
                    waiter.until(
                        lambda d: d.find_element_by_css_selector(verify_selector))
                    parse_captcha(request, spider, driver, waiter)
                else:
                    # 假定没有验证码监测, 非超时等待
                    driver.find_element_by_css_selector(verify_selector)
                    parse_captcha(request, spider, driver, waiter)
            except:
                ...
            waiter.until(lambda d: d.find_element_by_css_selector(
                'div[class*="video-card-mask"] strong[class*="video-count"]'), message='not find video')
            last_crawler_time = int(time.time())
            channel_url = driver.current_url

            # 最近6个视屏的平均点赞量
            lastest6_selector = 'div.video-feed-item a.video-feed-item-wrapper'
            lastest6_urls = [a.get_attribute('href').strip(
            ) for a in driver.find_elements_by_css_selector(lastest6_selector)]
            lastes6_likes = []

            page_source = driver.page_source

            for url in lastest6_urls[:6]:
                try:
                    driver.get(url)
                except Exception as e:
                    spider.logger.info(e, exc_info=True)
                like_selector = 'strong.bar-item-text'
                try:
                    _like = waiter.until(
                        lambda d: d.find_element_by_css_selector(like_selector)).text
                    lastes6_likes.append(_like)
                except Exception as e:
                    driver.refresh()
                    _like = waiter.until(
                        lambda d: d.find_element_by_css_selector(like_selector)).text
                    lastes6_likes.append(_like)

            if len(lastes6_likes) > 0:
                _like_list = list(map(parse_tiktok_number, lastes6_likes))
                _avg = reduce(lambda x, y: x + y, _like_list) / len(_like_list)
                request.meta.update(channel_avg_likes=_avg)
            else:
                request.meta.update(channel_avg_likes=0)

            request.meta.update(create_time=create_time,
                                last_crawler_time=last_crawler_time, channel_url=channel_url)

            return make_response(driver.current_url, page_source)

        except IgnoreRequest as e:  # 加载页面超时
            if g.info_settings['verbose']:
                print(
                    f'[{spider.run_info} {time_now()}] load fail: {str(e).strip()} {driver.current_url}')
                raise e
        except TimeoutException as e:  # 可能无视屏数据 或者反爬监测, 尝试更换ip重启浏览器
            if g.info_settings['verbose']:
                print(
                    f'[{spider.run_info} {time_now()}] crawl fail: {str(e).strip()} [页面结构可能发生变化] {driver.current_url}')
            # driver.get_screent_png().save(f'./logs/{spider.run_info}.png')
            spider.logger.error(
                f'[{spider.run_info} {time_now()}] crawl fail: {e} [页面结构可能发生变化] {driver.current_url}')
            raise Trigger('restart', msg='页面结构可能发生变化或需要验证码')

        except Exception as e:
            if g.info_settings['verbose']:
                print(
                    f'[{spider.run_info} {time_now()}] unknow error: {str(e).strip()} {driver.current_url}')
            spider.logger.error(
                f'{spider.run_info} unknow error: {e} [{driver.current_url}]')
            # spider.adb_save_fail_url('TikTok', request.url, f'{e}', self.time_stamp())
            # driver.get_screent_png().save(f'./logs/{driver.current_url}-崩溃.png')
            raise Trigger('restart', msg='可能需要重启浏览器')

    @staticmethod
    def crawl_mcn(item: dict, dm: SaveToSqlPipesline):
        logger = spider.logger
        row = dict_to_obj(item)
        db = dm
        try:
            logger.info(f'---------resource url {row.resource_url}-----------')
            driver.get(row.resource_url)
            time.sleep(3)
            try:
                driver.find_element_by_xpath(
                    '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            except Exception as e:
                logger.error(e, exc_info=True)
                logger.info('refresh now .....')
                logger.refresh()
                time.sleep(3)
            fans = driver.find_element_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            items = driver.find_elements_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/main/div[2]/div[1]/div')
            store = []
            for i in items:
                store.append(i.text)
            fans = parse_tiktok_number(fans.text)
            store = store[:6]
            store = list(map(parse_tiktok_number, store))
            _data = dict(subscription=fans)
            if len(store) > 0:
                avg = reduce(lambda x, y: x + y, store) / len(store)
                if avg:
                    _data.update(avg_watchs=avg)
            logger.info('-' * 50)
            logger.info(_data)
            logger.info('-' * 50)
            try:
                a = items[0].find_element_by_tag_name('a')
                _div = a.find_element_by_tag_name('div')
                _url = _div.get_attribute('style')
                logger.info(f'-----style is: {_url}------------------')
                dt = TiktokCrawl.parse_update_time(_url)
                logger.info(f'------parsed url raw dt is: {dt}-----------')
                if dt:
                    _data.update(last_update_time=dt)
            except Exception as e:
                logger.info(e, exc_info=True)
            db.update(Tbl.tbl_marketing_resource_expanded_mcn,
                      ('id', row.id), _data, logger)
        except Exception as e:
            logger.error(e, exc_info=True)

    @staticmethod
    def crawl_partner(item: dict, dm: SaveToSqlPipesline):
        update_table = 'tbl_partners'
        row = dict_to_obj(item)
        logger = spider.logger
        db = dm
        if not row.spider_url:
            return
        try:
            driver.get(row.spider_url)
            logger.info(f'------url is: {row.spider_url}-----------')
            try:
                driver.find_element_by_xpath(
                    '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            except Exception as e:
                logger.info(e, exc_info=True)
                logger.info('refresh now .............')
                driver.refresh()
                time.sleep(3)
            fans = driver.find_element_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            logger.info(fans.text)
            fans = parse_tiktok_number(fans.text)
            _data = dict(subscribes=fans, last_crawler_time=time.time())
            logger.info('-' * 60)
            logger.info(_data)
            logger.info('-' * 60)
            db.update(update_table, ('id', row.id), _data, logger)
        except Exception as e:
            logger.error(e, exc_info=True)

    @staticmethod
    def crawl_total_resource(item: dict, dm: SaveToSqlPipesline):
        logger = spider.logger
        row = dict_to_obj(item)
        channel_url = row.channel_url
        logger.info('----tiktok total resource crawl----')
        try:
            driver.get(channel_url)
            logger.info(
                f'--------row id is: {row.id} url is: {channel_url}-----------')
            try:
                driver.find_element_by_xpath(
                    '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            except Exception as e:
                logger.info(e, exc_info=True)
                logger.info('refresh now!!!')
                driver.refresh()
                time.sleep(3)

            fans = driver.find_element_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
            like = driver.find_element_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[3]/strong')
            items = driver.find_elements_by_xpath(
                '//*[@id="main"]/div[2]/div[2]/div/main/div[2]/div[1]/div')
            store = []
            avg_watch = 0

            _data = dict(last_crawler_time=time.time())
            # dm.update_total_resource(row.id, _data)

            try:
                a = items[0].find_element_by_tag_name('a')
                _div = a.find_element_by_tag_name('div')
                _url = _div.get_attribute('style')
                logger.info(f'-----style is: {_url}------------------')
                dt = TiktokCrawl.parse_update_time(_url)
                logger.info(f'------parsed url raw dt is: {dt}-----------')
                if dt:
                    _data.update(last_update_time=dt)
                    # dm.update_total_resource(row.id, {'last_update_time': dt})
            except Exception as e:
                logger.info(e, exc_info=True)

            for i in items[:6]:
                store.append(i.text)
            _a_list = []
            for i in items[:6]:
                a = i.find_element_by_tag_name('a')
                _a_list.append(a.get_attribute('href'))

            if fans is not None:
                fans = parse_tiktok_number(fans.text)
                _data.update(channel_subscription=fans)
                # dm.update_total_resource(row.id, dict(channel_subscription=fans))
            if store is not None:
                store = store[:6]
                store = list(map(parse_tiktok_number, store))
                count = len(store)
                if count > 0:
                    _sum = reduce(lambda x, y: x + y, store)
                    avg = _sum / count
                    avg_watch = avg
                    _data.update(channel_avg_watchs=avg)
                    # dm.update_total_resource(row.id, dict(channel_avg_watchs=avg))

            _like_list = []
            if len(_a_list) > 0:
                logger.info('找每到个视频的点赞数据')
                for i in _a_list:
                    driver.get(i)
                    time.sleep(4)
                    like_xpath = '//*[@id="main"]/div[2]/div[2]/div/div/main/div/div[1]/span/div/div[1]/div[5]/div[2]/div[1]/strong'
                    try:
                        _like = driver.find_element_by_xpath(like_xpath).text
                        logger.info(f'like is : {_like}')
                        _like_list.append(_like)
                    except:
                        driver.refresh()
                        logger.info('refreshed.......')
                        time.sleep(3)
                        _like = driver.find_element_by_xpath(like_xpath).text
                        _like_list.append(_like)

                if len(_like_list) > 0:
                    _like_list = list(map(parse_tiktok_number, _like_list))
                    _avg = reduce(lambda x, y: x + y,
                                  _like_list) / len(_like_list)
                    logger.info(f'tiktok avg is : {_avg}')
                    avg_like = _avg
                    _data.update(channel_avg_likes=_avg)
                    # _data = dict(channel_avg_likes=_avg)
                    if avg_watch and avg_like:
                        interact_rate = avg_like / avg_watch * 100
                        logger.info(
                            f'-----------login info interact rate is: {interact_rate}----------------')
                        _data.update(channel_interact_rate=interact_rate)
                    # dm.update_total_resource(row.id, _data)

            dm.update(Tbl.tbl_marketing_total_resource, ('id', row.id), _data)
        except Exception as e:
            logger.info(e, exc_info=True)

    @staticmethod
    def crawl_promoter_detail(item: dict, dm: SaveToSqlPipesline):
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
            fans = parse_tiktok_number(fans.text)
            store = []
            for i in items:
                store.append(i.text)
            store = store[:6]
            store = list(map(parse_tiktok_number, store))
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
                dt = TiktokCrawl.parse_update_time(_url)
                logger.info(f'------parsed url raw dt is: {dt}-----------')
                if dt:
                    _data.update(crawl_last_update_time=dt)
            except Exception as e:
                logger.info(e, exc_info=True)
            dm.update(Tbl.tbl_promoter_task_accept_detail,
                      ('id', row.id), _data, logger)
        except Exception as e:
            logger.error(e, exc_info=True)


# region
# 抓取tiktok channel
# class TiktokSpider(ChromeSpider):
    # name = "tiktok"

    # allowed_domains = ['www.tiktok.com']

    # start_urls = ['https://www.tiktok.com/@imkevinhart?lang=zh-Hant-TW']

    # custom_settings = {
    #     "LOG_LEVEL": "INFO",  # 减少Log输出量，仅保留必要的信息
    #     # 'LOG_FILE': f'logs/{name}-{now()}-{0}.log',
    #     "DOWNLOADER_MIDDLEWARES": {
    #         "mediamz_crawl.middlewares.BrowserDownloadMiddle": 543
    #     },
    #     "ITEM_PIPELINES": {"mediamz_crawl.pipelines.CrawlSocialsPipeline": 300},
    #     'EXTENSIONS': {
    #         'mediamz_crawl.extensions.EMailExensions': 500
    #     }

    # }

    # extra = {'channel_type': '明星'}

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)

    # def start_requests(self):
    #     url = self.start_urls[0]
    #     yield self.browser_request(url, TiktokbasicFlag.start_url)

    # def parse(self, response):
    #     pass
# endregion


# 解析channel 数据
class TiktokcrawlerSpider(ChromeSpider):

    name = "tiktok"

    allowed_domains = ['www.tiktok.com']

    def is_tiktok(self, url: str) -> bool:
        return bool(re.search(r'tiktok', url))

    def create_cursors(self) -> List[Tuple[Cursor, SaveToSqlPipesline]]:
        cursors = []
        for k in g.dbs:
            settings = g.dbs[k]
            try:
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
                if self.is_tiktok(a['channel_url']) and should_crawl(a['last_crawler_time'], a['crawler_mode']):
                    yield BrowserRequest(a['channel_url'], self.parse_channel, cont_flag=TiktokCrawl.crawl_analysisn, dont_filter=True)
            if m is not None:
                if self.is_tiktok(m['resource_url']) and m.get('is_delete', 0) == 0:
                    yield BrowserRequest(m['resource_url'], self.parse_channel, cont_flag=partial(TiktokCrawl.crawl_mcn, m, db), dont_filter=True)
            if r is not None:
                if self.is_tiktok(r['channel_url']) and should_crawl(a['last_crawler_time'], a['crawler_mode']):
                    yield BrowserRequest(r['channel_url'], self.parse_resource, cont_flag=partial(TiktokCrawl.crawl_total_resource, r, db), dont_filter=True)
            if c is not None:
                if self.is_tiktok(c['channel_url']) and c.get('is_delete', 0) == 0:
                    yield BrowserRequest(c['channel_url'], self.parse_log, cont_flag=partial(TiktokCrawl.crawl_log, c, db), dont_filter=True)
            if p is not None:
                if self.is_tiktok(p['spider_url']):
                    yield BrowserRequest(p['spider_url'], self.parse_partner, cont_flag=partial(TiktokCrawl.crawl_partner, p, db), dont_filter=True)
            if d is not None:
                if self.is_tiktok(d['channel_url']):
                    yield BrowserRequest(d['channel_url'], self.parse_detail, cont_flag=partial(TiktokCrawl.crawl_promoter_detail, d, db), dont_filter=True)

    def start_requests(self):
        cursors = self.create_cursors()
        datas = [self.request_from_cursor(cursor, db)
                 for cursor, db in cursors]
        if datas:
            yield from zip_longest(*datas)

    # def make_requests_from_url(self, url):
    #     return BrowserRequest(url, self.parse_channel, cont_flag=crawl_channel, dont_filter=False)

    def parse_tiktok_number(self, value: str) -> int:
        num = re.findall('(\d+\.?\d+)(\w?)', value)
        if num:
            number, unit = num[0]
            number = float(number.strip())
            if unit.lower() == 'k':
                unit = 1000
            elif unit.lower() == 'm':
                unit = 1000000
            elif unit == '':
                unit = 1
            return int(number * unit)
        return 0

    def parse_analysisn(self, response: HtmlResponse):
        try:
            # 最近6个视屏的平均点赞量
            avg_likes = response.meta['channel_avg_likes']

            name_selector = 'h2[class*="share-title"]'
            name = response.css(f'{name_selector}::text').get('')
            subs_selector = 'div.number:nth-child(2) > strong:nth-child(1)'
            subs_count = response.css(subs_selector + '::text').get('')

            # 频道订阅量
            subs_count = self.parse_tiktok_number(subs_count)

            lates6_selector = 'div[class*="video-card-mask"] strong[class*="video-count"]::text'
            agvs = response.css(lates6_selector).getall()

            # 最近6个视屏平均观看量
            if agvs:
                agvs = agvs[:6]
                processed = [self.parse_tiktok_number(v) for v in agvs]
                avg_count = int(sum(processed) / len(processed))
            else:
                avg_count = 0

            # 频道互动率
            if avg_count and avg_likes:
                interact_rate = avg_likes / avg_count * 100
                channel_interact_rate = interact_rate
            else:
                channel_interact_rate = 0

            # 最近发布时间
            last_update_time_selector = 'div.video-feed-item:nth-child(1) span[class="lazyload-wrapper"] > div:nth-child(1)::attr(style)'
            styles = response.css(last_update_time_selector).get('')
            last_update_time = TiktokCrawl.parse_update_time(styles)

            channel_url = response.meta.get('channel_url', '')
            create_time = response.meta.get('create_time', 0)
            last_crawler_time = response.meta.get('last_crawler_time', 0)

            item = MediamzCrawlItem()
            item['platform'] = 'Tiktok'  # 平台
            item['channel_url'] = channel_url  # 渠道url
            item['channel_subscription'] = subs_count  # 渠道订阅量
            item['channel_avg_watchs'] = avg_count  # 渠道均观看量
            item['channel_interact_rate'] = channel_interact_rate  # 频道互动
            # creator = FieldType(dft='符青坤') # 创建人
            item['last_update_time'] = last_update_time
            item['create_time'] = create_time  # 时间戳
            item['last_crawler_time'] = last_crawler_time  # 上次抓取结束时间, 时间戳
            item['crawler_mode'] = 1

            self.message()
            recomends = response.css(
                'a[class*="user-item"]::attr(href)').getall()

            yield item

            if recomends:
                submit_values = [response.urljoin(url) for url in recomends]
                # self.caches.extend(submit_values)
                # if len(self.caches) == 10000:
                #     with open(f'./{self.run_info}.txt', 'a+', encoding='utf8') as file:
                #         file.writelines(self.caches)
                #         file.close()
                #         self.caches.clear()

                # 数据库多线程批量插入,产生死锁
                # self.adb_save_ignore_many_to_sql(
                #     'platforms', ['platform', 'channel_url'], submit_values)

                # for new_url in submit_values:
                #     pipelines.default.adb_save_ignore_to_sql(
                #         'platforms', platform='Tiktok', channel_url=new_url)

                # yield from self.push_urls([url for url in submit_values], TiktokbasicFlag.url_user)

        except Exception as e:
            print('error: %s' % e)

    def parse_mcn(self, response: HtmlResponse): ...

    def parse_resource(self, response: HtmlResponse): ...

    def parse_log(self, response: HtmlResponse): ...

    def parse_partner(self, response: HtmlResponse): ...
