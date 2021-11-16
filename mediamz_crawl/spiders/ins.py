import re
import time
import json
import pickle
import logging
import jmespath
import dateutil.parser

from urllib.parse import urlparse
from typing import List, Iterator, Tuple
from itertools import zip_longest
from functools import reduce, partial
from datetime import datetime, timedelta

from pymysql.cursors import Cursor
from redis import Redis

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from scrapy_selenium import (spider, driver, g, dict_to_obj,
                             pipe_lines, BrowserRequest, ChromeSpider)
from scrapy_selenium.pipelines import SaveToSqlPipesline

from mediamz_crawl.spiders import should_crawl, Tbl, Tools
from mediamz_crawl.spiders.ins_account import get_ins_other_account, get_ins_spider_account


class InsCrawl:

    prefix = 'ins_browser'

    @staticmethod
    def parse_ins_number(num_str: str) -> int:
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
    def parse_ins_dt(dt: str) -> int:
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
    def request_log(item: dict, dm: SaveToSqlPipesline, account: dict, url: str, logger, user_data=None):
        '''
        request 方式抓取数据
        '''
        def parse_user_info(user, data):
            pattern = 'edge_followed_by.count'
            now_followers = jmespath.search(pattern, user) or row.now_followers
            data.update(now_followers=now_followers)
            lasted = 'edge_felix_video_timeline.edges[*].{count: node.video_view_count, time: node.taken_at_timestamp}|[0:6]'
            lasted_infos = jmespath.search(lasted, user)
            if lasted_infos is not None:
                watchs = sum(map(lambda e: e['count'], lasted_infos))
                now_avg_watchs = watchs / len(lasted_infos)
                data.update(now_avg_watchs=now_avg_watchs, watchs=watchs)
                data.update(last_update_time=lasted_infos[0].get('time', row.last_update_time),
                            last_crawler_time=time.time())

                _celebrity_data = data.copy()
                _celebrity_data.pop('create_time', None)
                _celebrity_data.pop('celebrity_id', None)
                _celebrity_data.pop('watchs', None)
                _celebrity_data.update(id=row.id)

                if data.get('now_avg_watchs', row.now_avg_watchs or 0) > row.now_avg_watchs and _data.get('now_followers',
                                                                                                          row.now_followers or 0) > row.now_followers:
                    channel_status = 2
                elif data.get('now_avg_watchs', row.now_avg_watchs or 0) < row.now_avg_watchs and _data.get('now_followers',
                                                                                                            row.now_followers or 0) < row.now_followers:
                    channel_status = 4
                else:
                    channel_status = row.channel_status or 1
                _celebrity_data.update(channel_status=channel_status)
                return _celebrity_data

            return False

        row = dict_to_obj(item)
        user_name = url.strip('https://www.instagram.com/').strip('/').strip()
        if user_data:
            user = user_data
        else:
            from ins_scraper import InstagramScraper
            proxies = {'http': f'http://{account["ip"]}'}
            scraper = InstagramScraper(
                login_user=account['user'], login_pass=account['pwd'], proxies=proxies)
            user = scraper.get_shared_data_userinfo(user_name)
        _data = dict(create_time=time.time(), celebrity_id=row.id)

        if user is not None:
            celebrity_data = parse_user_info(user, _data)
            dm.insert(Tbl.tbl_celebrity_spider_log, _data, logger)

            if celebrity_data is not None:
                dm.update(Tbl.tbl_celebrity, ('id', row.id),
                          celebrity_data, logger)
                # dm.update_celebrity(celebrity_data)
            logger.info('-----ins celebrity log-------')
            logger.info(_data)
            logger.info('------------------')
            return True
        else:
            if user_data:
                user = user_data
            else:
                scraper.load_login_cookies(Tools.redis_client)
                user = scraper.get_shared_data_userinfo(user_name)
            if user is not None:
                celebrity_data = parse_user_info(user, _data)
                dm.insert(Tbl.tbl_celebrity_spider_log, _data, logger)

                if celebrity_data is not None:
                    dm.update(Tbl.tbl_celebrity, ('id', row.id),
                              celebrity_data, logger)
                logger.info('-----ins celebrity log-------')
                logger.info(_data)
                logger.info('------------------')
                return True
            logger.info(f'-----ins celebrity fail: url is {url}-------')

    @staticmethod
    def parse_spider_log_from_browser(html_content, row, dm, account, url, logger):
        shared_data = re.findall(
            r'window._sharedData\s+?\=\s+?(.*?);</script>', html_content, re.DOTALL)
        if shared_data:
            try:
                d = json.loads(shared_data[0])
            except:
                d = None
            if d is not None:
                user_data = jmespath.search(
                    'entry_data.ProfilePage[0].graphql.user', d)
                if user_data is not None:
                    return InsCrawl.request_log(row, dm, account, url, logger, user_data)
            return False

    @staticmethod
    def _save_redis_cookies(account, cookies, redis_client: Redis):
        content = pickle.dumps(cookies)
        redis_client.setex(InsCrawl.prefix + account['user'], 60*60*3, content)

    @staticmethod
    def _get_login_cookies(account, redis_client: Redis):
        content = redis_client.get(InsCrawl.prefix + account['user'])
        if content is not None:
            cookies = pickle.loads(content)
            return cookies
        else:
            InsCrawl._instagram_login(account, redis_client)
            content = redis_client.get(InsCrawl.prefix + account['user'])
            if content is not None:
                cookies = pickle.loads(content)
                return cookies

    @staticmethod
    def get_with_cookies(account, url):
        driver.get(url)
        driver.delete_all_cookies()
        cookies = InsCrawl._get_login_cookies(account, Tools.redis_client)
        if cookies is not None:
            for cookie in cookies:
                if isinstance(cookie.get('expiry'), float):
                    cookie['expiry'] = int(cookie['expiry'])
                driver.add_cookie(cookie)
        if cookies is not None:
            time.sleep(1)
            driver.get(url)
            time.sleep(1)
            return True
        return False

    @staticmethod
    def _instagram_login(account: dict, redis_client: Redis = None) -> bool:
        logger = spider.logger
        logger.info(account)
        try:
            driver.get('https://www.instagram.com/accounts/login/')
            time.sleep(2)
            wait = driver.create_waiter(15)
            phone = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#loginForm > div > div:nth-child(1) > div > label > input')))
            phone.send_keys(account['user'])
            time.sleep(3)
            pwd = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#loginForm > div > div:nth-child(2) > div > label > input')))
            pwd.send_keys(account['pwd'])
            time.sleep(3)
            btn = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#loginForm > div > div:nth-child(3) > button')))
            time.sleep(3)
            action = driver.action_chains()
            action.click(btn).perform()
            time.sleep(3)

            if redis_client is not None:
                InsCrawl._save_redis_cookies(
                    driver.get_cookies(), Tools.redis_client)

            return True
        except Exception as e:
            try:
                driver.get_screenshot_as_file('login.png')
            except:
                ...
            logger.error(e, exc_info=True)
            return False

    @staticmethod
    def crawl_analysisn(item: dict, dm: SaveToSqlPipesline, account):
        row = dict_to_obj(item)
        logger = spider.logger
        logger.info('-' * 20 + 'start instagram' + '-' * 20)

        try:
            InsCrawl.get_with_cookies(account, row.channel_url)
            time.sleep(3)
            logger.info(f'-------url is: {row.channel_url}-------')
            parsed = urlparse(row.channel_url)
            cur_url = driver.current_url
            if re.search(r'/accounts/login', cur_url):
                logger.info('帐号可能被封？')
                raise Exception('account has be verify')

            cur = urlparse(cur_url)
            if parsed.path != cur.path:
                try:
                    driver.get_screenshot_as_file('ins.png')
                except:
                    ...
                raise Exception('触发验证规则？')

            action = driver.action_chains()
            fans = driver.find_element_by_xpath(
                '//*[@id="react-root"]/section/main/div/header/section/ul/li[2]/a/span')
            fans = fans.get_attribute('title').replace(',', '')
            post_list = driver.find_elements_by_xpath(
                '//*[@id="react-root"]/section/main/div/div[3]/article/div[1]/div/div')
            store = []
            adverts_url = row.adverts_url
            comments_count = ''
            ad_parsed = urlparse(adverts_url)

            try:
                for lst in post_list:
                    a = lst.find_elements_by_tag_name('a')
                    for ai in a:
                        tmp_parsed = urlparse(ai.get_attribute('href'))
                        logger.info(f"get url for item is: {tmp_parsed.path}")
                        if ad_parsed.path == tmp_parsed.path:
                            logger.info('url has matched! get this url')
                            action.move_to_element(ai).perform()
                            time.sleep(1)
                            t = ai.find_element_by_css_selector('.qn-0x')
                            comments_count = t.text
                            logger.info('-' * 30)
                            logger.info(f"comments count is: {comments_count}")
                            logger.info('-' * 30)
                            break

            except Exception as e:
                logger.error(e, exc_info=True)
                logger.info('-' * 30)
                logger.info('获取失败 ')
                logger.info('-' * 30)
                try:
                    driver.get_screenshot_as_file('detail_action.png')
                except:
                    ...

            for lst in post_list:
                a = lst.find_elements_by_tag_name('a')
                for ai in a:
                    action.move_to_element(ai).perform()
                    time.sleep(1)
                    t = ai.find_element_by_css_selector('.qn-0x')
                    store.append(t.text)
                    if len(store) == 6:
                        break
                if len(store) == 6:
                    break

            _data = dict()
            if fans:
                fans = InsCrawl.parse_ins_number(fans)
                _data.update(channel_subscription=fans)

            if store:
                logger.info('-' * 50)
                logger.info(store)
                logger.info('-' * 50)
                avg_watch = 0

                _list_like = []
                _list_comment = []
                for i in store:
                    t = re.split(r'\s+', i)
                    if len(t) >= 2:
                        _list_like.append(t[0])
                        _list_comment.append(t[1])

                _list_like = list(map(InsCrawl.parse_ins_number, _list_like))
                if len(_list_like) > 0:
                    avg = reduce(lambda x, y: x + y, _list_like) / \
                        len(_list_like)
                    avg_watch = avg
                    _data.update(channel_avg_watchs=avg)

                _list_comment = list(
                    map(InsCrawl.parse_ins_number, _list_comment))
                if len(_list_comment) > 0:
                    avg_comment = reduce(lambda x, y: x + y,
                                         _list_comment) / len(_list_comment)
                    _data.update(channel_avg_likes=avg_comment)

                    if avg_watch and avg_comment:
                        interact_rate = avg_comment / avg_watch * 100
                        logger.info(
                            f'---ins avg watch {avg_watch} avg comment is {avg_comment} rate {interact_rate}----')
                        _data.update(channel_interact_rate=interact_rate)

            # if len(_data.keys()) > 0:
            #     dm.update_data(row.id, _data)

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
                _data.update(last_update_time=tt.timestamp())

                # dm.update_data(row.id, {'last_update_time': tt.timestamp()})
            except Exception as e:
                logger.info('get first crawl_last_update_time error')
                logger.info(e, exc_info=True)

            # if dm.calculate_time(row):
            driver.get(row.adverts_url)
            if re.search(r'/accounts/login', driver.current_url):
                raise Exception('account has be verify')
            time.sleep(1)
            love = driver.find_element_by_xpath(
                '//*[@id="react-root"]/section/main/div/div[1]/article/div[3]/section[2]/div/div/a')
            love = love.text
            logger.info("love of this is: {} ".format(love))
            time.sleep(1)

            _data = dict(last_crawler_time=time.time())
            if love:
                love = InsCrawl.parse_ins_number(love)
                _data.update(adverts_likes=love, adverts_plays=love)
            if comments_count:
                store = re.split(r'\s+', comments_count)
                comment = InsCrawl.parse_ins_number(store[1])
                _data.update(adverts_comment=comment)

            logger.info('-' * 40)
            logger.info(_data)
            logger.info('-' * 40)
            # dm.update_data(row.id, _data)
            dm.update(Tbl.tbl_marketing_analysisn, ('id', row.id), _data)

        except Exception as e:
            try:
                driver.get_screenshot_as_file('exception_screen.png')
            except:
                ...
            logger.info(e, exc_info=True)

    @staticmethod
    def crawl_mcn(item: dict, dm: SaveToSqlPipesline, account):
        row = dict_to_obj(item)
        logger = spider.logger
        try:
            InsCrawl.get_with_cookies(account, row.resource_url)
            driver.get(row.resource_url)
            cur_url = driver.current_url
            parsed = urlparse(row.resource_url)
            time.sleep(3)
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

            '''
            post_list = browser.find_elements_by_xpath(
                '//*[@id="react-root"]/section/main/div/div[2]/article/div[1]/div/div')
            '''

            # post_list = browser.find_elements_by_css_selector(
            #     '#react-root > section > main > div > div._2z6nI > article > div:nth-child(1) > div')
            post_list = driver.find_elements_by_xpath(
                '//*[@id="react-root"]/section/main/div/div[3]/article/div[1]/div/div')

            store = []
            _data = dict()
            for lst in post_list[:2]:
                a = lst.find_elements_by_tag_name('a')
                # logging.info(a)
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
                fans = InsCrawl.parse_ins_number(fans)
                _data.update(subscription=fans)
                _list = list(map(InsCrawl.parse_ins_number, store))
                if len(_list) > 0:
                    avg = reduce(lambda x, y: x + y, _list) / len(_list)
                    _data.update(avg_watchs=avg)

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
                _data.update(last_update_time=tt.timestamp())
                # dm.update_mcn_task(row.id, {'last_update_time': tt.timestamp()})
            except Exception as e:
                logger.info('get first crawl_last_update_time error')
                logger.info(e, exc_info=True)
            finally:
                dm.update(Tbl.tbl_marketing_resource_expanded_mcn,
                          ('id', row.id), _data, logger)

        except Exception as e:
            logger.error(e, exc_info=True)
            try:
                driver.get_screenshot_as_file('mcn_ins_exc.png')
            except:
                logger.info('截图失败')

    @staticmethod
    def crawl_log(item: dict, dm: SaveToSqlPipesline, account):
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
                InsCrawl.get_with_cookies(account, channel_url)
                time.sleep(2)
                waiter = driver.create_waiter(5)
                fans = waiter.until(lambda d: d.find_element_by_xpath(
                    '//*[@id="react-root"]/section/main/div/header/section/ul/li[2]/a/span'))
                driver.execute_script('document.documentElement.scrollTop=500')

                if InsCrawl.parse_spider_log_from_browser(driver.page_source, row, dm, account, row.channel_url, logger):
                    logger.info('----crawl ins spider is ok ------')
                else:
                    logger.info('-----crawl ins spider is fail------')
            except:
                raise

    @staticmethod
    def crawl_total_resource(item: dict, dm: SaveToSqlPipesline, account):
        row = dict_to_obj(item)
        logger = spider.logger
        try:
            InsCrawl.get_with_cookies(account, row.channel_url)
            time.sleep(2)
            logger.info(
                f'-------row id is: {row.id} url is: {row.channel_url}-------')
            time.sleep(2)
            parsed = urlparse(row.channel_url)
            cur_url = driver.current_url
            if re.search(r'/accounts/login', cur_url):
                logger.info('帐号可能被封？')
                raise Exception('account has be verify')

            cur = urlparse(cur_url)
            if parsed.path != cur.path:
                driver.get_screenshot_as_file('ins.png')
                raise Exception('触发验证规则？')

            action = driver.action_chains()
            fans = driver.find_element_by_xpath(
                '//*[@id="react-root"]/section/main/div/header/section/ul/li[2]/a/span')
            fans = fans.get_attribute('title').replace(',', '')
            post_list = driver.find_elements_by_xpath(
                '//*[@id="react-root"]/section/main/div/div[3]/article/div[1]/div/div')
            store = []

            for lst in post_list[:2]:
                a = lst.find_elements_by_tag_name('a')
                for ai in a:
                    action.move_to_element(ai).perform()
                    time.sleep(1)
                    t = ai.find_element_by_css_selector('.qn-0x')
                    store.append(t.text)
                    if len(store) == 6:
                        break
                if len(store) == 6:
                    break

            _data = dict(last_crawler_time=time.time())
            if fans:
                fans = InsCrawl.parse_ins_number(fans)
                _data.update(channel_subscription=fans)

            if store:
                logger.info('-' * 50)
                logger.info(store)
                logger.info('-' * 50)
                avg_watch = 0

                _list_like = []
                _list_comment = []
                for i in store:
                    t = re.split(r'\s+', i)
                    if len(t) >= 2:
                        _list_like.append(t[0])
                        _list_comment.append(t[1])

                _list_like = list(map(InsCrawl.parse_ins_number, _list_like))
                if len(_list_like) > 0:
                    avg = reduce(lambda x, y: x + y, _list_like) / \
                        len(_list_like)
                    avg_watch = avg
                    _data.update(channel_avg_watchs=avg)

                _list_comment = list(
                    map(InsCrawl.parse_ins_number, _list_comment))
                if len(_list_comment) > 0:
                    avg_comment = reduce(lambda x, y: x + y,
                                         _list_comment) / len(_list_comment)
                    _data.update(channel_avg_likes=avg_comment)

                    if avg_watch and avg_comment:
                        interact_rate = avg_comment / avg_watch * 100
                        logger.info(
                            f'---ins avg watch {avg_watch} avg comment is {avg_comment} rate {interact_rate}----')
                        _data.update(channel_interact_rate=interact_rate)

            # if len(_data.keys()) > 0:
            #     dm.update_total_resource(row.id, _data)

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
                _data.update(last_update_time=tt.timestamp())
                # dm.update_total_resource(
                #     row.id, {'last_update_time': tt.timestamp()})
            except Exception as e:
                logger.info('get first crawl_last_update_time error')
                logger.info(e, exc_info=True)
            finally:
                dm.update(Tbl.tbl_marketing_total_resource,
                          ('id', row.id), _data, logger)

        except Exception as e:
            logger.info(e, exc_info=True)

    @staticmethod
    def crawl_partner(item: dict, dm: SaveToSqlPipesline, account):
        row = dict_to_obj(item)
        logger = spider.logger
        if not row.spider_url:
            return
        logger.info(f'-----------partner url {row.spider_url}---------')
        try:
            InsCrawl.get_with_cookies(account, row.url)
            time.sleep(1)
            parsed = urlparse(row.url)
            cur_url = driver.current_url

            if re.search(r'/accounts/login', cur_url):
                logger.info('帐号可能被封？')
                raise Exception('account has be verify')

            cur = urlparse(cur_url)
            if parsed.path != cur.path:
                try:
                    driver.get_screenshot_as_file('ins.png')
                except Exception as e:
                    logger.info('截图超时')
                raise Exception('触发验证规则？')

            fans = driver.find_element_by_xpath(
                '//*[@id="react-root"]/section/main/div/header/section/ul/li[2]/a/span')
            fans = fans.get_attribute('title').replace(',', '')
            _data = dict(subscribes=fans, last_crawler_time=time.time())
            dm.update(Tbl.tbl_partners, ('id', row.id), _data, logger)
            # dm.update_partners(row.id, _data)
        except Exception as e:
            logger.error(e, exc_info=True)

    @staticmethod
    def crawl_promoter_detail(item: dict, dm: SaveToSqlPipesline, account):
        row = dict_to_obj(item)
        logger = spider.logger
        try:
            InsCrawl.get_with_cookies(account, row.channel_url)
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
                fans = InsCrawl.parse_ins_number(fans)
                _data.update(crawl_fans=fans)
                _list = list(map(InsCrawl.parse_ins_number, store))
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


class InsCrawlerSpider(ChromeSpider):

    name = 'ins'

    allowed_domains = ['www.instagram.com']

    def is_ins(self, url: str) -> bool:
        return bool(re.search(r'instagram', url, re.I))

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
                if self.is_ins(a['channel_url']) and should_crawl(a['last_crawler_time'], a['crawler_mode']):
                    account = get_ins_other_account()
                    yield BrowserRequest(a['channel_url'], self.parse_analysisn, cont_flag=partial(InsCrawl.crawl_analysisn, a, db, account), dont_filter=True)
            if m is not None:
                if self.is_ins(m['resource_url']) and m.get('is_delete', 0) == 0:
                    account = get_ins_other_account()
                    yield BrowserRequest(m['resource_url'], self.parse_mcn, cont_flag=partial(InsCrawl.crawl_mcn, m, db, account), dont_filter=True)
            if r is not None:
                if self.is_ins(r['channel_url']) and should_crawl(a['last_crawler_time'], a['crawler_mode']):
                    account = get_ins_other_account()
                    yield BrowserRequest(r['channel_url'], self.parse_resource, cont_flag=partial(InsCrawl.crawl_total_resource, r, db, account), dont_filter=True)
            if c is not None:
                if self.is_ins(c['channel_url']) and c.get('is_delete', 0) == 0:
                    account = get_ins_spider_account()
                    flag = InsCrawl.request_log(c, db, account, c['channel_url'], self.logger)
                    if not flag:
                        yield BrowserRequest(c['channel_url'], self.parse_log, cont_flag=partial(InsCrawl.crawl_log, c, db, account), dont_filter=True)
            if p is not None:
                if self.is_ins(p['spider_url']):
                    account = get_ins_other_account()
                    yield BrowserRequest(p['spider_url'], self.parse_partner, cont_flag=partial(InsCrawl.crawl_partner, p, db, account), dont_filter=True)
            if d is not None:
                if self.is_ins(d['channel_url']):
                    account = get_ins_other_account()
                    yield BrowserRequest(d['channel_url'], self.parse_detail, cont_flag=partial(InsCrawl.crawl_promoter_detail, d, db, account), dont_filter=True)

    def start_requests(self):
        cursors = self.create_cursors()
        datas = [self.request_from_cursor(cursor, db)
                 for cursor, db in cursors]
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