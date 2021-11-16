import re
import time
import json
import redis
import pickle
import random
import logging
import jmespath
import requests
import dateutil.parser

from urllib.parse import urlparse
from functools import reduce
from datetime import datetime, timedelta
from scrapy_selenium.pipelines import SaveToSqlPipesline
from scrapy_selenium import RedisIpPool, g, dict_to_obj, spider, driver

from redis import Redis
from mediamz_crawl.spiders import should_crawl, Tbl, Tools

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

logging.getLogger('selenium').propagate = False
logging.getLogger('urllib3').propagate = False

ip_list = []
with open('staticip.txt') as f:
    for ip in f.readlines():
        ip = ip.strip()
        if ip: 
            ip_list.append(ip)

def should_crawl(last_crawler_time: int, crawler_mode: int) -> bool:
    if not last_crawler_time:
        return True
    last_crawler_time = datetime.fromtimestamp(last_crawler_time)
    if crawler_mode == 1:
        return True
    _map = {2: 3, 3: 7, 4: 15, 5: 30}
    delta_days = 0
    if crawler_mode in _map.keys():
        delta_days = _map[crawler_mode]
    delta = timedelta(days=delta_days)
    now = datetime.now()
    if (now - delta) > last_crawler_time:
        return True
    _next = last_crawler_time + delta
    if _next.strftime("%Y%m%d") == now.strftime("%Y%m%d"):
        return True
    return False

class UrlType:
    ins = 0
    tiktok = 1
    ytb = 2
    unknow = 3


class Tools:
    h = g.redis.get('host', 'localhost')
    p = g.redis.getint('port',  6379)
    db = g.redis.getint('db', 0)
    redis_client = redis.Redis(host=h, port=p, db=db)


class Tbl:
    
    # tables
    tbl_marketing_analysisn = 'tbl_marketing_analysisn'
    tbl_marketing_total_resource = 'tbl_marketing_total_resource'
    tbl_marketing_resource_expanded_mcn = 'tbl_marketing_resource_expanded_mcn'
    tbl_promoter_task_accept_detail = 'tbl_promoter_task_accept_detail'
    tbl_partners = 'tbl_partners'
    tbl_celebrity = 'tbl_celebrity'
    tbl_celebrity_spider_log = 'tbl_celebrity_spider_log'

    # limits
    spider_limit = 3 * 60 * 60 # 3h


class IpPool(RedisIpPool):
    duration = 300
    
    def _request(self, url, **kwargs):
        if url is not None and 'youtube' in url.lower():
            return random.choice(ip_list)
        elif url is not None and 'instagram' in url.lowser():
            account = kwargs.pop('account', None)
            if account is not None:
                return account['ip']

        ip_url = g.ip.get('url')
        try:
            res = requests.get(ip_url).text.strip()
            if res.find('{') > -1:
                return None
            return res
        except:
            return None


class YtbTools:

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


class InsTools:
    
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
    def request_log(item: dict, dm: 'SaveToSqlPipesline', account: dict, url: str, logger, user_data=None):
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

                if data.get('now_avg_watchs', row.now_avg_watchs or 0) > row.now_avg_watchs and _data.get(
                        'now_followers',
                        row.now_followers or 0) > row.now_followers:
                    channel_status = 2
                elif data.get('now_avg_watchs', row.now_avg_watchs or 0) < row.now_avg_watchs and _data.get(
                        'now_followers',
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
                    return InsTools.request_log(row, dm, account, url, logger, user_data)
            return False

    @staticmethod
    def _save_redis_cookies(account, cookies, redis_client: Redis):
        content = pickle.dumps(cookies)
        redis_client.setex(InsTools.prefix + account['user'], 60 * 60 * 3, content)

    @staticmethod
    def _get_login_cookies(account, redis_client: Redis):
        content = redis_client.get(InsTools.prefix + account['user'])
        if content is not None:
            cookies = pickle.loads(content)
            return cookies
        else:
            InsTools._instagram_login(account, redis_client)
            content = redis_client.get(InsTools.prefix + account['user'])
            if content is not None:
                cookies = pickle.loads(content)
                return cookies

    @staticmethod
    def get_with_cookies(account, url):
        driver.get(url)
        driver.delete_all_cookies()
        cookies = InsTools._get_login_cookies(account, Tools.redis_client)
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
                InsTools._save_redis_cookies(
                    driver.get_cookies(), Tools.redis_client)

            return True
        except Exception as e:
            try:
                driver.get_screenshot_as_file('login.png')
            except:
                ...
            logger.error(e, exc_info=True)
            return False


class TiktokTools:

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


class Crawl:
    '''
    具体的抓取过程
    '''

    @staticmethod
    def crawl_promoter_detail(type: int, item: dict, dm: SaveToSqlPipesline, **kw):

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

        funcs = [_ins_promoter_detail, _tiktok_promoter_detail, _ytb_promoter_detail]

        try:
            return funcs[type](item, dm, **kw)
        except:
            raise

    @staticmethod
    def crawl_analysisn(type: int, item: dict, dm: SaveToSqlPipesline, **kw):

        def _ins_crawl_analysisn(item: dict, dm: SaveToSqlPipesline, **kw):
            row = dict_to_obj(item)
            logger = spider.logger
            logger.info('-' * 20 + 'start instagram' + '-' * 20)
            account = kw.pop('account')
            try:
                InsTools.get_with_cookies(account, row.channel_url)
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
                    fans = InsTools.parse_ins_number(fans)
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

                    _list_like = list(map(InsTools.parse_ins_number, _list_like))
                    if len(_list_like) > 0:
                        avg = reduce(lambda x, y: x + y, _list_like) / \
                            len(_list_like)
                        avg_watch = avg
                        _data.update(channel_avg_watchs=avg)

                    _list_comment = list(
                        map(InsTools.parse_ins_number, _list_comment))
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
                    love = InsTools.parse_ins_number(love)
                    _data.update(adverts_likes=love, adverts_plays=love)
                if comments_count:
                    store = re.split(r'\s+', comments_count)
                    comment = InsTools.parse_ins_number(store[1])
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


        def _tiktok_crawl_analysisn(item: dict, dm: SaveToSqlPipesline, **kw):
            row = dict_to_obj(item)
            logger = spider.logger
            channel_url = row.channel_url
            try:
                advert_watch_count = 0
                driver.get(channel_url)
                logger.info(f'--------url is: {channel_url}-----------')
                try:
                    driver.find_element_by_xpath('//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
                except Exception as e:
                    logger.info(e, exc_info=True)
                    logger.info('refresh now!!!')
                    driver.refresh()
                    time.sleep(3)

                fans = driver.find_element_by_xpath('//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[2]/strong')
                like = driver.find_element_by_xpath('//*[@id="main"]/div[2]/div[2]/div/header/h2[1]/div[3]/strong')
                items = driver.find_elements_by_xpath('//*[@id="main"]/div[2]/div[2]/div/main/div[2]/div[1]/div')
                store = []
                for i in items[:6]: store.append(i.text)
                _a_list = []
                for i in items[:6]:
                    a = i.find_element_by_tag_name('a')
                    _a_list.append(a.get_attribute('href'))

                try:
                    a = items[0].find_element_by_tag_name('a')
                    _div = a.find_element_by_tag_name('div')
                    _url = _div.get_attribute('style')
                    logging.info(f'-----style is: {_url}------------------')
                    dt = parse_expire_time(_url)
                    logging.info(f'------parsed url raw dt is: {dt}-----------')
                    if dt:
                        dm.update_data(row.id, {'last_update_time': dt})
                except Exception as e:
                    logging.info(e, exc_info=True)

                '''顺便解析一下当前的广告的数据，通过查找频道页的数据'''
                parsed = urlparse(row.adverts_url)
                logging.info(parsed)
                for i in items:
                    a = i.find_element_by_tag_name('a')
                    href = a.get_attribute('href')
                    tmp = urlparse(href)
                    logging.info(tmp)
                    if tmp.path == parsed.path:
                        logging.info('-' * 30 + 'matched url')
                        try:
                            advert_watch_count = i.find_element_by_tag_name('strong').text
                        except:
                            pass
                        break

                logging.info('-' * 40)
                logging.info("advert watch count is: {}".format(advert_watch_count))
                logging.info('-' * 40)
                avg_watch = 0

                if fans is not None:
                    fans = parse_tiktok_number(fans.text)
                    dm.update_data(row.id, dict(channel_subscription=fans))
                if store is not None:
                    store = store[:6]
                    store = list(map(parse_tiktok_number, store))
                    count = len(store)
                    if count > 0:
                        _sum = reduce(lambda x, y: x + y, store)
                        avg = _sum / count
                        avg_watch = avg
                        dm.update_data(row.id, dict(channel_avg_watchs=avg))
                _like_list = []
                if len(_a_list) > 0:
                    logging.info('找每到个视频的点赞数据')
                    for i in _a_list:
                        driver.get(i)
                        time.sleep(4)
                        like_xpath = '//*[@id="main"]/div[2]/div[2]/div/div/main/div/div[1]/span/div/div[1]/div[5]/div[2]/div[1]/strong'
                        try:
                            _like = driver.find_element_by_xpath(like_xpath).text
                            logging.info(f'like is : {_like}')
                            _like_list.append(_like)
                        except:
                            driver.refresh()
                            logging.info('refreshed.......')
                            time.sleep(3)
                            _like = driver.find_element_by_xpath(like_xpath).text
                            _like_list.append(_like)

                    if len(_like_list) > 0:
                        _like_list = list(map(parse_tiktok_number, _like_list))
                        _avg = reduce(lambda x, y: x + y, _like_list) / len(_like_list)
                        logging.info(f'tiktok avg is : {_avg}')
                        avg_like = _avg
                        _data = dict(channel_avg_likes=_avg)
                        if avg_watch and avg_like:
                            interact_rate = avg_like / avg_watch * 100
                            logging.info(f'-----------login info interact rate is: {interact_rate}----------------')
                            _data.update(channel_interact_rate=interact_rate)
                        # dm.update_data(row.id, _data)

                if dm.calculate_time(row):
                    adverts_url = row.adverts_url
                    driver.get(adverts_url)
                    like_xpath = '//*[@id="main"]/div[2]/div[2]/div/div/main/div/div[1]/span/div/div[1]/div[5]/div[2]/div[1]/strong'
                    reply_xpath = '//*[@id="main"]/div[2]/div[2]/div/div/main/div/div[1]/span/div/div[1]/div[5]/div[2]/div[2]/strong'
                    forward_xpath = '//*[@id="main"]/div[2]/div[2]/div/div/main/div/div[1]/span/div/div[1]/div[5]/div[2]/div[3]/strong'
                    try:
                        _like = driver.find_element_by_xpath(like_xpath).text
                    except:
                        driver.refresh()
                        time.sleep(3)
                    _like = driver.find_element_by_xpath(like_xpath).text
                    _reply =driver.find_element_by_xpath(reply_xpath).text
                    _forward = driver.find_element_by_xpath(forward_xpath).text

                    try:
                        _watch = advert_watch_count != 0 and parse_tiktok_number(str(advert_watch_count)) or 0
                        logger.info(_watch)
                    except Exception as e:
                        logger.error(e, exc_info=True)
                        _watch = 0

                    if _watch == 0:
                        logging.info('视频下架或没有在列表里！')
                        _watch = row.adverts_plays

                    if _like:
                        _like = parse_tiktok_number(_like)
                        _like_rate = _watch != 0 and _like / _watch or 0
                    else:
                        _like = 0
                        _like_rate = 0
                    if _reply:
                        _reply = parse_tiktok_number(_reply)
                        _reply_rate = _watch != 0 and _reply / _watch or 0
                    else:
                        _reply = 0
                        _reply_rate = 0
                    if _forward:
                        _forward = parse_tiktok_number(_forward)
                        _forward_rate = _watch != 0 and _forward / _watch or 0
                    else:
                        _forward = 0
                        _forward_rate = 0
                    last_time = time.time()
                    avg_watch = row.channel_avg_watchs
                    _data = dict(last_crawler_time=last_time, adverts_plays=_watch, adverts_likes=_like,
                                adverts_likes_rate=_like_rate * 100, adverts_comment=_reply,
                                adverts_comment_rate=_reply_rate * 100,
                                adverts_forward=_forward, adverts_forward_rate=_forward_rate * 100,
                                plays_difference=_watch - avg_watch)
                    logger.info('-' * 40)
                    logger.info(_data)
                    logger.info('-' * 40)
                    # dm.update_data(row.id, _data)
                    dm.update(Tbl.tbl_marketing_analysisn, ('id', row.id), _data, logger)
            except Exception as e:
                logger.error(e, exc_info=True)
        
        def _ytb_crawl_analysisn(item: dict,dm:SaveToSqlPipesline,**kw):
            row = dict_to_obj(item)
            logger = spider.logger
            channel_url = row.channel_url
            store = []
            subscribe_count = 0
            try:
                driver.get(channel_url)
                subscriber = driver.find_element_by_css_selector(
                    '#subscriber-count')  # 订阅数量
                subscribe_count = YtbTools.parse_ytb_number(subscriber.text)
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
                        store.append(YtbTools.parse_ytb_number(watch_times))
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
                    _like_list = list(map(YtbTools.parse_ytb_number, _like_list))
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
                _watch = YtbTools.parse_ytb_number(watch.text)
                container = driver.find_element_by_xpath(
                    '//*[@id="menu-container"]')
                like = container.find_element_by_css_selector('#text').text
                _like = YtbTools.parse_ytb_number(like)
                driver.execute_script("window.scrollBy(0,200)")  # 往下滚动
                reply = wait.until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="count"]/yt-formatted-string/span[1]')))
                reply = reply.text
                _reply = YtbTools.parse_ytb_number(reply)
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
                dm.update(Tbl.tbl_marketing_analysisn, ('id', row.id), _data, logger)

            except Exception as e:
                logger.error(e, exc_info=True)

        funcs = [_ins_crawl_analysisn, _tiktok_crawl_analysisn, _ytb_crawl_analysisn]

        try:
            return funcs[type](item, dm, **kw)
        except:
            raise

    @staticmethod
    def crawl_mcn(type: int, item: dict, dm: SaveToSqlPipesline, **kw):
        
        def _ins_crawl_mcn(item: dict, dm: SaveToSqlPipesline, **kw):
            row = dict_to_obj(item)
            logger = spider.logger
            account = kw.pop('account')
            try:
                InsTools.get_with_cookies(account, row.resource_url)
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
                    fans = InsTools.parse_ins_number(fans)
                    _data.update(subscription=fans)
                    _list = list(map(InsTools.parse_ins_number, store))
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

        def _tiktok_crawl_mcn(item: dict, dm: SaveToSqlPipesline, **kw):
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
                fans = TiktokTools.parse_tiktok_number(fans.text)
                store = store[:6]
                store = list(map(TiktokTools.parse_tiktok_number, store))
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
                    dt = TiktokTools.parse_update_time(_url)
                    logger.info(f'------parsed url raw dt is: {dt}-----------')
                    if dt:
                        _data.update(last_update_time=dt)
                except Exception as e:
                    logger.info(e, exc_info=True)
                db.update(Tbl.tbl_marketing_resource_expanded_mcn,
                        ('id', row.id), _data, logger)
            except Exception as e:
                logger.error(e, exc_info=True)
        
        def _ytb_crawl_mcn(item: dict, dm: SaveToSqlPipesline, **kw):
            row = dict_to_obj(item)
            logger = spider.logger
            try:
                driver.get(row.resource_url)
                logger.info(f'----------mcn url: {row.resource_url}---------')
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


        funcs = [_ins_crawl_mcn, _tiktok_crawl_mcn, _ytb_crawl_mcn]

        try:
            return funcs[type](item, dm, **kw)
        except:
            raise

    @staticmethod
    def crawl_total_resource(type: int, item: dict, dm: SaveToSqlPipesline, **kw):

        def _ins_crawl_total_resource(item: dict, dm: SaveToSqlPipesline, **kw):
            row = dict_to_obj(item)
            logger = spider.logger
            account = kw.pop('account')
            try:
                InsTools.get_with_cookies(account, row.channel_url)
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
                    fans = InsTools.parse_ins_number(fans)
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

                    _list_like = list(map(InsTools.parse_ins_number, _list_like))
                    if len(_list_like) > 0:
                        avg = reduce(lambda x, y: x + y, _list_like) / \
                            len(_list_like)
                        avg_watch = avg
                        _data.update(channel_avg_watchs=avg)

                    _list_comment = list(
                        map(InsTools.parse_ins_number, _list_comment))
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


        def _tiktok_crawl_total_resource(item: dict, dm: SaveToSqlPipesline, **kw):
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
                    dt = TiktokTools.parse_update_time(_url)
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
                    fans = TiktokTools.parse_tiktok_number(fans.text)
                    _data.update(channel_subscription=fans)
                    # dm.update_total_resource(row.id, dict(channel_subscription=fans))
                if store is not None:
                    store = store[:6]
                    store = list(map(TiktokTools.parse_tiktok_number, store))
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
                        _like_list = list(map(TiktokTools.parse_tiktok_number, _like_list))
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


        def _ytb_crawl_total_resource(item: dict, dm: SaveToSqlPipesline, **kw):
            row = dict_to_obj(item)
            logger = spider.logger
            channel_url = row.channel_url
            store = []
            try:
                logger.info(
                    f"-------row id is: {row.id} channel url is: {channel_url}-----------")
                driver.get(channel_url)
                subscriber = driver.find_element_by_css_selector(
                    '#subscriber-count')
                subscribe_count = YtbTools.parse_ytb_number(
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
                        store.append(YtbTools.parse_ytb_number(
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
                            map(YtbTools.parse_ytb_number, _like_list))
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
        
        funcs = [_ins_crawl_total_resource, _tiktok_crawl_total_resource, _ytb_crawl_total_resource]

        try:
            return funcs[type](item, dm, **kw)
        except:
            raise

    @staticmethod
    def crawl_partner(type: int, item: dict, dm: SaveToSqlPipesline, **kw):

        def _ins_crawl_partner(item: dict, dm: SaveToSqlPipesline, **kw):
            row = dict_to_obj(item)
            logger = spider.logger
            account = kw.pop('account')
            if not row.spider_url:
                return
            logger.info(f'-----------partner url {row.spider_url}---------')
            try:
                InsTools.get_with_cookies(account, row.url)
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

        def _tiktok_crawl_partner(item: dict, dm: SaveToSqlPipesline, **kw):
            update_table = 'tbl_partners'
            row = dict_to_obj(item)
            logger = spider.logger
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
                fans = TiktokTools.parse_tiktok_number(fans.text)
                _data = dict(subscribes=fans, last_crawler_time=time.time())
                logger.info('-' * 60)
                logger.info(_data)
                logger.info('-' * 60)
                dm.update(update_table, ('id', row.id), _data, logger)
            except Exception as e:
                logger.error(e, exc_info=True)

        def _ytb_crawl_partner(item: dict, dm: SaveToSqlPipesline, **kw):
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
                subscribe = YtbTools.parse_ytb_number(subscribe.text)
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

        funcs = [_ins_crawl_partner, _tiktok_crawl_partner, _ytb_crawl_partner]
        try:
            return funcs[type](item, dm, **kw)
        except:
            raise
    
    @staticmethod
    def crawl_log(type : int, item: dict, dm: SaveToSqlPipesline, **kw):

        def _ins_crawl_log(type, item: dict, dm: SaveToSqlPipesline, **kw):
            row = dict_to_obj(item)
            logger = spider.logger
            account = kw.pop('account')
            logger.info('----- crawl spider log ------')
            t = 0 if row.last_crawler_time is None else row.last_crawler_time
            if (time.time() - t) >= Tbl.spider_limit:
                channel_url = row.channel_url
                store = []
                try:
                    logger.info(
                        f"-------log crawl channel url is: {channel_url}-----------")
                    InsTools.get_with_cookies(account, channel_url)
                    time.sleep(2)
                    waiter = driver.create_waiter(5)
                    fans = waiter.until(lambda d: d.find_element_by_xpath(
                        '//*[@id="react-root"]/section/main/div/header/section/ul/li[2]/a/span'))
                    driver.execute_script('document.documentElement.scrollTop=500')

                    if InsTools.parse_spider_log_from_browser(driver.page_source, row, dm, account, row.channel_url, logger):
                        logger.info('----crawl ins spider is ok ------')
                    else:
                        logger.info('-----crawl ins spider is fail------')
                except:
                    raise

        def _tiktok_crawl_log(type, item: dict, dm: SaveToSqlPipesline, **kw):
            logger = spider.logger
            row = dict_to_obj(item)
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
                        dt = TiktokTools.parse_update_time(_url)
                        logger.info(f'------parsed url raw dt is: {dt}-----------')
                        if dt:
                            _data.update(last_update_time=dt)
                    except:
                        ...
                except Exception as e:
                    logger.info(e, exc_info=True)
                if fans is not None:
                    fans = TiktokTools.parse_tiktok_number(fans.text)
                    _data.update(now_followers=fans)

                channel_status = row.channel_status

                if store is not None:
                    store = store[:6]
                    store = list(map(TiktokTools.parse_tiktok_number, store))
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

        def _ytb_crawl_log(type, item: dict, dm: SaveToSqlPipesline, **kw):
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
                    subscribe_count = YtbTools.parse_ytb_number(
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
                            store.append(YtbTools.parse_ytb_number(
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

        funcs = [_ins_crawl_log, _tiktok_crawl_log, _ytb_crawl_log]
        try:
            return funcs[type](item, dm, **kw)
        except:
            raise

