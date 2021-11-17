import time
import shutil
import logging
import platform
import subprocess

from copy import deepcopy
from functools import partial
from collections import namedtuple

from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.exceptions import IgnoreRequest
from werkzeug.local import LocalStack, LocalProxy

from .globals import g
from .utils import time_now
from .ippools import IpPoolBase
from .request import BrowserRequest
from .webdriver import SeleniumChrome
from .decorators import trigger, Trigger
from .exceptions import TimeoutException
from .selenium_spiders import ChromeSpiderBase


from selenium.webdriver.support.wait import WebDriverWait

__all__ = ('spider', 'request', 'driver', 'waiter', 'middle')

Info = namedtuple('Info', 'spider request driver waiter middle')

_middle_ctx_err_msg = 'Working outside of BrowserDownloadMiddle context.'
_middle_ctx_stack = LocalStack()

_Logger = logging.getLoggerClass()


def _find_obj(name):
    top = _middle_ctx_stack.top
    if top is None:
        raise RuntimeError(_middle_ctx_err_msg)
    return getattr(top, name)


spider: ChromeSpiderBase = LocalProxy(partial(_find_obj, 'spider'))
request: BrowserRequest = LocalProxy(partial(_find_obj, 'request'))
driver: SeleniumChrome = LocalProxy(partial(_find_obj, 'driver'))
waiter: WebDriverWait = LocalProxy(partial(_find_obj, 'waiter'))
middle: 'BrowserDownloadMiddle' = LocalProxy(partial(_find_obj, 'middle'))


def make_response(url: str = None, body: str = None, req: 'Request' = None, status: int = None, encoding: str = None) -> HtmlResponse:
    u = request.url if url is None else url
    b = driver.page_source if body is None else body
    r = request._get_current_object() if req is None else req
    s = 200 if status is None else status
    e = 'utf8' if encoding is None else encoding
    return HtmlResponse(url=u, body=b, request=r, status=s, encoding=e)


def restart_browser(msg):
    middle.restart_browser(spider, msg, True)
    # if g.info_settings.get('verbose', False):
    #     print(f'[{spider.run_info} {time_now()}]: restart 触发器执行')


def open_new_tag(msg):
    driver.execute_script('window.open("")')
    driver.switch_to.window(driver.window_handles[0])
    driver.close()
    time.sleep(0.1)
    driver.switch_to.window(driver.window_handles[0])


@trigger(frequencys=2,
         calls=restart_browser,
         events='restart',
         raise_as=IgnoreRequest)
def middle_restart_get(self, url, time_out=5):
    try:
        self.set_page_load_timeout(time_out)
        self.set_script_timeout(time_out)
        self.get(url)
    except TimeoutException:
        self.execute_script('window.stop()')
        raise Trigger('restart', msg="页面超时, 重启浏览器")


@trigger(1, calls=open_new_tag, events='newTag', raise_as=IgnoreRequest)
def middle_tag_get(self, url, time_out=5):
    try:
        self.set_page_load_timeout(time_out)
        self.set_script_timeout(time_out)
        self.get(url)
    except TimeoutException:
        self.execute_script('window.stop()')
        raise Trigger('newTag', msg="页面超时, 新建标签")


SeleniumChrome.middle_restart_get = middle_restart_get
SeleniumChrome.middle_tag_get = middle_tag_get


class BrowserDownloadMiddle:

    def spider_opened(self, spider):
        spider.set_middle(self)
        spider.logger.info('Spider opened: %s' % spider.name)

    def spider_closed(self, reason):
        try:
            print('browser quit by %s' % reason)
            self.driver.quit()
        except Exception as e:
            print(e)

    @classmethod
    def from_crawler(cls, crawler):
        middle = cls()
        crawler.signals.connect(middle.spider_opened,
                                signal=signals.spider_opened)
        crawler.signals.connect(middle.spider_closed,
                                signal=signals.spider_closed)
        return middle

    @property
    def dont_open(self):
        return self._dont_open

    @dont_open.setter
    def dont_open(self, v: bool):
        if v:
            try:
                self.driver.quit()
            except:
                ...
            finally:
                self.driver = None
                self.waiter = None
        self._dont_open = v

    def __init__(self) -> None:
        settings = deepcopy(g.chrome_settings)
        settings.pop('version', None)

        self._dont_open = False
        self.browser_settings = settings  # 浏览器配置参数
        self.wait = settings.pop('chrome_wait_time')  # 浏览等待时间

        self.ip_enable = g.dynamic_settings.get(
            'ipool_enable', False)  # 是否启用代理ip

        # 是否启用浏览器定时重启
        self.browser_restart_enabled = g.dynamic_settings.get('enable', False)
        self.browser_exist_time = g.dynamic_settings.get(
            'browser_time', 0)  # 浏览器使用时间

        # chrome浏览器初始化
        # if not (self.browser_restart_enabled and self.browser_exist_time == 0):
        #     self.driver = SeleniumChrome(**self.browser_settings)
        #     self.waiter = self.driver.create_waiter(self.wait)
        #     self.driver.delete_all_cookies()

        self.driver = None
        self.waiter = None

        # ip池
        self.ip_pool = IpPoolBase.getClass(
            g.dynamic_settings.get('ipool_class', 'IpPoolBase'))()

    def _create_browser(self, use_ip=False, request_url=None):
        proxy_ip = self.ip_pool.random(request_url) if use_ip else None
        self.driver = SeleniumChrome(
            proxy=proxy_ip, **self.browser_settings)
        self.waiter = self.driver.create_waiter(self.wait)
        self.driver.update_start_time()

    def restart_browser(self, spider, msg='', use_ip=False, url=None):
        try:
            try:
                self.driver.quit()
                path = self.browser_settings.get('user_data_dir', '')
                if path:
                    try:
                        shutil.rmtree(path)
                    except:
                        ...
            except:
                ...
            # del self.driver
            # del self.waiter
            # region
            # try:
            #     if platform.system() != 'Windows':
            #         code = subprocess.Popen('killall -9 chrome', shell=True)
            #         # code = subprocess.Popen('killall -9 %s' % g.chrome_settings['executable_path'], shell=True)
            #         code.wait()
            # except Exception as e:
            #     print(f'clean error {e}')
            # endregion

            if g.info_settings.get('verbose', False):
                print(
                    f'\033[1;31m[{spider.run_info} {time_now()}] [{msg}]: restart\033[0m')
            self._create_browser(use_ip, url)

        except Exception as e:
            spider.logger.error(f'{msg}{spider.run_info} restart fail {e}')
            if g.info_settings.get('verbose', False):
                print(
                    f"\033[1;31m[{msg}{spider.run_info} {time_now()}]: restart fail {e} \033[0m")

    # 请求中间，逻辑委托到ChromeEnu对象
    def process_request(self, request, spider):
        '''
        下载中间件,处理request请求,使用selenium,返回None, response或者抛出ignoreRequest
        '''
        try:
            if self.driver is None:
                self._create_browser(use_ip=True, request_url=request.url)
                
            _middle_ctx_stack.push(
                Info(spider, request, self.driver, self.waiter, self))

            if not isinstance(request, BrowserRequest):
                return None

            if self._dont_open:
                return None

            if self.browser_need_restart(self.ip_enable, request.url):  # 重启浏览器
                self.restart_browser(spider, self.ip_enable, request.url)

            return self._crawl_from_browser(request, spider)

        except IgnoreRequest:
            spider.logger.info(
                f'{spider.run_info}: ignore request', exc_info=True)
            raise
        except:
            spider.logger.error(f'crawler error', exc_info=True)
            raise IgnoreRequest
        finally:
            _middle_ctx_stack.pop()

    def browser_need_restart(self, use_ip, url):
        '''
        判断浏览器是否需要重启
        '''
        if self.driver is None:
            self._create_browser(use_ip, url)
        if self.browser_restart_enabled:
            if (self.driver.browser_time -
                    self.driver.start_time) >= self.browser_exist_time:
                return True
        return False

    def _crawl_from_browser(self, request, spider):
        if request.meta.get('selenium', None):
            if request.meta['selenium'].get('cont_flag', None) is not None:
                resp = request.meta['selenium']['cont_flag']()
                if isinstance(resp, str):
                    return make_response(body=resp, req=request)
                elif isinstance(resp, HtmlResponse):
                    return resp
                elif isinstance(resp, (tuple, list)):
                    values = resp[:2]
                    return make_response(body=values[0], status=values[1], req=request)
                else:
                    return resp
            else:
                return self._execute_from_browser_request(request, spider, self.driver,
                                                          self.waiter)

    def _execute_from_browser_request(self, request, spider, driver, waiter):
        driver.get(request.url)
        selenium_meta = request.meta.get('selenium')
        wait_time = selenium_meta['wait_time']
        by_selector = selenium_meta['by_selector']
        by_xpath = selenium_meta['by_xpath']
        by_id = selenium_meta['by_id']
        by_name = selenium_meta['by_name']
        by_class = selenium_meta['by_class']
        by_tag = selenium_meta['by_tag']
        by_link = selenium_meta['by_link']
        find_by = selenium_meta['find_by']
        find_value = selenium_meta['find_value']
        sleep_time = selenium_meta['sleep_time']
        waiter = WebDriverWait(driver, wait_time) if wait_time else waiter
        if by_selector:
            waiter.until(
                lambda d: driver.find_element_by_css_selector(by_selector))
        elif by_xpath:
            waiter.until(lambda d: driver.find_element_by_x_path(by_xpath))
        elif by_id:
            waiter.until(lambda d: driver.find_element_by_id(by_id))
        elif by_name:
            waiter.until(lambda d: driver.find_element_by_name(by_name))
        elif by_class:
            waiter.until(lambda d: driver.find_element_by_class_name(by_class))
        elif by_tag:
            waiter.until(lambda d: driver.find_element_by_tag(by_tag))
        elif by_link:
            waiter.until(lambda d: driver.find_element_by_link_text(by_link))
        elif find_by and find_value:
            waiter.until(lambda d: driver.find_element(find_by, find_value))
        elif sleep_time:
            time.sleep(sleep_time)
        return HtmlResponse(url=driver.current_url,
                            body=driver.page_source,
                            request=request,
                            status=200,
                            encoding='utf-8')
