from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.remote.webelement import WebElement

from scrapy.exceptions import IgnoreRequest

import time
import random
import base64
import platform
import typing as t

from PIL import Image
from io import BytesIO

from .globals import g
from .decorators import lasyproperty

# 定制化Chrome
class SeleniumChrome(Chrome):
    def __init__(
            self,
            executable_path="chromedriver",
            port=0,
            options=None,
            service_args=None,
            desired_capabilities=None,
            service_log_path=None,
            chrome_options=None,
            keep_alive=False,
            headless=False,  # 设置无头模式
            disable_image=True,  # 禁止图片加载
            disable_gpu=True,  # 禁u止gpu加速
            debug=False,  # 调试模式
            screen_size: str = '1920,1080',  # 屏幕尺寸,
            proxy=None,  # 代理设定,'ip:port
            user_data_dir: str = None,
            disk_cache_dir: str = None,
            disk_cache_size: int = None,  # 设置
            page_timeout: int = None  # 页面超时设置
    ):
        if debug == False:
            if chrome_options is None:
                chrome_options = ChromeOptions()
                # 屏蔽webdriver特征方法1,高版本
                chrome_options.add_argument('--disable-blink-features=AutomationControlled')
                # 屏蔽屏蔽自动化受控提示,webdriver version>76
                chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
                chrome_options.add_experimental_option('useAutomationExtension', False)
                # if disable_gpu:
                    # 禁用GPU加速,GPU加速可能会导致Chrome出现黑屏，且CPU占用率高达80%以上
                chrome_options.add_argument('--disable-gpu')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--start-maximized')
                chrome_options.add_argument('--window-size=%s' % screen_size) # 设置屏幕尺寸
                chrome_options.add_argument('--log-level=3') # 日志级别
                # if platform.system() == 'Linux':
                chrome_options.add_argument('--no-sandbox')

                if user_data_dir:
                    chrome_options.add_argument('--user-data-dir=%s' % user_data_dir)
                if disk_cache_dir:
                    chrome_options.add_argument('--disk-cache-dir=%s' % disk_cache_dir)
                if disk_cache_size:
                    chrome_options.add_argument('--disk-cache-size=%s' % disk_cache_size)

                if proxy is not None:
                    chrome_options.add_argument('--proxy-server=http://%s' % proxy)
                if headless:
                    ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
                    chrome_options.add_argument('--headless') # 设置浏览器无头
                    chrome_options.add_argument('user-agent=%s' % ua)
                if disable_image:
                    chrome_options.add_argument('blink-settings=imagesEnabled=false') # 添加屏蔽chrome浏览器禁用图片的设置,高版本

                # chrome_options = ChromeOptions()
                # chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
                # chrome_options.add_experimental_option('useAutomationExtension', False)
                # chrome_options.add_argument("--disable-blink-features=AutomationControlled")
                # # chrome_options.add_argument('--headless')
                # chrome_options.add_argument('--log-level=3')
                # chrome_options.add_argument('--no-sandbox')
                # chrome_options.add_argument('--disable-gpu')
                # chrome_options.add_argument('--disable-dev-shm-usage')
                # chrome_options.add_argument('--start-maximized')
                # chrome_options.add_argument('--window-size=1920,1080')

                # 屏蔽'保存密码'提示框
                prefs = {}
                prefs['credentials_enable_service'] = False
                prefs['profile.password_manager_enabled'] = False
                chrome_options.add_experimental_option('prefs', prefs)
        else:
            chrome_options = ChromeOptions()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--user-agent=%s' % g.ua.chrome)
            chrome_options.add_argument('--window-size=%s' % screen_size)
            chrome_options.add_argument('blink-settings=imagesEnabled=true')

        super().__init__(
            executable_path=executable_path,
            port=port,
            # options=None,
            service_args=service_args,
            desired_capabilities=desired_capabilities,
            service_log_path=service_log_path,
            chrome_options=chrome_options,
            # keep_alive=keep_alive,
        )

        self.start_time = int(time.time())
        if page_timeout is not None:
            self.set_page_load_timeout(page_timeout)
            self.set_script_timeout(page_timeout)

    @property
    def browser_time(self):
        return int(time.time())

    def update_start_time(self):
        self.start_time = int(time.time())

    # 鼠标滚轮滑动
    def scroll_window(self, x: int = 0, y: int = 0) -> None:
        self.execute_script("window.scrollBy(%s, %s)" % (x, y))

    def get_screent_png(self) -> Image.Image:
        screen_shot = self.get_screenshot_as_png()
        img = Image.open(BytesIO(screen_shot))
        return img

    def until(self, css_selector, time_out=5, get_all=False, re_raise=True):
        try:
            bk = self._waiter._timeout
            self._waiter._timeout = time_out
            css = self.find_elements_by_css_selector if get_all else self.find_element_by_css_selector
            return self._waiter.until(lambda d: css(css_selector))
        except TimeoutException:
            if re_raise:
                raise
        finally:
            self._waiter._timeout = bk

    def u(self, css_selector, timeout=10, re_raise=True) -> t.Optional[WebElement]:
        try:
            bk = self._waiter._timeout
            self._waiter._timeout = timeout
            element = self._waiter.until(
                lambda e: self.find_element_by_css_selector(css_selector))
            return element
        except TimeoutException:
            if re_raise:
                raise
        finally:
            self._waiter._timeout = bk

    def us(self, css_selector, timeout=10, re_raise=True) -> t.List[t.Optional[WebElement]]:
        try:
            bk = self._waiter._timeout
            self._waiter._timeout = timeout
            element = self._waiter.until(
                lambda e: self.find_elements_by_css_selector(css_selector))
            return element
        except TimeoutException:
            if re_raise:
                raise
        finally:
            self._waiter._timeout = bk

    @lasyproperty
    def _waiter(self) -> WebDriverWait:
        return WebDriverWait(self, timeout=10)

    def get_element_geo(self, element: WebElement = None,
                        css_selector: str = None,
                        scroll_x=0,
                        scroll_y=0,
                        screen_scaled: float = 1.0) -> t.Tuple[int, int, int, int]:
        if css_selector:
            ele = self.u(css_selector)
        else:
            ele = element
        x1, y1, w1, h1 = (
            ele.location["x"],
            ele.location["y"],
            ele.size["width"],
            ele.size["height"],
        )
        scaled = screen_scaled
        x1 -= scroll_x
        y1 -= scroll_y
        x2 = w1 + x1
        y2 = h1 + y1
        x1, y1, x2, y2 = int(x1 * scaled), int(y1 *
                                               scaled), int(x2 * scaled), int(y2 * scaled)
        return x1, y1, x2-x1, y2-y1

    # 获取元素的截图
    def get_element_image(self,
                          element,
                          scroll_x=0,
                          scroll_y=0,
                          screen_scaled: float = 1.0) -> Image.Image:
        x1, y1, w1, h1 = (
            element.location["x"],
            element.location["y"],
            element.size["width"],
            element.size["height"],
        )
        scaled = screen_scaled
        x1 -= scroll_x
        y1 -= scroll_y
        x2 = w1 + x1
        y2 = h1 + y1
        x1, y1, x2, y2 = x1 * scaled, y1 * scaled, x2 * scaled, y2 * scaled
        screen_shot = self.get_screenshot_as_png()
        img = Image.open(BytesIO(screen_shot))
        return img.crop((x1, y1, x2, y2))

    def get_element_image_b64(self,
                              element,
                              scroll_x=0,
                              scroll_y=0,
                              screen_scaled: float = 1.0) -> bytes:
        image = self.get_element_image(element, scroll_x, scroll_y,
                                       screen_scaled)
        output_buffer = BytesIO()
        image.save(output_buffer, format='PNG')
        binary_data = output_buffer.getvalue()
        base64_data = base64.b64encode(binary_data)
        return base64_data

    def pil_to_base64(self, image: Image.Image):
        output_buffer = BytesIO()
        image.save(output_buffer, format='PNG')
        binary_data = output_buffer.getvalue()
        base64_data = base64.b64encode(binary_data)
        return base64_data

    # 按住并拖动元素
    def move_element(self, element, distance: int):
        tracks = []
        mid = distance * 4 / 5
        current = 0
        t = 0.4
        v = 0
        while current < distance:
            if current < mid:
                a = 2
            else:
                a = -3
            v0 = v
            v = v0 + a * t
            move = v0 * t + 0.5 * a * t * t
            current += move
            tracks.append(round(move))
        ActionChains(self).click_and_hold(element).perform()
        for track in tracks:
            ActionChains(self).move_by_offset(track, 0).perform()
        time.sleep(min(random.random() * 2, 0.914))
        ActionChains(self).release().perform()

    def create_waiter(self, time_out) -> WebDriverWait:
        return WebDriverWait(self, time_out)

    def action_chains(self) -> ActionChains:
        return ActionChains(self)

    def click_element(self, element) -> None:
        ActionChains(self).click(element).perform()

    def remove_element(self,
                       elements=None,
                       *,
                       css_selector: str = None) -> None:
        # element = driver.find_element_by_class_name('classname')
        if css_selector is None:
            self.execute_script(
                """
            var element = arguments[0];
            element.parentNode.removeChild(element);
            """, elements)
        else:
            self.execute_script("""
            var elements = document.querySelectorAll("%s");
            
            for (let element of elements){
                element.parentNode.removeChild(element);
                }
            """ % css_selector)
