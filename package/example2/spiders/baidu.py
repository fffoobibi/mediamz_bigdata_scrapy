
from scrapy_selenium import BrowserRequest, ChromeSpider
from scrapy_selenium.globals import middle, spider, driver, waiter, request
from scrapy.http import HtmlResponse

class BaiduSpider(ChromeSpider):

    name='baidu'
    
    def start_requests(self):
        url = 'https://www.baidu.com'
        yield BrowserRequest(url, cont_flag=self.brower_test)

    def parse(self, resp: HtmlResponse):
        print('=======> 百度' , resp.meta, spider)

    def brower_test(self):
        driver.get(request.url)
        request.meta['gg'] = 20
        print('meta ====> ', type(request.meta))
        from copy import deepcopy
        return self.browser_request('https://www.jianshu.com', self.again, wait_time=2)

    def again(self, resp):
        print('======> ', spider)


