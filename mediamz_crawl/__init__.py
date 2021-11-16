import requests

from scrapy_selenium.ippools import IpPoolBase

class ProxyPool(IpPoolBase):

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "zh-CN,zh;q=0.9",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
    }

    url = 'https://www.cloudam.cn/ip/takeip/hg2umQCupAFCD7najoyBH4z5EpkH0Bnc?protocol=proxy&regionid=us&needpwd=false&duplicate=true&amount=1&type=text'

    def _request(self):
        try:
            resp = requests.get(self.url, headers=self.headers)
            if isinstance(resp.text.strip(), bytes):
                return str(resp.text.strip(), encoding='utf8')
            return resp.text.strip()
        except:
            return None