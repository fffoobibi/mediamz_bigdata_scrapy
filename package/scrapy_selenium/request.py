from scrapy import Request
from selenium.webdriver.common.by import By

import copy


class BrowserRequest(Request):
    def __init__(self,
                 url,
                 callback=None,
                 cont_flag=None,
                 method="GET",
                 headers=None,
                 body=None,
                 cookies=None,
                 meta=None,
                 encoding='utf-8',
                 priority=0,
                 dont_filter=False,
                 errback=None,
                 flags=None,
                 cb_kwargs=None,
                 *,
                 by_selector: str = None,
                 by_xpath: str = None,
                 by_id: str = None,
                 by_name: str = None,
                 by_class: str = None,
                 by_tag: str = None,
                 by_link_text: str = None,
                 find_by: By = None,
                 find_value: str = None,
                 wait_time: int = None,
                 sleep_time: int = None):
        met = copy.deepcopy(meta) or {}
        sele_meta = met.get('selenium') or {}
        self.cont_flag = sele_meta.get('cont_flag') if sele_meta.get('cont_flag') is not None else cont_flag
        self.by_selector = sele_meta.get('by_selector') if sele_meta.get('by_selector') is not None else by_selector
        self.by_xpath = sele_meta.get('by_xpath') if sele_meta.get('by_xpath') is not None else by_xpath
        self.by_id = sele_meta.get('by_id') if sele_meta.get('by_id') is not None else by_id
        self.by_name = sele_meta.get('by_name') if sele_meta.get('by_name') is not None else by_name
        self.by_class = sele_meta.get('by_class') if sele_meta.get('by_class') is not None else by_class
        self.by_tag = sele_meta.get('by_tag') if sele_meta.get('by_tag') is not None else by_tag
        self.by_link = sele_meta.get('by_link_text') if sele_meta.get('by_link_text') is not None else by_link_text
        self.find_by = sele_meta.get('find_by') if sele_meta.get('find_by') is not None else find_by
        self.find_value = sele_meta.get('find_value') if sele_meta.get('find_value') is not None else find_value
        self.wait_time = sele_meta.get('wait_time') if sele_meta.get('wait_time') is not None else wait_time
        self.sleep_time = sele_meta.get('sleep_time') if sele_meta.get('sleep_time') is not None else sleep_time

        selenium_meta = met.setdefault('selenium', {})
        selenium_meta['cont_flag'] = self.cont_flag
        selenium_meta['by_selector'] = self.by_selector
        selenium_meta['by_xpath'] = self.by_xpath
        selenium_meta['by_id'] = self.by_id
        selenium_meta['by_name'] = self.by_name
        selenium_meta['by_class'] = self.by_class
        selenium_meta['by_tag'] = self.by_tag
        selenium_meta['by_link'] = self.by_link
        selenium_meta['find_by'] = self.find_by
        selenium_meta['find_value'] = self.find_value
        selenium_meta['wait_time'] = self.wait_time
        selenium_meta['sleep_time'] = self.sleep_time

        super().__init__(url, callback, meta=met, method=method, body=body, headers=headers, priority=priority,
                         dont_filter=dont_filter, cookies=cookies, encoding=encoding, errback=errback,
                         cb_kwargs=cb_kwargs,
                         flags=flags)

    def __repr__(self):
        return f'Browser<{self.method} {self.url}>'
