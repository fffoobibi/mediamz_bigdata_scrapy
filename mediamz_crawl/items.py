# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

from scrapy_selenium import SqlItem

class MediamzCrawlItem(SqlItem):
    
    _table_ = 'tbl_marketing_analysisn'

    platform = scrapy.Field()  # 平台

    channel_name = scrapy.Field()  # 博主名

    channel_url = scrapy.Field()  # 渠道url

    channel_subscription = scrapy.Field()  # 渠道订阅量

    channel_avg_watchs = scrapy.Field()  # 渠道均观看量

    channel_interact_rate = scrapy.Field() # 渠道交互率

    creator = scrapy.Field()  # 创建人

    create_time = scrapy.Field()  # 时间戳

    last_crawler_time = scrapy.Field()  # 上次抓取结束时间, 时间戳

    crawler_mode = scrapy.Field()  #  爬虫模式 1.每天早8点抓取 2.每三天抓取 3.每7天抓取 4.每15日抓取 5.每30天抓取

    last_update_time = scrapy.Field() # 视屏最新发布时间

    # adverts_mode = scrapy.Field()  # 广告模式

    # adverts_url = scrapy.Field() # 广告链接

    # crawler_mode = scrapy.Field() # 爬虫模式 1.每天早8点抓取 2.每三天抓取 3.每7天抓取 4.每15日抓取 5.每30天抓取

    # last_crawler_time = scrapy.Field() # 上次抓取时间，每次抓取结束更新时间

    # creator = scrapy.Field() # 创建用户

    # create_time = scrapy.Field() # 创建时间
