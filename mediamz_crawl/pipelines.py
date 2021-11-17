# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from typing import Optional
from twisted.enterprise import adbapi

from scrapy_selenium import g
from scrapy_selenium.pipelines import SaveToSqlPipesline


class MediamzPipeline(SaveToSqlPipesline):

    name = g.db_mediamz.get('db')

    def create_db(self) -> Optional[adbapi.ConnectionPool]:
        try:
            settings = g.db_mediamz.copy()
            settings['port'] = g.db_mediamz.getint('port')
            return adbapi.ConnectionPool('pymysql', **settings)
        except Exception as e:
            return 

class SaPipeline(SaveToSqlPipesline):

    name= g.db_source.get('db')

    def create_db(self) -> Optional[adbapi.ConnectionPool]:
        try:
            settings = g.db_source.copy()
            settings['port'] = g.db_source.getint('port')
            return adbapi.ConnectionPool('pymysql', **settings)
        except:
            return 

class MediamzCNPipeline(SaveToSqlPipesline):

    name = g.db_mediamzcn.get('db')

    def create_db(self) -> Optional[adbapi.ConnectionPool]:
        try:
            settings = g.db_mediamzcn.copy()
            settings['port'] = g.db_mediamzcn.getint('port')
            return adbapi.ConnectionPool('pymysql', **settings)
        except:
            return 
