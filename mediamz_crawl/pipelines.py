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

    name = g.dbs.get('db_mediamz')

    def create_db(self) -> Optional[adbapi.ConnectionPool]:
        try:
            settings = g.dbs[self.name]
            return adbapi.ConnectionPool('pymysql', **settings)
        except:
            return 

class SaPipeline(SaveToSqlPipesline):

    name= g.dbs.get('db_source')

    def create_db(self) -> Optional[adbapi.ConnectionPool]:
        try:
            settings = g.dbs[self.name]
            return adbapi.ConnectionPool('pymysql', **settings)
        except:
            return 

class MediamzCNPipeline(SaveToSqlPipesline):

    name = g.dbs.get('db_mediamzcn')

    def create_db(self) -> Optional[adbapi.ConnectionPool]:
        try:
            settings = g.dbs[self.name]
            return adbapi.ConnectionPool('pymysql', **settings)
        except:
            return 
