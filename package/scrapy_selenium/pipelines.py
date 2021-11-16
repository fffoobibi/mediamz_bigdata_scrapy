import logging
from twisted.enterprise import adbapi
from typing import List, Optional, Tuple, Any
from werkzeug.local import LocalProxy, LocalStack

from .items import SqlItem
from .globals import g
from .selenium_spiders import ChromeSpiderBase

Logger = logging.getLoggerClass()

_pipes_ctx_stack = LocalStack()
_pipes_ctx_err_msg = 'Working outside of scrapy context.'


class _PipesContainer:

    def __init__(self):
        self.default: 'SaveToSqlPipesline' = None

    def __getattr__(self, name) -> 'SaveToSqlPipesline':
        return self.__dict__[name]

    def __setattr__(self, name: str, value: 'SaveToSqlPipesline') -> None:
        self.__dict__.setdefault(name, value)

    @staticmethod
    def _find_pipe_line():
        top = _pipes_ctx_stack.top
        if top is None:
            raise RuntimeError(_pipes_ctx_err_msg)
        return top


_pipes_ctx_stack.push(_PipesContainer())

pipe_lines: _PipesContainer = LocalProxy(_PipesContainer._find_pipe_line)


class SaveToSqlPipesline(object):

    name: str = 'default'

    def create_db(self) -> Optional[adbapi.ConnectionPool]:
        try:
            default_settings = g.dbdefault.copy()
            default_settings.pop('enable', None)
            db = adbapi.ConnectionPool('pymysql', **default_settings)
        except:
            db = None
        return db

    def __init__(self, *a, **kw) -> None:
        try:
            name = self.name or self.__class__.__name__
            container = _pipes_ctx_stack.pop()
            if self.__class__.__name__ == 'SaveToSqlPipesline':
                container.default = self
            else:
                setattr(container, name, self)
            self.db_pool = self.create_db()
        finally:
            _pipes_ctx_stack.push(container)

    def close_spider(self, spider):
        if self.db_pool is not None:
            try:
                self.db_pool.close()
            except:
                pass

    def process_item(self, item, spider):
        if isinstance(spider, ChromeSpiderBase):
            spider.crawl_counts += 1
        if isinstance(item, SqlItem):
            if self.db_pool is not None:
                if item._save_:
                    self._save_sql_item(item, spider.logger)
        return item

    def _save_sql_item(self, item: SqlItem, logger: Logger):
        """
        保存 item 至数据库
        """
        save_dict = item.as_dict()
        save_dict.pop('_table_', None)
        save_dict.pop('_save_', None)
        self.insert(item._table_, save_dict, logger)

    def insert_many(self, table: str, values: List[dict], logger: Optional[Logger] = None, ignore: bool = True):
        def _insert_many(cursor, table: str):
            keys = list(values[0].keys())
            len_keys = len(keys)
            ig = 'IGNORE' if ignore else ''
            sql = "INSERT %s INTO %s(%s) VALUES(%s);" % (
                table, ig, ','.join(keys), ','.join(['%s'] * len_keys))
            vs = []
            for v in values:
                vs.append([v[k] for k in keys])
            cursor.executemany(sql, vs)

        if self.db_pool:

            defer = self.db_pool.runInteraction(_insert_many, table)
            if logger is not None:
                defer.addErrback(lambda err: logger.error(
                    'insert many error %s' % err, exc_info=True))

    def insert_update_many(self, table: str, keys: List[str],
                           values: List[Tuple],
                           updates: List[str], ignore: bool = True, logger: Optional[Logger] = None):

        def _do_insert_or_update(cursor, table: str, keys: List[str],
                                 values: List[Tuple], updates: List[str]):
            ig = ' IGNORE ' if ignore else ' '
            sql = "INSERT%sINTO %s(%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s;" % (ig,
                                                                                  table, ','.join(keys), ('%s,' * len(keys)).strip(','), ''.join(
                                                                                      [f'{update}=values({update}), '
                                                                                       for update in updates]).strip(','))
            cursor.executemany(sql, values)

        if self.db_pool:
            defer = self.db_pool.runInteraction(_do_insert_or_update, table,
                                                keys, values, updates)
            if logger is not None:
                defer.addErrback(lambda err: logger.error(
                    'insert many error %s' % err, exc_info=True))

    def update(self, table: str, by: tuple = None, item: dict = None, logger: Optional[Logger] = None, **fields):
        """
        更新数据
        """
        def _adb_update(cursor, sql, value):
            cursor.execute(sql, value)

        if self.db_pool:
            if item is not None:
                item.pop('_table_', None)
                item.pop('_save_', None)
                vals = ','.join([f"{key}=%s"for key in item])
                value = [item[key] for key in item]
            else:
                vals = ','.join([f"{key}=%s"for key in fields])
                value = [fields[key] for key in fields]

            sql = f'UPDATE {table} SET {vals} WHERE {by[0]}={by[1]};'
            defer = self.db_pool.runInteraction(_adb_update, sql, value)
            if logger is not None:
                defer.addErrback(
                    lambda err: logger.error('update error %s' % err, exc_info=True))

    def insert(self, table: str, item: dict = None, logger: Optional[Logger] = None, ignore: bool = True, **fields):
        """
        插入数据
        """
        def _adb_insert(cursor, sql, value):
            cursor.execute(sql, value)

        if self.db_pool:

            if item is not None:
                item.pop('_table_', None)
                item.pop('_save_', None)
                keys = ','.join(item.keys())
                vals = ','.join(['%s'] * len(item.keys()))
                value = [item[key] for key in item]
            else:
                keys = ','.join(fields.keys())
                vals = ','.join(['%s'] * len(fields.keys()))
                value = [fields[key] for key in fields]
            ig = ' IGNORE ' if ignore else ' '
            sql = f'INSERT{ig}INTO {table}({keys}) VALUES({vals});'
            defer = self.db_pool.runInteraction(_adb_insert, sql, value)
            if logger is not None:
                defer.addErrback(lambda err: logger.error(
                    'insert error %s' % err, exc_info=True))

    def delete(self, table: str, where: str = None, logger: Optional[Logger] = None):
        """
        where: str
        example:
            delete('test', 'id=3')
        """
        def _delete_(cursor, sql):
            cursor.execute(sql)

        if self.db_pool:

            w = f'where {where}' if where is not None else ''
            sql = f'DELETE FROM {table} {w}'.strip()
            defer = self.db_pool.runInteraction(_delete_, sql)
            if logger is not None:
                defer.addErrback(lambda err: logger.error(
                    'delete error %s' % err, exc_info=True))
