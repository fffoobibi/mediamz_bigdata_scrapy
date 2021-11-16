# from typing import Callable
# import pymysql
# db = pymysql.connect(host='localhost', user='root',
#                      password='qwer123456', database='mediamz_sim', port=3306)


# def lzip(*iterables, convert: Callable=None):

#     def _all_done(lis):
#         flag = 0
#         l = len(lis)
#         for f, v in lis:
#             if isinstance(f, StopIteration):
#                 flag += 1
#         return flag == l

#     iters = [iter(i) for i in iterables]

#     while True:
#         iter_v = []
#         for i in iters:
#             try:
#                 iter_v.append((None, next(i)))
#             except StopIteration as e:
#                 iter_v.append((e, None))

#         if _all_done(iter_v):
#             break
#         else:
#             ret = []
#             for f ,v in iter_v:
#                 if convert is None:
#                     ret.append(v)
#                 else:
#                     ret.append(convert(v))
#             yield ret


# cursor = db.cursor(pymysql.cursors.SSDictCursor)


# def get_table(table):
#     cursor.execute(f'select * from {table}')
#     return cursor.fetchall()

# from itertools import zip_longest

# z = zip_longest(get_table('tbl_user'), get_table('tbl_task'))

# for r1, r2 in z:
#     print('r1====> ', r1)
#     print('r2====> ', r2)
#     print('-' * 50)

# from scrapy_selenium import g
# print(g)

# import logging
# from logging.handlers import TimedRotatingFileHandler

# th = TimedRotatingFileHandler('./test.log', when='S', backupCount=3, encoding='utf8')
# th.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
# th.setLevel(logging.INFO)
# logger = logging.getLogger('test')
# logger.addHandler(th)
# logger.setLevel(logging.INFO)
# logger.info('test')

# from itertools import zip_longest


# def a(x, y):
#     for i in range(x, y):
#         if i % 2 == 0:
#             yield f'{i} %'
#         yield f'{i} end'

# def b(x, y):
#     for i in range(x, y):
#         yield f'{i} bb'

# for t in zip_longest(a(1, 4), a(5, 10), b(9,15)):
#     print(t)



# print(test())
# import redis
# s = redis.Redis('localhost', 6379, encoding='utf8')
# s.setex('test', 10, 'sdfsdf')
# print(s.keys('*'))
# print(s.get('test').decode())

# https://www.cloudam.cn/ip/whitelist/add/ogL3zFJRN4XYhlD6BlqAKOotOC0HkxDf?protocol=proxy&needpwd=false&userip=10.50.0.71


from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.chrome import options
p = r'C:\Users\admin\Desktop\newproject\chromedriver_95.4638.exe'

options = ChromeOptions()
options.add_argument()

b = Chrome(executable_path=p, options=options)

b.get('https://www.tiktok.com/@istrepy?')