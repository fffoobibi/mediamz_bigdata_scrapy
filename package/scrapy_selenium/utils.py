from datetime import datetime
from tempfile import gettempdir
from os.path import abspath, join

from .globals import g

import platform
import subprocess

def time_now(fmt='%Y-%m-%d %H-%M-%S') -> str:
    return datetime.now().strftime(fmt)

def date_now() -> str:
    return datetime.now().strftime("%Y%m%d")

def connect_vpn(spider):
    if g.vpn_settings['enable']:
        try:
            if platform.system() == 'Windows':
                p = subprocess.Popen('ping -n 2 -w 1000 www.tiktok.com', stdout=subprocess.PIPE)
                res = p.stdout.read().decode('gbk')
                if res.find('100% 丢失'):
                    spider.logger.info('连接vpn...')
                    file = abspath(join(gettempdir(), 'scrapy_selenium_vpn_script.ps1'))
                    command = subprocess.Popen('powershell.exe -File %s' % file)
                    command.wait()
        except:
            pass