# 命令解析模块,该模块根据平台负责解析config.cfg， config.windows.cfg, config.linux.cfg 或config.java.cfg 配置文件

import re
import os
import tempfile
import platform
import configparser

from os.path import abspath

from .env import env
from .defaults import (main_script, dft_cfg, vpn_script, DEFAULT_CHROME_SETTINGS, DEFAULT_CRAWL_SETTINGS,
                       DEFAULT_DYNAMIC_SETTINGS, DEFAULT_INFO_SETTINGS, DEFAULT_MAIL_SETTINGS,
                       DEFAULT_MYSQL_SETTINGS, DEFAULT_REDIS_SETTINGS, DEFAULT_VPN_SETTINGS)

def _err_none(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except:
        return 

def _set_dft_config(k, parser):
    try:
        return dict(parser.items(k))
    except:
        if k == 'dbdefault' or k == 'mysql_settings':
            return DEFAULT_MYSQL_SETTINGS
        elif k == 'redis':
            return DEFAULT_REDIS_SETTINGS
        elif k == 'chrome_settings':
            return DEFAULT_CHROME_SETTINGS
        elif k == 'crawl_settings':
            return DEFAULT_CRAWL_SETTINGS
        elif k == 'mail_settings':
            return DEFAULT_MAIL_SETTINGS
        elif k == 'vpn_settings':
            return DEFAULT_VPN_SETTINGS
        elif k == 'dynamic_settings':
            return DEFAULT_DYNAMIC_SETTINGS
        elif k == 'info_settings':
            return DEFAULT_INFO_SETTINGS
        else:
            raise


def _create_g(cfg_path="./config.cfg"):
    parser = configparser.ConfigParser()
    parser.read(cfg_path, encoding='utf8')

    # 解析default db 配置
    key = 'dbdefault'
    mysql_settings = _set_dft_config(key, parser)
    mysql_settings['port'] = int(mysql_settings['port'])
    env.register(key, mysql_settings)

    # 解析redis 配置
    key = 'redis'
    redis_settings = _set_dft_config(key, parser)
    redis_settings['port'] = parser.getint(key, 'port')
    redis_settings['db'] = parser.getint(key, 'db')
    env.register(key, redis_settings)

    # 全局chorme 相关配置
    key = 'chrome_settings'
    df = DEFAULT_CHROME_SETTINGS
    chrome_settings = {}
    chrome_settings['chrome_wait_time'] = _err_none(parser.getint, key, 'chrome_wait_time') or df['chrome_wait_time'] # parser.getint(key, 'chrome_wait_time')
    chrome_settings['page_timeout'] = _err_none(parser.getint, key, 'page_timeout') or df['page_timeout'] #parser.getint(key, 'page_timeout')
    chrome_settings['headless'] = _err_none(parser.getboolean, key, 'headless') or df['headless']  #parser.getboolean(key, 'headless')
    chrome_settings['disable_image'] = _err_none(parser.getboolean, key, 'disable_image') or df['disable_image'] #parser.getboolean(key, 'disable_image')
    chrome_settings['disable_gpu'] = _err_none(parser.getboolean, key, 'disable_gpu') or df['disable_gpu'] #parser.getboolean(key, 'disable_gpu')
    chrome_settings['screen_size'] = _err_none(parser.get, key, 'screen_size') or df['screen_size'] #parser.get(key, 'screen_size')
    chrome_settings['debug'] = _err_none(parser.getboolean, key, 'debug') or df['debug'] #parser.getboolean(key, 'debug')
    chrome_settings['version'] = _err_none(parser.get, key, 'version') or df['version'] #parser.get(key, 'version')
    f = _err_none(parser.get, key, 'executable_path') or df['executable_path']
    if f == 'none' or f is None:
        chrome_settings['executable_path'] = None
    else:
        chrome_settings['executable_path'] = abspath(f) #abspath(parser.get(key, 'executable_path'))

    f = _err_none(parser.get, 'user_data_dir') or df['user_data_dir']
    if f == 'none' or f is None:
        chrome_settings['user_data_dir'] = None
    else:
        chrome_settings['user_data_dir'] = abspath(f) #abspath(parser.get(key, 'user_data_dir'))
    f = _err_none(parser.get, key, 'disk_cache_dir') or df['disk_cache_dir']

    if f == 'none' or f is None:
        chrome_settings['disk_cache_dir'] = None
    else:
        chrome_settings['disk_cache_dir'] = abspath(f) #abspath(parser.get(key, 'disk_cache_dir'))

    f = _err_none(parser.get, key, 'disk_cache_size') or df['disk_cache_size']

    if f == 'none' or f is None:
        chrome_settings['disk_cache_size'] = None
    else:
        chrome_settings['disk_cache_size'] = abspath(f) #abspath(parser.get(key, 'disk_cache_size'))
    env.register(key, chrome_settings)

    # 为每个爬虫配置chrome
    other_keys = parser.keys()
    for k in other_keys:
        if (k.startswith('chrome_settings::')):
            v = dict(parser.items(k))
            env.register(k, v)

    # 解析爬虫 相关配置
    key = 'crawl_settings'
    crawl_settings = _set_dft_config(key, parser)
    crawl_settings['crawl_mode'] = parser.getint(key, 'crawl_mode')
    crawl_settings['crawl_process_count'] = parser.getint(
        key, 'crawl_process_count')
    crawl_settings['crawl_spider_count'] = parser.getint(
        key, 'crawl_spider_count')
    crawl_settings['crawl_time'] = _parse_time_to_seconds(
        parser.get(key, 'crawl_time'))
    crawl_settings['crawl_item'] = parser.getint(key, 'crawl_item')
    crawl_settings['crawl_persist'] = parser.getboolean(key, 'crawl_persist')
    crawl_settings['crawl_flush'] = parser.getboolean(key, 'crawl_flush')
    process = crawl_settings['enable_spiders'].strip().split(',')

    crawl_mode = None
    crawl_process = None
    crawl_count = None
    crawl_time = None
    crawl_item = None

    dft_mode = crawl_settings['crawl_mode']
    dft_process = crawl_settings['crawl_process_count']
    dft_count = crawl_settings['crawl_spider_count']
    dft_time = crawl_settings['crawl_time']
    dft_item = crawl_settings['crawl_item']

    # 'enable-spiders': [[file, crawl_mode, count, crawl_time, crawl_item, crawl_process, start_urls], ...]该字段为每个爬虫设置运行参数
    parse_configs = []
    pattern = '.\-?\d+$'
    for single_settings in process:  # tiktok, crawler::i5||test::m1::p3
        spiders = single_settings.strip().split('||')
        process_settings = []
        start_urls = []
        for spider_settings in spiders:
            parse_args = spider_settings.strip().split('::')
            for arg in parse_args:
                arg = arg.strip()
                flag = re.findall(pattern, arg)
                if arg.startswith('m') and flag:
                    crawl_mode = int(arg[1:])
                elif arg.startswith('c') and flag:
                    crawl_count = int(arg[1:])
                elif arg.startswith('t') and flag:
                    crawl_time = int(arg[1:])
                elif arg.startswith('p') and flag:
                    crawl_process = int(arg[1:])
                elif arg.startswith('i') and flag:
                    crawl_item = int(arg[1:])
                elif arg.startswith('u'):  # uhttps://www.baidu.com
                    if re.findall(r'u\s*?http', arg):
                        urls = str(arg[1:]).strip()
                        ss = urls.split('**')
                        for url in ss:
                            start_urls.append(url.strip())
                else:
                    crawl_file = arg
            cmds = [
                crawl_file, crawl_mode or dft_mode, crawl_count or dft_count,
                crawl_time or dft_time, crawl_item or dft_item, crawl_process
                or dft_process,
                tuple(start_urls)
            ]
            process_settings.append(cmds)
        parse_configs.append(process_settings)

    for process_settings in parse_configs:  # 取管道内最大进程数为基准
        ms = []
        cs = []
        ts = []
        i_s = []
        ps = []
        for sing in process_settings:
            _, m, c, t, i, p, urls = sing
            ms.append(m)
            cs.append(c)
            ts.append(t)
            i_s.append(i)
            ps.append(p)
        for sing in process_settings:
            sing[1] = ms[-1]
            sing[2] = cs[-1]
            sing[3] = ts[-1]
            sing[4] = i_s[-1]
            sing[5] = ps[-1]

    crawl_settings['enable_spiders'] = parse_configs
    env.register(key, crawl_settings)

    # 邮箱参数 设置
    key = 'mail_settings'
    mail_settings = _set_dft_config(key, parser)
    mail_settings['enable'] = parser.getboolean(key, 'enable')
    mail_settings['user'] = parser.get(key, 'user')
    mail_settings['password'] = parser.get(key, 'password')
    mail_settings['host'] = parser.get(key, 'host')
    mail_settings['sends'] = [
        m.strip() for m in parser.get(key, 'sends').split(',')
    ]
    mail_settings['notice_frequency'] = _parse_time_to_seconds(
        parser.get(key, 'notice_frequency'))
    env.register(key, mail_settings)

    # vpn参数
    key = 'vpn_settings'
    vpn_settings = _set_dft_config(key, parser)
    vpn_settings['enable'] = parser.getboolean(key, 'enable')
    vpn_settings['name'] = parser.get(key, 'name')
    vpn_settings['user'] = parser.get(key, 'user')
    vpn_settings['passwd'] = parser.get(key, 'passwd')
    env.register(key, vpn_settings)

    # dynamic 设置
    key = 'dynamic_settings'
    dynamic_settings = _set_dft_config(key, parser)
    dynamic_settings['enable'] = parser.getboolean(key, 'enable')
    dynamic_settings['browser_time'] = _parse_time_to_seconds(
        parser.get(key, 'browser_time'))
    dynamic_settings['ipool_enable'] = parser.getboolean(key, 'ipool_enable')
    env.register(key, dynamic_settings)

    # infosetting 设置
    key = 'info_settings'
    info_settings = _set_dft_config(key, parser)
    info_settings['verbose'] = parser.getboolean(key, 'verbose')
    info_settings['computer'] = parser.get(key, 'computer')
    env.register(key, info_settings)

    keys = parser.keys()
    collect = {}
    for k in keys:
        if k.startswith('db') and k != 'dbdefault':
            v = dict(parser.items(k))
            v['port'] = int(v.get('port'))
            collect[k] = v
    env.register('dbs', collect)

    # other settings
    other_keys = parser.keys()
    not_contains = 'redis chrome_settings mail_settings crawl_settings info_settings dynamic_settings vpn_settings'.split(
        ' ')
    for k in other_keys:
        if (k not in not_contains) and (not k.startswith('chrome_settings')):
            v = dict(parser.items(k))
            env.register(k, v)

    env.has_init = True


# 字符串解析为时间
def _parse_time_to_seconds(input_str: str) -> int:
    '''
    1h2m -> 60 * 60 + 2 * 60
    '''
    res = re.findall(r'(\d+?\s*?)(d|h|m|s)', input_str, re.IGNORECASE)
    s = 0
    for v, t in res:
        if t.lower() == 'd':
            s += int(v) * 24 * 60 * 60
        elif t.lower() == 'h':
            s += int(v) * 60 * 60
        elif t.lower() == 'm':
            s += int(v) * 60
        elif t.lower() == 's':
            s += int(v)
    return int(input_str) if input_str.isdigit() else s


def create_g_from_cfg(cfg_path: str = None):
    suffix = platform.system()  # windows, linux, java
    if cfg_path is None:
        path = os.getcwd()
    else:
        path = cfg_path
    list_dir = os.listdir(path)

    if 'scrapy.cfg' in list_dir:
        ab_path = os.path.abspath('.')
    else:
        ab_path = os.path.abspath('..')

    list_dir = os.listdir(ab_path)
    if f'config.{suffix.lower()}.cfg' in list_dir:
        cfg = os.path.join(ab_path, f'config.{suffix.lower()}.cfg')
    else:
        cfg = os.path.join(ab_path, 'config.cfg')
    cfg = os.path.abspath(cfg)
    if not os.path.exists(cfg):
        # cf1 = os.path.join(path, 'config.windows.cfg')
        # cf2 = os.path.join(path, 'config.linux.cfg')
        with open(cfg, 'w+', encoding='utf8') as fp:
            fp.write(dft_cfg)

    if not os.path.exists(os.path.join(path, 'main.py')):
        with open(os.path.join(path, 'main.py'), 'w+', encoding='utf8') as fp:
            fp.write(main_script)

    _create_g(cfg)

    if env.vpn_settings['enable']:
        vpn_file = os.path.join(tempfile.gettempdir(),
                                'scrapy_selenium_vpn_script.ps1')
        with open(vpn_file, 'w+', encoding='utf8') as fp:
            fp.write(vpn_script % (
                env.vpn_settings['name'], env.vpn_settings['user'], env.vpn_settings['passwd']))
