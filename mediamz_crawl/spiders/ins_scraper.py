import os
import re
import sys
import codecs
import time
import json
from typing import Optional
import redis
import pickle
import logging
import requests
import configparser

from requests.sessions import RequestsCookieJar

BASE_URL = 'https://www.instagram.com/'
LOGIN_URL = BASE_URL + 'accounts/login/ajax/'
LOGOUT_URL = BASE_URL + 'accounts/logout/'
CHROME_WIN_UA = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
USER_URL = BASE_URL + '{0}/?__a=1'
USER_INFO = 'https://i.instagram.com/api/v1/users/{0}/info/'
STORIES_UA = 'Instagram 123.0.0.21.114 (iPhone; CPU iPhone OS 11_4 like Mac OS X; en_US; en-US; scale=2.00; 750x1334) AppleWebKit/605.1.15'

MAX_CONCURRENT_DOWNLOADS = 5
CONNECT_TIMEOUT = 90
MAX_RETRIES = 5
RETRY_DELAY = 5
MAX_RETRY_DELAY = 60

LATEST_STAMPS_USER_SECTION = 'users'


class PartialContentException(Exception):
    pass


class InstagramScraper(object):

    def __init__(self, **kwargs):
        default_attr = dict(username='', usernames=[], filename=None,
                            login_user=None, login_pass=None,
                            followings_input=False, followings_output='profiles.txt',
                            destination='./', logger=None, retain_username=False, interactive=False,
                            quiet=False, maximum=0, media_metadata=False, profile_metadata=False, latest=False,
                            latest_stamps=False, cookiejar=None, filter_location=None, filter_locations=None,
                            media_types=['image', 'video',
                                         'story-image', 'story-video'],
                            tag=False, location=False, search_location=False, comments=False,
                            verbose=0, include_location=False, filter=None, proxies={}, no_check_certificate=False,
                            template='{urlname}', log_destination='')

        allowed_attr = list(default_attr.keys())
        default_attr.update(kwargs)

        for key in default_attr:
            if key in allowed_attr:
                self.__dict__[key] = default_attr.get(key)

        # story media type means story-image & story-video
        if 'story' in self.media_types:
            self.media_types.remove('story')
            if 'story-image' not in self.media_types:
                self.media_types.append('story-image')
            if 'story-video' not in self.media_types:
                self.media_types.append('story-video')

        # Read latest_stamps file with ConfigParser
        self.latest_stamps_parser = None
        if self.latest_stamps:
            parser = configparser.ConfigParser()
            parser.read(self.latest_stamps)
            self.latest_stamps_parser = parser
            # If we have a latest_stamps file, latest must be true as it's the common flag
            self.latest = True

        # Set up a logger
        if self.logger is None:
            self.logger = InstagramScraper.get_logger(level=logging.DEBUG, dest=default_attr.get(
                'log_destination'), verbose=default_attr.get('verbose'))

        self.posts = []
        self.stories = []

        self.session = requests.Session()
        if self.no_check_certificate:
            self.session.verify = False

        try:
            if self.proxies and type(self.proxies) == str:
                self.session.proxies = self._get_json(self.proxies)
        except ValueError:
            self.logger.error("Check is valid json type.")
            raise

        self.session.headers = {'user-agent': CHROME_WIN_UA}
        if self.cookiejar and os.path.exists(self.cookiejar):
            with open(self.cookiejar, 'rb') as f:
                self.session.cookies.update(pickle.load(f))
        self.session.cookies.set('ig_pr', '1')
        self.rhx_gis = ""

        self.cookies = None
        self.authenticated = False
        self.logged_in = False
        self.last_scraped_filemtime = 0
        self.initial_scraped_filemtime = 0
        if default_attr['filter']:
            self.filter = list(self.filter)
        self.quit = False

    def _get_json(self, text):
        try:
            return json.loads(text)
        except json.JSONDecodeError as error:
            self.logger.error('Text is not json: ' + text)
            raise

    def sleep(self, secs):
        min_delay = 1
        for _ in range(secs // min_delay):
            time.sleep(min_delay)
            if self.quit:
                return

        time.sleep(secs % min_delay)

    def _retry_prompt(self, url, exception_message):
        """Show prompt and return True: retry, False: ignore, None: abort"""
        answer = input('Repeated error {0}\n(A)bort, (I)gnore, (R)etry or retry (F)orever?'.format(
            exception_message))
        if answer:
            answer = answer[0].upper()
            if answer == 'I':
                self.logger.info(
                    'The user has chosen to ignore {0}'.format(url))
                return False
            elif answer == 'R':
                return True
            elif answer == 'F':
                self.logger.info('The user has chosen to retry forever')
                global MAX_RETRIES
                MAX_RETRIES = sys.maxsize
                return True
            else:
                self.logger.info('The user has chosen to abort')
                return None

    def safe_get(self, *args, **kwargs):
        retry = 0
        retry_delay = RETRY_DELAY
        while True:
            if self.quit:
                return
            try:
                response = self.session.get(
                    timeout=CONNECT_TIMEOUT, cookies=self.cookies, *args, **kwargs)
                if response.status_code == 404:
                    return
                response.raise_for_status()
                content_length = response.headers.get('Content-Length')
                if content_length is not None and len(response.content) != int(content_length):
                    # if content_length is None we repeat anyway to get size and be confident
                    raise PartialContentException('Partial response')
                return response
            except (KeyboardInterrupt):
                raise
            except (requests.exceptions.RequestException, PartialContentException) as e:
                if 'url' in kwargs:
                    url = kwargs['url']
                elif len(args) > 0:
                    url = args[0]
                if retry < MAX_RETRIES:
                    self.logger.warning(
                        'Retry after exception {0} on {1}'.format(repr(e), url))
                    self.sleep(retry_delay)
                    retry_delay = min(2 * retry_delay, MAX_RETRY_DELAY)
                    retry = retry + 1
                    continue
                else:
                    keep_trying = self._retry_prompt(url, repr(e))
                    if keep_trying == True:
                        retry = 0
                        continue
                    elif keep_trying == False:
                        return
                raise

    def get_json(self, *args, **kwargs):
        resp = self.safe_get(*args, **kwargs)
        if resp is not None:
            return resp.text

    def authenticate_as_guest(self):

        self.session.headers.update(
            {'Referer': BASE_URL, 'user-agent': STORIES_UA})
        req = self.session.get(BASE_URL)

        self.session.headers.update({'X-CSRFToken': req.cookies['csrftoken']})

        self.session.headers.update({'user-agent': CHROME_WIN_UA})
        self.rhx_gis = ""
        self.authenticated = True

    def authenticate_with_login(self):
        self.session.headers.update(
            {'Referer': BASE_URL, 'user-agent': STORIES_UA})
        req = self.session.get(BASE_URL)

        self.session.headers.update({'X-CSRFToken': req.cookies['csrftoken']})

        login_data = {'username': self.login_user, 'password': self.login_pass}
        login = self.session.post(
            LOGIN_URL, data=login_data, allow_redirects=True)
        self.session.headers.update(
            {'X-CSRFToken': login.cookies['csrftoken']})
        self.cookies = login.cookies
        login_text = self._get_json(login.text)
        if login_text.get('authenticated') and login.status_code == 200:
            self.authenticated = True
            self.logged_in = True
            self.session.headers.update({'user-agent': CHROME_WIN_UA})
            self.rhx_gis = ""
            return True
        else:
            self.logger.error('Login failed for ' + self.login_user)
            logging.error('Login failed for ' + self.login_user)
            if 'checkpoint_url' in login_text:
                checkpoint_url = login_text.get('checkpoint_url')
                self.logger.error(
                    'Please verify your account at ' + BASE_URL[0:-1] + checkpoint_url)
                logging.error('Please verify your account at ' + BASE_URL[0:-1] + checkpoint_url)
                if self.interactive is True:
                    self.login_challenge(checkpoint_url)
            elif 'errors' in login_text:
                for count, error in enumerate(login_text['errors'].get('error')):
                    count += 1
                    self.logger.debug(
                        'Session error %(count)s: "%(error)s"' % locals())
            else:
                self.logger.error(json.dumps(login_text))
                logging.error(json.dumps(login_text))
            return False
            sys.exit(1)

    def login_challenge(self, checkpoint_url):
        self.session.headers.update({'Referer': BASE_URL})
        req = self.session.get(BASE_URL[:-1] + checkpoint_url)
        self.session.headers.update(
            {'X-CSRFToken': req.cookies['csrftoken'], 'X-Instagram-AJAX': '1'})

        self.session.headers.update(
            {'Referer': BASE_URL[:-1] + checkpoint_url})
        mode = int(input('Choose a challenge mode (0 - SMS, 1 - Email): '))
        challenge_data = {'choice': mode}
        challenge = self.session.post(
            BASE_URL[:-1] + checkpoint_url, data=challenge_data, allow_redirects=True)
        self.session.headers.update(
            {'X-CSRFToken': challenge.cookies['csrftoken'], 'X-Instagram-AJAX': '1'})

        code = int(input('Enter code received: '))
        code_data = {'security_code': code}
        code = self.session.post(
            BASE_URL[:-1] + checkpoint_url, data=code_data, allow_redirects=True)
        self.session.headers.update({'X-CSRFToken': code.cookies['csrftoken']})
        self.cookies = code.cookies
        code_text = self._get_json(code.text)

        if code_text.get('status') == 'ok':
            self.authenticated = True
            self.logged_in = True
        elif 'errors' in code.text:
            for count, error in enumerate(code_text['challenge']['errors']):
                count += 1
                self.logger.error(
                    'Session error %(count)s: "%(error)s"' % locals())
        else:
            self.logger.error(json.dumps(code_text))

    def logout(self):
        """Logs out of instagram."""
        if self.logged_in:
            try:
                logout_data = {
                    'csrfmiddlewaretoken': self.cookies['csrftoken']}
                self.session.post(LOGOUT_URL, data=logout_data)
                self.authenticated = False
                self.logged_in = False
            except requests.exceptions.RequestException:
                self.logger.warning('Failed to log out ' + self.login_user)

    def get_profile_info(self, dst, username):
        if self.profile_metadata is False:
            return
        url = USER_URL.format(username)
        resp = self.get_json(url)

        if resp is None:
            self.logger.error(
                'Error getting user info for {0}'.format(username))
            return

        self.logger.info(
            'Saving metadata general information on {0}.json'.format(username))

        user_info = self._get_json(resp)['graphql']['user']

        try:
            profile_info = {
                'biography': user_info['biography'],
                'followers_count': user_info['edge_followed_by']['count'],
                'following_count': user_info['edge_follow']['count'],
                'full_name': user_info['full_name'],
                'id': user_info['id'],
                'is_business_account': user_info['is_business_account'],
                'is_joined_recently': user_info['is_joined_recently'],
                'is_private': user_info['is_private'],
                'posts_count': user_info['edge_owner_to_timeline_media']['count'],
                'profile_pic_url': user_info['profile_pic_url'],
                'external_url': user_info['external_url'],
                'business_email': user_info['business_email'],
                'business_phone_number': user_info['business_phone_number'],
                'business_category_name': user_info['business_category_name']
            }
        except (KeyError, IndexError, StopIteration):
            self.logger.warning(
                'Failed to build {0} profile info'.format(username))
            return

        item = {
            'GraphProfileInfo': {
                'info': profile_info,
                'username': username,
                'created_time': 1286323200
            }
        }
        self.save_json(item, '{0}/{1}.json'.format(dst, username))

    def get_shared_data_userinfo(self, username=''):

        resp = self.get_json(BASE_URL + username)

        userinfo = None

        if resp is not None:
            try:
                if "window._sharedData = " in resp:
                    shared_data = resp.split("window._sharedData = ")[
                        1].split(";</script>")[0]
                    if shared_data:
                        userinfo = self.deep_get(self._get_json(
                            shared_data), 'entry_data.ProfilePage[0].graphql.user')
                if "window.__additionalDataLoaded(" in resp and not userinfo:
                    parameters = resp.split("window.__additionalDataLoaded(")[
                        1].split(");</script>")[0]
                    if parameters and "," in parameters:
                        shared_data = parameters.split(",", 1)[1]
                        if shared_data:
                            userinfo = self.deep_get(
                                self._get_json(shared_data), 'graphql.user')
            except (TypeError, KeyError, IndexError):
                import traceback
                traceback.print_exc()
        return userinfo

    @staticmethod
    def save_json(data, dst='./'):
        if not os.path.exists(os.path.dirname(dst)):
            os.makedirs(os.path.dirname(dst))

        if data:
            output_list = {}
            if os.path.exists(dst):
                with open(dst, "rb") as f:
                    output_list.update(json.load(codecs.getreader('utf-8')(f)))

            with open(dst, 'wb') as f:
                output_list.update(data)
                json.dump(output_list, codecs.getwriter('utf-8')(f),
                          indent=4, sort_keys=True, ensure_ascii=False)

    @staticmethod
    def get_logger(level=logging.DEBUG, dest='', verbose=0):
        """Returns a logger."""
        logger = logging.getLogger(__name__)

        dest += '/' if (dest != '') and dest[-1] != '/' else ''
        fh = logging.FileHandler(dest + 'instagram-scraper.log', 'w')
        fh.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        fh.setLevel(level)
        logger.addHandler(fh)

        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        sh_lvls = [logging.ERROR, logging.WARNING, logging.INFO]
        sh.setLevel(sh_lvls[verbose])
        logger.addHandler(sh)

        logger.setLevel(level)

        return logger

    def deep_get(self, dict, path):
        def _split_indexes(key):
            split_array_index = re.compile(r'[.\[\]]+')  # ['foo', '0']
            return filter(None, split_array_index.split(key))

        ends_with_index = re.compile(r'\[(.*?)\]$')  # foo[0]

        keylist = path.split('.')

        val = dict

        for key in keylist:
            try:
                if ends_with_index.search(key):
                    for prop in _split_indexes(key):
                        if prop.isdigit():
                            val = val[int(prop)]
                        else:
                            val = val[prop]
                else:
                    val = val[key]
            except (KeyError, IndexError, TypeError):
                return None

        return val

    def save_cookies(self):
        if self.cookiejar:
            with open(self.cookiejar, 'wb') as f:
                pickle.dump(self.session.cookies, f)

    def _save_login_cookies_to_redis(self, redis_cli, ex=6) -> Optional[RequestsCookieJar]:
        """
        :param redis_cli:
        :param ex: 过期时间, 小时为单位
        :return:
        """
        flag = self.authenticate_with_login()
        if flag:
            content = pickle.dumps(self.session.cookies)
            redis_cli.setex(self.login_user, 60*60*ex, content)
            return self.session.cookies
        return 

    def load_login_cookies(self, redis_cli: redis.Redis, ex=6) -> bool:
        content = redis_cli.get(self.login_user)
        if content is None:
            cookie_jar = self._save_login_cookies_to_redis(redis_cli, ex)
            if cookie_jar:
                self.session.cookies.update(cookie_jar)
                logging.info(f'{self.login_user} load cookies success')
                return True
            logging.info(f'{self.login_user} load cookies fail')
            return False
        else:
            cookie_jar = pickle.loads(content)
            self.session.cookies.update(cookie_jar)
            logging.info(f'{self.login_user} load cookies success')
            return True

    def clear_session_cookies(self):
        self.session.cookies.clear_session_cookies()

if __name__ == '__main__':
    from ins_account import account_list
    p = 5
    accoun = account_list[p]
    proxies = {'http': f'http://{accoun["ip"]}'}
    r = redis.StrictRedis('localhost', 6379, 0)
    scraper = InstagramScraper(login_user=accoun['user'], login_pass=accoun['pwd'], proxies=proxies)
    scraper.load_login_cookies(r)
    user = scraper.get_shared_data_userinfo('glorialrot')  # latitaeve # glorialrot
    print(user)
    r.close()

