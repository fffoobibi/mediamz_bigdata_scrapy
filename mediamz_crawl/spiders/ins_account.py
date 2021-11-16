import random

account_list = [
    {'user': 'eredugym_6653@protonmail.com', 'pwd': 'eredu6653', 'ip': '54.241.41.236:20016', 'use': 0},
    {'user': 'clairewa@protonmail.com', 'pwd': 'wh10305114', 'ip': '54.219.33.201:20097', 'use': 0},
    {'user': 'ladunnaxe_7908@protonmail.com', 'pwd': 'ladunn7908', 'ip': '34.199.11.232:20067', 'use': 0},
    {'user': '+8613866515025', 'pwd': 'adminG', 'ip': '204.236.160.81:20049', 'use': 0},
    {'user': 'miaoyamin0562', 'pwd': 'wzwl@1234', 'ip': '3.101.46.103:20064', 'use': 0},

    {'user': 'luvmyselfalot', 'pwd': 'HYpP3HE8z55FZtm', 'ip': '54.177.104.82:20000', 'use': 0},
    {'user': 'clementina_hopson_437', 'pwd': 'PI6Rtsb36X', 'ip': '54.241.41.236:20016', 'use': 0},
    {'user': 'liusha1994', 'pwd': 'vXU5Q1MHhc', 'ip': '54.219.33.201:20097', 'use': 0},
]

def get_ins_other_account() -> dict:
    accounts = account_list[:3]
    return random.choice(accounts)

def get_ins_spider_account() -> dict:
    accounts = account_list[2:5]
    return random.choice(accounts)

def detect_ips():
    
    def detect(ip):
        try:
            requests.adapters.DEFAULT_RETRIES = 3
            thisProxy = "http://" + ip
            url = 'http://httpbin.org/ip'
            resp = requests.get(url=url, timeout=8,
                                proxies={"http": thisProxy})
            resp.json()
            print(f"测试ip: {ip} 可用")
            return f"测试ip: {ip} 可用"
        except:
            try:
                res = re.findall(r'<title>(.*?)</title>', resp.text)[0]
            except:
                res = ""
            print(f"测试ip: {ip} 不可用 {res}")
            try:
                return f"{ip}: 不可用 {res}"
            except:
                pass

    import requests, re
    from concurrent.futures import ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=5)

    ips = [user['ip'] for user in account_list]

    fp = open('staticip.txt', 'r', encoding='utf8')
    ips = [line.strip() for line in fp if line.strip()]
    # fp.close()

    with open('out.txt', 'w', encoding='utf8') as f:
        for result in executor.map(detect, ips):
            f.write(f'{result}\n')


if __name__ == '__main__':
    detect_ips()

