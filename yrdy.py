# from selenium.webdriver import Chrome, ChromeOptions
# from selenium.webdriver.chrome import options
# p = r'C:\Users\admin\Desktop\newproject\chromedriver_95.4638.exe'

# options = ChromeOptions()
# options.add_argument()

# b = Chrome(executable_path=p, options=options)

# b.get('https://www.tiktok.com/@istrepy?')

import configparser

parser = configparser.ConfigParser()

parser.read('config.windows.cfg', encoding='utf8')

def err(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except: ...

print(parser.items('test'))
print('a ===>', err(parser.items, 'test'))

print('d ==>', err(parser.get, 'test', 'd'))