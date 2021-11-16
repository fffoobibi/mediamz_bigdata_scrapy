from setuptools import setup, find_packages

setup(
    name="scrapy-selenium",
    version="1.0.0",
    packages=["scrapy_selenium"],
    install_requires=["scrapy", "scrapy_redis", "selenium",
                      "yagmail", "requests", "fake_useragent==0.1.11", "pymysql"],

    python_requires=">=3.6",
    author="fatebibi",
    author_email="2836204894@qq.com",
    description="Scrapy and Selenium",
)
