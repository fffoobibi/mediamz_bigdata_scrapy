
from datetime import datetime, timedelta
from scrapy_selenium import SpidersControler
from multiprocessing import freeze_support

    
if __name__ == '__main__':
    freeze_support()
    # 注册自定义的任务调度
    SpidersControler.register_schedule(-1, 'date', run_date=str(datetime.now() + timedelta(seconds=5))[:-7])
   # SpidersControler.schedule_ex()
    SpidersControler.simple()
