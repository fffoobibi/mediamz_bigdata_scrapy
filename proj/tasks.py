from proj.app_test import app

# ===============> periodic 定时任务

@app.on_after_configure.connect
def setup_periodic_task(sender, **kwargs):
    # 每天15 31分执行任务
    sender.add_periodic_task(
        10.0,
        # crontab(minute='56', hour='15'),
        add.s(15, 20),
        name='every 10 seconds'
    )


@app.task
def add(x, y):
    print(f'add x={x}, y={y}, ==> {x+y}')
    return x + y

@app.task(bind=True)
def test(self, x, y):
    print('test.name : ', test.name, f'args: {self.request.args} kwargs: {self.request.kwargs}')

# ==============================> retry

@app.task(autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 5}) #default_retry_delay=5)
def retry_test():
    print('compute 3 / 0')
    return 3 / 0

@app.task(bind=True)
def retry_test2(self):
    print('compute 3/0 =====> 2')
    try:
        return 3 / 0
    except Exception as e:
        raise self.retry(exc=e, countdown=5, max_retries=3)

