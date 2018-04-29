from __future__ import absolute_import
from mqtest.mqtest_celery import app
from mqtest.generalapp.service import publish_message
from django.conf import settings
from celery.signals import worker_ready, worker_process_init # noqa


#@worker_process_init.connect
#def at_start(*args, **kwargs):
#    print(args, kwargs)
#    settings.PRO.setup()


@app.task
def test_task():
    print('test task is called')
    return 'Test Task Done'


@app.task
def pub2mq():
    publish_message()


@app.task
def ins_pub2mq(msg):
    # print(dir(settings))
    settings.PRO.pub_msg(msg)


@app.task
def fanout_task(msg):
    # settings.PRO.setup()
    settings.PRO.pub_msg(msg)
