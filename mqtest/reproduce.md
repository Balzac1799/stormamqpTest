# How to reproduce the error
My Python version: 3.6 ; OS: Debian 8

1. Clone the repo and change work directory to `mqtest` where you can find the `manage.py`
2. Install the `requirements.txt`
3. Start the celery, `Celery worker -A mqtest -l info`
4. Open another terminal tab, run `python manage.py shell`
5. Follow step 4, type `from mqtest.taskapp.tasks import fanout_task`, then `fanout_task.delay('Hello its me')`
6. You should see the `KeyError` in `celery` terminal tab.
