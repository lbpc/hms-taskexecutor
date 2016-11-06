import time
import schedule
from taskexecutor.config import CONFIG
from taskexecutor.facts import FactsSender
from taskexecutor.executor import Executors
from taskexecutor.utils import set_thread_name


class Scheduler:
    def __init__(self):
        self._stopping = False
        self._executors = Executors()
        periodic_jobs = dict()
        try:
            periodic_jobs[FactsSender("unix-account", "quota").update] = \
                CONFIG.schedule.facts.unix_account.quota.interval
            periodic_jobs[FactsSender("database", "quota").update] = \
                CONFIG.schedule.facts.database.quota.interval
            periodic_jobs[FactsSender("mailbox", "quota").update] = \
                CONFIG.schedule.facts.mailbox.quota.interval
        except:
            pass
        for job, interval in periodic_jobs.items():
            schedule.every(interval).seconds.do(
                    self._executors.pool.submit, job
            )

    def start(self):
        set_thread_name("Scheduler")
        while not self._stopping:
            schedule.run_pending()
            if schedule.jobs and not self._stopping:
                time.sleep(abs(schedule.idle_seconds()))
            else:
                time.sleep(.1)

    def schedule(self):
        return schedule

    def stop(self):
        schedule.clear()
        self._stopping = True
