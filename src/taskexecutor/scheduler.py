import time
import schedule
from taskexecutor.config import CONFIG
import taskexecutor.facts
import taskexecutor.executor
import taskexecutor.utils


class Scheduler:
    def __init__(self):
        self._stopping = False
        self._executors = taskexecutor.executor.Executors()
        periodic_jobs = dict()
        for res_type in ("unix-account", "database", "mailbox"):
            if res_type in CONFIG.enabled_resources:
                periodic_jobs[taskexecutor.facts.FactsSender(res_type, "quota").update] = \
                    getattr(CONFIG.schedule.facts, res_type.replace("-", "_")).quota.interval
        for job, interval in periodic_jobs.items():
            schedule.every(interval).seconds.do(self._executors.pool.submit, job)

    def start(self):
        taskexecutor.utils.set_thread_name("Scheduler")
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
