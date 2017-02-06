import time
import schedule

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.facts
import taskexecutor.executor
import taskexecutor.utils


class Scheduler:
    def __init__(self):
        self._stopping = False
        self._futures = list()
        self._executors = taskexecutor.executor.Executors()
        for res_type, fact in vars(CONFIG.schedule.facts).items():
            res_type = taskexecutor.utils.to_lower_dashed(res_type)
            if res_type in CONFIG.enabled_resources:
                constructor = taskexecutor.constructor.Constructor()
                facts_reporter = constructor.get_facts_reporter(res_type)
                for fact_type, scheduling in vars(fact).items():
                    reporter_method = getattr(facts_reporter, "report_{}".format(fact_type))
                    job = schedule.every(scheduling.interval).seconds.do(self.submit_future, reporter_method)
                    LOGGER.info(job)

    def start(self):
        taskexecutor.utils.set_thread_name("Scheduler")
        while not self._stopping:
            schedule.run_pending()
            for future in self._futures:
                if not future.running():
                    if future.exception():
                        LOGGER.error(future.exception())
                    self._futures.remove(future)
            if schedule.jobs and not self._stopping:
                time.sleep(abs(schedule.idle_seconds()))
            else:
                time.sleep(.1)

    def submit_future(self, func):
        self._futures.append(self._executors.pool.submit(func))

    def schedule(self):
        return schedule

    def stop(self):
        schedule.clear()
        self._stopping = True
