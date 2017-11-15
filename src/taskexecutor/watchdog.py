import psutil
import time
import queue

from taskexecutor.logger import LOGGER


class ProcessWatchdog:
    __uids_queue = queue.Queue()

    def __init__(self, interval, max_lifetime):
        self._stopping = False
        self.interval = interval
        self.max_lifetime = max_lifetime
        self._restricted_uids = set()

    @classmethod
    def get_uids_queue(cls):
        return cls.__uids_queue

    @property
    def restricted_uids(self):
        return self._restricted_uids

    @staticmethod
    def _get_processes(uids):
        procs = list()
        for p in psutil.process_iter():
            try:
                if p.uids().real in uids:
                    procs.append(p)
            except psutil.NoSuchProcess:
                continue
        return procs

    @staticmethod
    def _filter_processes(processes):
        filtered = list()
        for p in processes:
            try:
                pp = psutil.Process(p.ppid()) if p.ppid() else None
                if not (pp and len(pp.cmdline()) > 0 and "pure-ftpd" in pp.cmdline()[0] and pp.uids().real == 0):
                    filtered.append(p)
            except psutil.NoSuchProcess:
                continue
        return filtered

    @staticmethod
    def _get_related_paths(process):
        paths = list()
        try:
            paths.append(process.cwd())
            if "OLDPWD" in process.environ().keys():
                paths.append(process.environ()["OLDPWD"])
        except psutil.NoSuchProcess:
            pass
        return paths

    @staticmethod
    def get_workdirs_by_uid(uid):
        dirs = list()
        for p in ProcessWatchdog._get_processes([uid]):
            try:
                dirs.extend(ProcessWatchdog._get_related_paths(p))
            except psutil.NoSuchProcess:
                continue
        return dirs

    def kill_long_processes(self, processes):
        for p in processes:
            try:
                with p.oneshot():
                    uid = p.uids().real
                    pid = p.pid
                    cwd = p.cwd()
                    cmdline = " ".join(p.cmdline())
                    environ = " ".join(["{}={}".format(k, v) for k, v in p.environ().items()])
                    lifetime = int(time.time()) - p.create_time()
                if lifetime > self.max_lifetime:
                    LOGGER.info("Killing process: uid={0}, pid={1}, cwd='{2}', cmdline='{3}', environ='{4}', l"
                                "ifetime={5}s".format(uid, pid, cwd, cmdline, environ, lifetime))
                    p.kill()
            except psutil.NoSuchProcess:
                pass

    def run(self):
        uids_queue = self.get_uids_queue()
        while not self._stopping:
            timestamp = time.time()
            while time.time() < timestamp + self.interval:
                while uids_queue.qsize() > 0:
                    uid = uids_queue.get_nowait()
                    if uid >= 0:
                        self.restricted_uids.add(uid)
                        LOGGER.debug("Started watching UID {}".format(uid))
                    elif abs(uid) in self.restricted_uids:
                        self.restricted_uids.remove(abs(uid))
                        LOGGER.debug("Stopped watching UID {}".format(abs(uid)))
                if self._stopping:
                    break
                time.sleep(.1)
            restricted_processes = self._filter_processes(self._get_processes(self.restricted_uids))
            self.kill_long_processes(restricted_processes)

    def stop(self):
        self._stopping = True
