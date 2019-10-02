import math, logging, time, uuid

from threading import Lock, Thread, Event
from queue import Queue, PriorityQueue
from lace.logging import trace

from idms.settings import INITIAL_THREAD_COUNT, MAX_THREAD_COUNT
from idms.lib.assertions.exceptions import SatisfactionError

@trace('idms.thread')
class ThreadManager(object):
    log = logging.getLogger('idms.thread.watcher')
    def __init__(self):
        def _watcher():
            self.log.info("Starting watcher")
            self._alive = True

            [self._add_worker() for _ in range(INITIAL_THREAD_COUNT)]
            while self._alive:
                job = self._jobs.get()
                w = self._workers.get()
                w.run(job)
                self._workers.put(w)

                with self._qlock:
                    if sum([w.utilization for w in self._worker_list]) / len(self._worker_list) > 0.8 \
                       and len(self._worker_list) < MAX_THREAD_COUNT:
                        self.log.info("Utilization spike detected, adding workers")
                        [self._add_worker() for _ in range(math.ceil(len(self._worker_list) * 0.66))]
                        
                    if sum([w.utilization for w in self._worker_list]) / len(self._worker_list) < 0.2 \
                       and len(self._worker_list) > INITIAL_THREAD_COUNT:
                        self.log.info("Utilization drop detected, removing workers")
                        c = math.ceil(len(self._worker_list) * 0.66)

                        popped = []
                        while not self._workers.empty():
                            w = self._workers.get()
                            if not w.lock.locked():
                                self._worker_list.remove(w)
                                c -= 1
                            else:
                                popped.append(w)
                            if not c: break
                        [self._workers.put(w) for w in popped]

        self._jobs = Queue()
        self._workers, self._worker_list = PriorityQueue(), []
        self._qlock, self._alive = Lock(), False
        runner = Thread(target=_watcher, name="idms_engine", daemon=True)
        runner.start()

    def add_job(self, job, *args, **kwargs):
        tok = Event()
        self.log.debug("Create job -- {}".format(job.__name__))
        self._jobs.put((lambda: job(*args, **kwargs), tok))
        return tok
    def shutdown(self):
        if not self._jobs.empty():
            self.log.warn("[{}] Jobs discarded due to premature shutdown".format(self._jobs.qsize()))
        self._alive = False
    def wait_for(self, jobs):
        jobs = jobs if isinstance(jobs, list) else [jobs]
        [j.wait() for j in jobs]
    def _add_worker(self):
        w = _Worker()
        self._worker_list.append(w)
        self._workers.put(w)

@trace('idms.thread')
class _Worker(object):
    counter = 0
    log = logging.getLogger('idms.thread.worker')
    def __init__(self):
        self.id, self.lock, self.jobs = _Worker.counter, Lock(), Queue()
        runner = Thread(target=self._worker, name="idms_worker_" + str(self.id), daemon=True)
        runner.start()
        _Worker.counter += 1

    @property
    def utilization(self): return 1 if self.lock.locked() else 0
    def run(self, job): self.jobs.put(job)
        
    def _worker(self):
        self.log.debug("Starting Worker")
        while True:
            job, tok = self.jobs.get()
            self.log.debug("Running job on [{}]".format(self.id))
            with self.lock:
                try: job()
                except SatisfactionError as e: self.log.warn(str(e))
                except Exception as e:
                    self.log.error(str(e))
                finally:
                    tok.set()

    def __lt__(a, b): return a.utilization < b.utilization
