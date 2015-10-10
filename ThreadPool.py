from multiprocessing import Queue, Process, Lock, Value
import logging, timeit, time
from Schedulers import Scheduler


# Preventing DDOS. This number is arbitrary
MAX_NUMBER_JOBS_IN_QUEUE = 1000



class UsersQueue(object):
    def __init__(self, maxsize=0):
        self.q = Queue(maxsize)
        self.time = Value('d', 0)
        self.tasks_submitted = Value('i', 0)
        self.task_completed = Value('i', 0)
        self.order_lock = Lock()


class Worker(Process):
    """Thread executing tasks from a given tasks queue"""

    def __init__(self, tasks, killer, scheduler):
        Process.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.killer = killer
        self.logger = logging.getLogger('Worker')
        self.scheduler = scheduler
        self.start()

    def is_all_tasks_empty(self):
        for task in self.tasks:
            if not task.q.empty():
                return False
        return True

    def run(self):
        while not self.killer.value or not self.is_all_tasks_empty():

            i = self.scheduler.schedule()
            self.tasks[i].order_lock.acquire()
            # Critical section
            if self.tasks[i].q.empty():
                self.tasks[i].order_lock.release()
                continue
            self.scheduler.add_statistic_on_start(i, time.time())
            func, args, kwargs = self.tasks[i].q.get()

            try:
                t = timeit.timeit(lambda: func(*args, **kwargs), number=1)
                self.tasks[i].time.value += t
                self.tasks[i].task_completed.value += 1
                self.scheduler.add_statistics_on_finish(i, t, time.time())
            except Exception as e:
                # io is slow, so we want to rrelase the lock is
                self.tasks[i].order_lock.release()
                self.logger.warning("%s", e)
                continue

            # end of critical section
            self.tasks[i].order_lock.release()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads, num_users, sched_policy='default'):
        self.procs = []
        self.tasks = []
        self.num_users = num_users
        self.die = Value('b', False)
        self.tasks_submitted = 0
        self.scheduler = Scheduler(sched_policy)
        for _ in range(num_users):
            self.tasks.append(UsersQueue(0))
            self.scheduler.register_user(_)

        for _ in range(num_threads):
            p = Worker(self.tasks, self.die, self.scheduler)
            self.procs.append(p)

    def kill(self):
        self.die.value = True

    def add_task(self, num, func, *args, **kargs):
        """Add a task to the queue"""

        """Because MAX_NUMBER_JOBS_IN_QUEUE is aproximate, I dont use lock here"""
        if not self.die.value and self.tasks[num].tasks_submitted.value < MAX_NUMBER_JOBS_IN_QUEUE:
            self.tasks[num].q.put((func, args, kargs))
            self.tasks_submitted += 1
            self.tasks[num].tasks_submitted.value += 1
        else:
            raise Exception('Unable to add task')

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        [p.join() for p in self.procs]
