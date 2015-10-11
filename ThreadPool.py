from multiprocessing import Queue, Process, Lock, Value
import logging, timeit, time
from Schedulers import Scheduler


# Preventing DDOS. This number is arbitrary
MAX_NUMBER_JOBS_IN_QUEUE = 1000


class SingleTaskLock(object):
    """Lock Wrapper for single task policy"""

    def __init__(self):
        self.lock = Lock()

    def acquire(self, block=True, timeout=None):
        return self.lock.acquire(block, timeout)

    def release(self):
        self.lock.release()

    def single_task_release(self):
        self.lock.release()

    def multi_task_release(self):
        pass


class MultiTaskLock(object):
    """Lock Wrapper for multi task policy"""

    def __init__(self):
        self.lock = Lock()

    def acquire(self, block=True, timeout=None):
        return self.lock.acquire(block, timeout)

    def release(self):
        self.lock.release()

    def single_task_release(self):
        pass

    def multi_task_release(self):
        self.lock.release()


class UsersQueue(object):
    def __init__(self, maxsize=0, policy='single_task'):
        self.q = Queue(maxsize)
        self.active_tasks = Value('i', 0)
        if policy == 'single_task':
            self.order_lock = SingleTaskLock()
        elif policy == 'multi_task':
            self.order_lock = MultiTaskLock()
        else:
            raise Exception('No policy!!')


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
                # lock should be freed in both cases
                self.tasks[i].order_lock.release()
                continue
            self.logger.debug('Starting task %d', i)
            self.scheduler.add_statistic_on_start(i, time.time())
            func, args, kwargs = self.tasks[i].q.get()
            self.tasks[i].order_lock.multi_task_release()
            try:
                t = timeit.timeit(lambda: func(*args, **kwargs), number=1)
                self.tasks[i].active_tasks.value -= 1
                self.scheduler.add_statistics_on_finish(i, t, time.time())
                self.logger.debug('Finished task %d', i)
            except Exception as e:
                # io is slow, so we want to release the lock before
                self.tasks[i].order_lock.single_task_release()
                self.logger.warning("%s", e)
                continue

            # end of critical section for single task policy
            self.tasks[i].order_lock.single_task_release()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads, num_users, sched_policy='default', queue_policy='single_task'):
        self.logger = logging.getLogger('ThreadPool')
        self.procs = []
        self.tasks = []
        self.num_user = num_users
        self.users = range(num_users)
        self.die = Value('b', False)
        self.scheduler = Scheduler(sched_policy)
        self.logger.info('%s policy choosen for scheduling', sched_policy)
        self.logger.info('%s policy choosen for queue', queue_policy)
        for _ in self.users:
            q = UsersQueue(0, queue_policy)
            self.tasks.append(q)
            self.scheduler.register_user(_, q)

        for _ in range(num_threads):
            p = Worker(self.tasks, self.die, self.scheduler)
            self.procs.append(p)

    def kill(self):
        self.die.value = True

    def add_task(self, num, func, *args, **kargs):
        """Add a task to the queue"""

        """Because MAX_NUMBER_JOBS_IN_QUEUE is aproximate, I dont use lock here"""
        if not self.die.value and self.tasks[num].active_tasks.value < MAX_NUMBER_JOBS_IN_QUEUE:
            self.tasks[num].q.put((func, args, kargs))
            self.tasks[num].active_tasks.value += 1
        else:
            raise Exception('Unable to add task')

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        [p.join() for p in self.procs]
