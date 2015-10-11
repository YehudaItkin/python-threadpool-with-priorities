from multiprocessing import Value, Lock
import random, logging, time


def random_scheduler(self):
    return random.randrange(len(self.tasks))


def average_runtime_scheduler(self):
    """Choose the user with minimal runtime"""
    # During the search minimal average runtime can change (I dont take global lock on queues)
    # but it doesnt bother us. Process with lower runtime still will have higher priorities
    min_avg = float('inf')
    # if all the ques are empty or working return the first user
    min_user = random.randrange(len(self.tasks))
    for user, task in self.tasks.iteritems():
        if not task.queue.q.empty() and task.queue.order_lock.acquire(False):
                if task.average_runtime.value < min_avg:
                    min_user = user
                    min_avg = task.average_runtime.value
                task.queue.order_lock.release()
    return min_user


def average_wait_scheduler(self):
    """Choose the user with maximum average wait"""

    # During the search minimal average runtime can change (I dont take global lock on queues)
    # but it doesnt bother us. Process with lower higher wait will have higher priorities
    # Indeed this scheduler is a bad idea. The process with low waittime will never run
    # and has no chances to change this waittime.
    # Th e solution is - update waittime on every iteration of scheduling. Thats done in
    # dirty wait scheduler
    max_avg = 0.0
    # if all the ques are empty or working return the first user
    user = random.randrange(len(self.tasks))
    for u, task in self.tasks.iteritems():
        if not task.queue.q.empty() and task.queue.order_lock.acquire(False):
            if task.average_wait.value >= max_avg:
                user = u
                max_avg = task.average_wait.value
            task.queue.order_lock.release()
    return user

def dirty_wait_scheduler(self):
    """Choose the user with maximum average wait"""

    # During the search minimal average runtime can change (I dont take global lock on queues)
    # but it doesnt bother us. Process with lower higher wait will have higher priorities
    # Indeed this scheduler is a bad idea. The process with low waittime will never run
    # and has no chances to change this waittime.
    # Th e solution is - update waittime on every iteration of scheduling. Thats done in
    # dirty wait scheduler
    max_avg = 0.0
    # if all the ques are empty or working return the first user
    user = random.randrange(len(self.tasks))
    for u, task in self.tasks.iteritems():
        if not task.queue.q.empty() and task.queue.order_lock.acquire(False):
            if task.dirty_wait.value >= max_avg:
                user = u
                max_avg = task.dirty_wait.value
            task.queue.order_lock.release()
    return user



def max_waittime_scheduler(self):
    """Choose the user that waits longest time"""
    # During the search minimal average runtime can change (I dont take global lock on queues)
    # but it doesnt bother us. Process with lower runtime still will have higher priorities
    max_time = float('inf')
    # if all the ques are empty or working return the first user
    user = random.randrange(len(self.tasks))
    for u, task in self.tasks.iteritems():
        if not task.queue.q.empty() and task.queue.order_lock.acquire(False):
                if task.last_task_finished.value < max_time:
                    user = u
                    max_time = task.last_task_finished.value
                task.queue.order_lock.release()
    return user


sched_policies = {'default': random_scheduler,
                  'random': random_scheduler,
                  'average_runtime_scheduler': average_runtime_scheduler,
                  'average_wait_scheduler': average_wait_scheduler,
                  'max_waittime_scheduler': max_waittime_scheduler,
                  'dirty_wait_scheduler': dirty_wait_scheduler
                  }


class Task(object):
    def __init__(self, user, queue):
        self.user = user
        self.queue = queue
        self.time = Value('d', 0)
        self.tasks_submitted = Value('i', 0)
        self.task_completed = Value('i', 0)
        self.last_task_finished = Value('d', time.time())
        self.average_wait = Value('d', 0.0) # the only way for average_wait_scheduler to work
        self.average_runtime = Value('d', 0.0)
        self.dirty_wait = Value('d', 0) # wait also when queue is empty
        self.av_dirty_wait = Value('d', 0) # wait also when queue is empty
        self.last_wait = Value('d', 0.0)
        self.priority = Value('d', 0.0)


class Scheduler(object):
    def __init__(self, sched_policy):
        self.logger = logging.getLogger('scheduler')
        self.sched_policy = sched_policies[sched_policy]
        self.tasks = {}
        self.global_scheduler_lock = Lock()

    def register_user(self, user, queue):
        # dictionary is thread safe. No lock here
        self.tasks[user] = Task(user, queue)

    def unregister_user(self):
        pass

    def update_statistics(self, time):
        self.global_scheduler_lock.acquire()
        for task in self.tasks.values():
            if task.queue.order_lock.acquire(False):
                task.dirty_wait.value = time - task.last_task_finished.value
                task.queue.order_lock.release()
        self.global_scheduler_lock.release()

    def add_statistic_on_start(self, user, start_time):
        self.global_scheduler_lock.acquire()
        self.tasks[user].tasks_submitted.value += 1
        self.tasks[user].last_wait.value = start_time - self.tasks[user].last_task_finished.value
        tc = self.tasks[user].task_completed.value
        aw = self.tasks[user].average_wait.value
        lw = self.tasks[user].last_wait.value
        # very simple math :)) average(n+1) = (average[n])*n + t[n+]))/(n+1)
        self.tasks[user].average_wait.value = (aw*tc + lw)/(tc + 1.0)
        self.global_scheduler_lock.release()

    def add_statistics_on_finish(self, user, time, finish_time):
        pass
        self.global_scheduler_lock.acquire()
        self.tasks[user].task_completed.value += 1
        self.tasks[user].time.value += time
        self.tasks[user].last_task_finished.value = finish_time
        avr = self.tasks[user].average_runtime.value
        tsc = self.tasks[user].task_completed.value
        # very simple math :)) average(n+1) = (average[n])*n + t[n+]))/(n+1)
        self.tasks[user].average_runtime.value = (avr*(tsc - 1.0) + time)/float(tsc)
        self.global_scheduler_lock.release()

    def schedule(self):
        self.global_scheduler_lock.acquire()
        user = self.sched_policy(self)
        self.global_scheduler_lock.release()
        return user

    def print_statistics(self):

        def print_statistics(user, task, logger):
            self.logger.info('user %d:', user)
            logger.info('\ttask_submitted: %d', task.tasks_submitted.value)
            logger.info('\ttask_completed: %d', task.task_completed.value)
            logger.info('\taverage_wait: %f sec', task.average_wait.value)
            logger.info('\taverage_runtime: %f sec', task.average_runtime.value)
            logger.info('\ttime of active work: %f sec', task.time.value)

        self.logger.info('statistics for users: ')
        [print_statistics(user, task, self.logger) for user, task in self.tasks.iteritems()]

        print '--------------------------------------------'
        self.logger.info('Average for pool:')
        num_users = len(self.tasks)
        task_submitted = reduce(lambda res, task: res + task.tasks_submitted.value, self.tasks.values(), 0)
        task_completed = reduce(lambda res, task: res + task.task_completed.value, self.tasks.values(), 0)
        av_wait = reduce(lambda res, task: res + task.average_wait.value, self.tasks.values(), 0)/num_users
        av_time = reduce(lambda res, task: res + task.average_runtime.value, self.tasks.values(), 0)/num_users
        av_active = reduce(lambda res, task: res + task.time.value, self.tasks.values(), 0)/num_users

        self.logger.info('\ttask_submitted: %d', task_submitted)
        self.logger.info('\ttask_completed: %d', task_completed)
        self.logger.info('\taverage_wait: %f sec', av_wait)
        self.logger.info('\taverage_task_runtime: %f sec', av_time)
        self.logger.info('\ttime of active work: %f sec', av_active)

        diff = self.tasks[0].average_wait.value - self.tasks[num_users - 1].average_wait.value
        self.logger.info('\tdifference between fastest and slowest procs: %f sec', diff)


