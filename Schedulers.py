from multiprocessing import Value, Lock
import random, logging, time


def random_scheduler(self):
    return random.randrange(len(self.tasks))

sched_policies = {'default': random_scheduler,
                  'random': random_scheduler,
                  }


class Task(object):
    def __init__(self, user):
        self.user = user
        self.time = Value('d', 0)
        self.tasks_submitted = Value('i', 0)
        self.task_completed = Value('i', 0)
        self.last_task_finished = Value('d', time.time())
        self.average_wait = Value('d', 0.0)
        self.average_runtime = Value('d', 0.0)
        self.last_wait = Value('d', 0.0)
        self.priority = Value('d', 0.0)
        self.sched_lock = Lock()


class Scheduler(object):
    def __init__(self, sched_policy):
        self.logger = logging.getLogger('scheduler')
        self.sched_policy = sched_policies[sched_policy]
        self.logger.info('%s policy choosen for scheduling', sched_policy)
        self.tasks = {}

    def print_statistics(self):

        self.logger.info('Real times:')
        real_time = [(user, task.time.value) for user, task in self.tasks.iteritems()]
        self.logger.info(real_time)
        task_completed = reduce(lambda res, task: res + task.task_completed.value, self.tasks.values(), 0)
        self.logger.info('Completed_tasks: %d', task_completed)
        self.logger.info('statistics for users: ')

        def print_statistics(user, task, logger):
            self.logger.info('user %d:', user)
            logger.info('\ttask_submitted: %d', task.tasks_submitted.value)
            logger.info('\ttask_completed: %d', task.task_completed.value)
            logger.info('\taverage_wait: %f sec', task.average_wait.value)
            logger.info('\taverage_runtime: %f sec', task.average_runtime.value)
            logger.info('\ttime of active work: %f sec', task.time.value)

        [print_statistics(user, task, self.logger) for user, task in self.tasks.iteritems()]

    def register_user(self, user):
        t = Task(user)
        self.tasks[user] = t

    def unregister_user(self):
        pass

    def add_statistic_on_start(self, user, start_time):
        self.tasks[user].sched_lock.acquire()
        self.tasks[user].tasks_submitted.value += 1
        self.tasks[user].last_wait.value = start_time - self.tasks[user].last_task_finished.value
        tc = self.tasks[user].task_completed.value
        aw = self.tasks[user].average_wait.value
        lw = self.tasks[user].last_wait.value
        # very simple math :)) average(n+1) = (average[n])*n + t[n+]))/(n+1)
        self.tasks[user].average_wait.value = (aw*tc + lw)/(tc + 1.0)
        self.tasks[user].sched_lock.release()

    def add_statistics_on_finish(self, user, time, finish_time):
        pass
        self.tasks[user].sched_lock.acquire()
        self.tasks[user].task_completed.value += 1
        self.tasks[user].time.value += time
        self.tasks[user].last_task_finished.value = finish_time
        avr = self.tasks[user].average_runtime.value
        tsc = self.tasks[user].task_completed.value
        self.tasks[user].average_runtime.value = (avr*(tsc - 1.0) + time)/float(tsc)
        self.tasks[user].sched_lock.release()

    def schedule(self):
        return self.sched_policy(self)


