from multiprocessing import Value
import random, logging


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
        self.last_task_finished = Value('i', 0)
        self.average_wait_time = Value('i', 0)
        self.priority = 0

class Scheduler(object):
    def __init__(self, sched_policy):
        self.logger = logging.getLogger('scheduler')
        self.sched_policy = sched_policies[sched_policy]
        self.logger.info('%s policy choosen for scheduling', sched_policy)
        self.tasks = {}

    def register_user(self, user):
        t = Task(user)
        self.tasks[user] = t

    def unregister_user(self):
        pass

    def add_statistics(self, user, task):
        task.sched_lock.acquire()
        self.tasks[user].tasks_submitted.value = task.tasks_submitted.value
        self.tasks[user].task_completed.value = task.task_completed.value
        self.tasks[user].time.value = task.time.value
        task.sched_lock.release()

    def schedule(self):
        return self.sched_policy(self)


