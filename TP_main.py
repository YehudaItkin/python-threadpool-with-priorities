from multiprocessing import Process
import time, logging
from random import randrange
import argparse
from Schedulers import sched_policies

from ThreadPool import ThreadPool

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ThreadPool parser')
    parser.add_argument('-u', '--number_of_users', type=int, default=10)
    parser.add_argument('-p', '--number_of_process', type=int, default=5)
    parser.add_argument('--scheduler_policy', choices=sched_policies.keys(), default='default')
    parser.add_argument('--queue_policy', choices=['single_task', 'multi_task'], default='single_task')
    parser.add_argument('--user_lifetime', type=float, default=60.0)
    args = parser.parse_args()

    num_of_users = args.number_of_users
    num_of_process = args.number_of_process
    sched_policy = args.scheduler_policy
    queue_policy = args.queue_policy

    sched_policy = 'average_wait_scheduler'
    queue_policy = 'single_task'
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%d/%m/%y %H:%M')

    logger = logging.getLogger('main')
    logger.setLevel(logging.INFO)


    def wait_delay(d, task_num):
        logger = logging.getLogger('running task %d' % task_num)
        logger.info('sleeping for %d sec', d)
        time.sleep(d)

    def producer(pool, user, delay, lifetime=60.0):
        logger = logging.getLogger('producer %d' % user)
        start_time = time.time()
        real_time_for_jobs = 0
        task_num = 0
        while time.time() - start_time < lifetime:
            d = randrange(1, 10)  # sleep up to 10 sec
            try:
                logger.info('Adding task: sleep for %d secs (task %d)', d, task_num)
                pool.add_task(user, wait_delay, d, task_num)
                real_time_for_jobs += d
            except Exception as e:
                logging.warning(e)
            task_num += 1
            time.sleep(delay)
        logger.info('user %s submitted jobs for %d secs', user, real_time_for_jobs)


    # 1) Init a Thread pool with the desired number of threads and number of users
    pool = ThreadPool(num_of_process, num_of_users, sched_policy, queue_policy)
    producers = []

    for user in range(num_of_users):
        base_time = 20.0  # the slowest user
        delay = base_time / (user + 1)
        p = Process(target=producer, args=(pool, user, delay,))
        producers.append(p)
        p.start()

    # 3) Wait for completion
    [p.join() for p in producers]

    pool.kill()
    pool.wait_completion()

    pool.scheduler.print_statistics()
