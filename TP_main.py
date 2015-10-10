from multiprocessing import Process
import time, logging
from random import randrange

from ThreadPool import ThreadPool

if __name__ == '__main__':

    num_of_users = 10
    num_of_process = num_of_users + 1
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%d/%m/%y %H:%M')

    logger = logging.getLogger('main')
    logger.setLevel(logging.INFO)


    def wait_delay(d, task_num):
        logger = logging.getLogger('running task %d' % task_num)
        logger.info('sleeping for %d sec', d)
        time.sleep(d)

    def producer(pool, user, delay, lifetime=180.0):
        logger = logging.getLogger('producer %d' % user)
        start_time = time.time()
        real_time_for_jobs = 0
        task_num = 0
        while time.time() - start_time < lifetime:
            d = randrange(1, 30)  # sleep up to 10 sec
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
    #    in the real world apps, the optimal number of threads = num_cores + 1
    pool = ThreadPool(num_of_process, num_of_users)
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