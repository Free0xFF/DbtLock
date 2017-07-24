'''
@author: yongmao.gui
'''
from Redlock import Redlock,Lock
import logging

key = None
redis_connection = ["redis://localhost:6379/0"]

'''
get lock
'''
def lock(name, validity, retry_count=3, retry_delay=500, **kwargs):
    global key
    if retry_count < 0:
        retry_count = 0
        is_blocking = True  # unlimited retry times
    else:
        is_blocking = False # limited retry times
    
    while True:
        try:
            dlm = Redlock(redis_connection, retry_count=retry_count+1, retry_delay=retry_delay/1000.0)
            lock = dlm.lock(name, validity)
            if lock is False:
                logging.info("Obtain lock failed!")
                err = 1
            else:
                logging.info("Obtain lock successfully!")
                key = lock.key.decode()
                logging.info("lock.key: {}".format(key))
                return 0
        except Exception as ex:
            logging.error("Error occurred while obtain lock: %s" % str(ex))
            err = 3
          
        if is_blocking:
            continue
        else:
            return err

'''
release lock
'''
def unlock(name):
    global key
    try:
        dlm = Redlock(redis_connection)
        lock = Lock(0, name, key)
        dlm.unlock(lock)
    except Exception as err:
        logging.error("Error occurred while release lock: %s" % str(err))
        return 3
    
    return 0


if __name__ == '__main__':
    #simulate clients
    import redis
    server = redis.StrictRedis.from_url(redis_connection[0])
    def incr(name):
        v = server.get(name)
        if v is None:
            v = 1
        else:
            v = int(v.decode()) + 1
        server.set(name, v)
    
    for i in range(50000):
        retcode = lock("test_dbt_lock", 3000)
        print("lock:retcode="+str(retcode))
        incr("key")
        retcode = unlock("test_dbt_lock")
        print("unlock:retcode="+str(retcode))
