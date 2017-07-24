import time
import string
import redis
import random
import logging
from collections import namedtuple
from redis.exceptions import RedisError

string_type = getattr(__builtins__, "basestring", str)
Lock = namedtuple("Lock", ["validity","resource","key"])

class CannotObtainLock(Exception):
    pass

class MultipleRedlockException(Exception):
    def __init__(self, errors, *args, **kwargs):
        super(MultipleRedlockException,self).__init__(*args, **kwargs)
        self.errors = errors
    
    def __str__(self):
        return ' :: '.join(str(e) for e in self.errors)
    
    def __repr__(self):
        self.__str__()
 
        
class Redlock(object):
    
    default_retry_count = 3
    default_retry_delay = 0.2
    clock_drift_factor = 0.01
    
    def __init__(self, connection_list, retry_delay=None, retry_count=None):
        self.servers = list()
        for conn_info in connection_list:
            try:
                if isinstance(conn_info, string_type):
                    server = redis.StrictRedis.from_url(conn_info)
                elif isinstance(conn_info, dict):
                    server = redis.StrictRedis(**conn_info)
                else:
                    server = conn_info
                self.servers.append(server)
            except RedisError as e:
                raise Warning(str(e))
        
        self.quorum = len(connection_list) // 2 + 1
        if len(self.servers) < self.quorum:
            raise CannotObtainLock("Cannnot obtains the lock, failed to connect the majority of servers.")
        
        self.retry_count = retry_count or self.default_retry_count
        self.retry_delay = retry_delay or self.default_retry_delay
    
    def get_unique_id(self):
        CHARACTER = string.ascii_letters + string.digits
        return ''.join(random.choice(CHARACTER) for _ in range(22)).encode()
    
    def lock_instance(self, server, resource, val, ttl):
        try:
            assert isinstance(ttl, int), "ttl {} is not an integer.".format(ttl)
        except AssertionError as e:
            raise ValueError(str(e))
        
        return server.set(resource, val, nx=True, px=ttl)
    
    def unlock_script(self, server, resource, val):
        value = server.get(resource).decode()
        if value == val:
            return server.delete(resource)
        else:
            return 0
        
    def unlock_instance(self, server, resource, val):
        try:
            code = self.unlock_script(server, resource, val)
            if code == 1:
                logging.info("unlock resource %s for server %s successfully." %(resource, str(server)))
            else:
                err = "Error unlock resource %s for server %s." % (resource, str(server))
                raise Exception(err)
        except Exception as err:
            logging.exception(str(err))
    
    def lock(self, resource, ttl):
        retry = 0
        val = self.get_unique_id()
        
        drift = int(ttl * self.clock_drift_factor) + 2
        redis_errors = list()
        while retry < self.retry_count:
            n = 0
            start_time = time.time()*1000
            del redis_errors[:]
            
            for server in self.servers:
                try:
                    if self.lock_instance(server,resource,val,ttl):
                        n += 1
                except RedisError as err:
                    redis_errors.append(err)
            
            elapsed_time = int(time.time()*1000) - start_time
            validity = int(ttl - elapsed_time - drift)
            
            if validity > 0 and n >= self.quorum:
                if redis_errors:
                    raise MultipleRedlockException(redis_errors)
                return Lock(validity, resource, val)
            else:
                # release
                for server in self.servers:
                    try:
                        self.unlock(server, resource, val)
                    except:
                        pass
                retry += 1
                time.sleep(self.retry_delay)
        return False   

    def unlock(self, lock):
        redis_error = list()
        for server in self.servers:
            try:
                self.unlock_instance(server, lock.resource, lock.key)
            except RedisError as err:
                redis_error.append(err)
        if redis_error:
            raise MultipleRedlockException(redis_error)        
