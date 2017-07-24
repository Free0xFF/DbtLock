package cn.com.yeoman.redlock.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.yeoman.redlock.Redlock;
import cn.com.yeoman.redlock.RedlockFactory;
import cn.com.yeoman.redlock.entity.LockEntity;
import redis.clients.jedis.Jedis;

public class RedlockUtility {
	public static final Logger logger = LoggerFactory.getLogger(RedlockUtility.class);
	public static String[] connList = {"redis://172.168.11.198:6379/0"};
	
	private static LockEntity entity = null;
	
	/**
	 * @param name
	 * @param validity    millis
	 * @param retryCount
	 * @param retryDelay
	 * @return
	 */
	public static int lock(String name, int validity, int retryCount, int retryDelay) {
		if(retryCount < 0 || retryDelay < 0) {
			throw new IllegalArgumentException("Illegal argument retryCount value, retryCount="+retryCount+", retryDelay="+retryDelay);
		}
		
		Boolean isBlocking = null;
		if(retryCount == 0) {
			isBlocking = true;
		} else {
			isBlocking = false;
		}
		
		int err = 0;
		while(true) {
			try {
				Redlock lock = RedlockFactory.create(connList, retryCount, retryDelay);
				entity = lock.lock(name, validity);
				
				if(entity == null) {
					logger.warn("Obtain lock failed");
					err = 1;
				} else {
					logger.info("Obtain lock successfully, key="+entity.getKey());
					return err;
				}
				
			} catch(Exception e) {
				logger.error(e.getMessage());
				err = 3;
			}
			
			if(isBlocking) continue;
			return err;
		}
	}
	
	public static int unlock(String resource) {
		int err = 0;
		try {
			Redlock lock = RedlockFactory.create(connList);
			lock.unlock(entity);
			
		} catch (Exception e) {
			logger.error(e.getMessage());
			err = 3;
		}
		
		return err;
	}
	
	public static void main(String[] args) {
		Jedis server = new Jedis(connList[0]);
		
		//simulate clients
		int retcode = -1;
		for(int i=0; i<50000; i++) {
			retcode = lock("test_for_lock", 3000, 3, 500);
			if(retcode != 0) {
				logger.error("Obtain lock failed!");
			}
			
			String value = server.get("key");
			if(null == value) {
				server.set("key", "1");
			} else {
				value = String.valueOf(Integer.parseInt(value) + 1);
				server.set("key", value);
			}
			retcode = unlock("test_for_lock");
			if(retcode != 0) {
				logger.error("Unlock lock failed!");
			}
		}
	}
}
