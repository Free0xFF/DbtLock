package cn.com.yeoman.redlock;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.yeoman.redlock.entity.LockEntity;
import cn.com.yeoman.redlock.exceptions.CannotObtainLockException;
import cn.com.yeoman.redlock.exceptions.MultipleRedlockException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.InvalidURIException;

public class Redlock {
	public static final Logger logger = LoggerFactory.getLogger(Redlock.class);
	private static final int default_retry_count = 3;
	private static final int default_retry_delay = 200; //ms
	private static final float clock_drift_factor = 0.01f;
	
	private int retryCount;
	private int retryDelay;
	private List<Jedis> servers;
	private int quorum;
	
	public Redlock(String[] connList, int... args) throws CannotObtainLockException {
		//初始化redis连接
		servers = new ArrayList<Jedis>();
		for(String connInfo : connList) {
			try {
				Jedis edis = new Jedis(connInfo);
				servers.add(edis);
			}
			catch(InvalidURIException e) {
				logger.warn(e.getMessage());
			}
		}
		
		//计算可正常连接的数量，必须要求一半以上
		this.quorum = connList.length/2 + 1;
		if(servers.size() < quorum) {
			throw new CannotObtainLockException();
		}
		
		//初始化其他参数
		this.retryCount = args.length>0 ? args[0] : default_retry_count;
		this.retryDelay = args.length>1 ? args[1] : default_retry_delay;
	}
	
	/**
	 * 
	 * @param server
	 * @param resource
	 * @param val
	 * @param ttl		millis, but expire is seconds
	 * @return
	 */
	public int lockInstance(Jedis server, String resource, String val, int ttl) {
		logger.debug("resource: "+resource+", value: "+val);
		//巨坑：即使setnx不成功，expire肯定成功，从而造成死锁，即resource键永远是一个值
		Long nxRet = server.setnx(resource, val);
		if(nxRet != Long.valueOf(1)) {
			logger.error("nxRet is not 1, nxRet="+nxRet);
			return 0;
		}
		
		Long exRet = server.expire(resource, ttl/1000);
		if(exRet == Long.valueOf(1)) {
			logger.info("exRet and nxRet'value both equal 1.");
			return 1;
		} else {
			logger.error("exRet is not 1, nxRet="+nxRet);
			return 0;
		}
	}
	
	public int unlockInstance(Jedis server, String resource, String val) {
		String value = server.get(resource);
		logger.debug("resource: "+resource+", val in redis: "+value+", val from user: "+val);
		if(value == null) {
			return 1; //说明expire了
		}
		if(value != null && val.equals(value)) {
			if(server.del(resource) == Long.valueOf(1)) {
				logger.info("unlockInstance successfully.");
				return 1;
			}
		}
		return 0;
	}
	
	/**
	 * 
	 * @param resource
	 * @param ttl  expire time, millis
	 * @return
	 * @throws MultipleRedlockException
	 */
	public LockEntity lock(String resource, int ttl) throws MultipleRedlockException {
		//1. 随机生成key和计算漂移时间
		String val = UUID.randomUUID().toString().substring(0, 22);
		int drift = (int)(ttl*clock_drift_factor) + 2;
		int retry = 0;
		List<Exception> error_list = new ArrayList<Exception>();
		
		//2. 结合retry，开始循环
		while(retry < retryCount) {
			int n = 0;
			long startTime = Clock.systemUTC().millis();
			
			try {
				for(Jedis server : servers) {
					if(lockInstance(server, resource, val, ttl) == 1) {
						n += 1;
					} else {
						throw new Exception("server "+server.toString()+" set lock failed!");
					}
				}
			} catch(Exception e) {
				error_list.add(e);
			}
			
			//3. 依次遍历服务器，锁实例
			//获取开始时间 // 锁实例 // 计算操作时间 // 获取有效时间
			//如果时间有效，而且成功的数量大于quorum，则返回锁实例，否则重试
			long elaspedTime = Clock.systemUTC().millis() - startTime;
			long validity = ttl - elaspedTime - drift;
			if(validity > 0 && n >= quorum) {
				if(!error_list.isEmpty()) {
					throw new MultipleRedlockException(error_list);
				}
				logger.info("server get lock successfully, the key is "+val);
				return new LockEntity(validity, resource, val);
			} else {
				logger.warn("validity <= 0 or n < quorum ? validity="+validity+", n>=quorum?"+(n>=quorum));
				try {
					for(Jedis server : servers) {
						if(unlockInstance(server, resource, val) == 1) {
							logger.info("server "+server+" release lock successfully.");
						} else {
							logger.error("resource: "+resource+" unlock failed!");
						}
					}
				} catch(Exception e) {
					logger.error(e.getMessage());
				} finally {
					retry++;
					logger.warn("retry to obtain lock after delay, retry="+retry+", retryDelay="+retryDelay);
					
					try {
						Thread.sleep(retryDelay);
					} catch (InterruptedException e) {
						logger.error(e.getMessage());
					}
				}
			}
		}
		return null;
	}
	
	public void unlock(LockEntity lock) throws MultipleRedlockException {
		//只要有一个server出现解锁异常，都失败
		List<Exception> error_list = new ArrayList<Exception>();
		
		try {
			for(Jedis server : servers) {
				if(unlockInstance(server, lock.getResource(), lock.getKey()) == 1) {
					logger.info("server "+server.toString()+" unlock successfully!");
				} else {
					logger.error("server "+server.toString()+" unlock failed!");
				}
			}
		} catch(Exception e) {
			error_list.add(e);
		}
		
		if(!error_list.isEmpty()) {
			throw new MultipleRedlockException(error_list);
		}
	}
}
