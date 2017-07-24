package cn.com.yeoman.redlock;

import cn.com.yeoman.redlock.exceptions.CannotObtainLockException;

public class RedlockFactory {
	public static Redlock create(String[] connList, int... args) throws CannotObtainLockException {
		return new Redlock(connList, args);
	}
}
