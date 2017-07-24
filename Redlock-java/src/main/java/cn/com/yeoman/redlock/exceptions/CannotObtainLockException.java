package cn.com.yeoman.redlock.exceptions;

public class CannotObtainLockException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public CannotObtainLockException() {}
	public CannotObtainLockException(String msg) {
		super(msg);
	}
}
