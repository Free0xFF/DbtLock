package cn.com.yeoman.redlock.exceptions;

import java.util.ArrayList;
import java.util.List;

public class MultipleRedlockException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2L;
	private List<Exception> errors;

	public MultipleRedlockException() {}
	
	public MultipleRedlockException(String msg) {
		super(msg);
	}
	
	public MultipleRedlockException(List<Exception> errors) {
		this.errors = errors;
	}
	
	@Override
	public String toString() {
		List<String> list = new ArrayList<String>();
		errors.stream().map(e -> list.add(e.getMessage()));
		return String.join(" :: ", list);
	}
}
