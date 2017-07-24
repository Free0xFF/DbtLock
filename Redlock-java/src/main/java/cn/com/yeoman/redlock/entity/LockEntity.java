package cn.com.yeoman.redlock.entity;

public class LockEntity {
	private long validity;
	private String resource;
	private String key;
	
	public LockEntity() {}
	public LockEntity(long validity, String resource, String key) {
		this.validity = validity;
		this.resource = resource;
		this.key = key;
	}
	
	public long getValidity() {
		return validity;
	}
	public void setValidity(long validity) {
		this.validity = validity;
	}
	public String getResource() {
		return resource;
	}
	public void setResource(String resource) {
		this.resource = resource;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	
	@Override
	public String toString() {
		return String.format("validity=%d, resource=%s, key=%s", validity, resource, key);
	}
}
