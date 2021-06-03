package asyncCache.common.di;

public interface SingleReadMemcache {
	public String put(String flowKey, byte[] data);
	public byte retrieve(String key);
}
