package asyncCache.common.di;

public interface Persistence {
	public void store(String key, byte[] data);
	public byte[] retrieve(String key, byte[] data);
}
