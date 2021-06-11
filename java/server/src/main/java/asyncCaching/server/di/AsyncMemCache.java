package asyncCaching.server.di;

import java.util.UUID;

public interface AsyncMemCache {
	public void cache(String flowKey, UUID key, byte[] data);
	public byte[] retrieve(UUID key);
}
