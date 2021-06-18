package asyncCaching.server.di;

import java.util.UUID;

public interface AsyncMemCache {
	public void cache(UUID key, byte[] data, long expectedDuration);
	public byte[] retrieve(UUID key);
}
