package asyncCaching.server.di;

import java.util.UUID;

public interface AsyncMemCache {
	public void cache(UUID key, String data, long expectedDuration);
	public String retrieve(UUID key);
}
