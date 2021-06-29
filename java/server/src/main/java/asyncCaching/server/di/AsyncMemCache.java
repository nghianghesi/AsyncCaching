package asyncCaching.server.di;

import java.util.UUID;
import java.util.concurrent.Future;

public interface AsyncMemCache {
	public void cache(UUID key, String data, long expectedDuration);
	public Future<String> retrieve(UUID key);
	public Future<Void> remove(UUID key);
	public long size();
}
