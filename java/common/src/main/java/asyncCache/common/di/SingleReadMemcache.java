package asyncCache.common.di;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface SingleReadMemcache {
	public UUID put(String flowKey, byte[] data);
	public CompletableFuture<byte[]> retrieve(UUID key);
}
