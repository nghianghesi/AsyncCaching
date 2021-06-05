package asyncCache.common.di;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface Persistence {
	public CompletableFuture<UUID> store(UUID key, byte[] data);
	public CompletableFuture<byte[]> retrieve(UUID key);
}
