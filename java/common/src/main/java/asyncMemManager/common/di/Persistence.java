package asyncMemManager.common.di;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface Persistence {
	public CompletableFuture<UUID> store(UUID key, byte[] data);
	public byte[] retrieve(UUID key);
	public void remove(UUID key);
}
