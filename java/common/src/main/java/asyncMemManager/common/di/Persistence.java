package asyncMemManager.common.di;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface Persistence {
	/**
	 * save data storage
	 * @param key
	 * @param data
	 * @return
	 */
	public CompletableFuture<UUID> store(UUID key, byte[] data);
	
	/**
	 * retrieve and remove data from storage
	 * @param key
	 * @return
	 */
	public byte[] retrieve(UUID key);
	
	/**
	 * remove data from storage.
	 * @param key
	 */
	public void remove(UUID key);
}
