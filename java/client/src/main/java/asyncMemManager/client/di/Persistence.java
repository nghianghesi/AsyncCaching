package asyncMemManager.client.di;

import java.util.UUID;

public interface Persistence {
	/**
	 * save data storage
	 * @param key
	 * @param data
	 * @return
	 */
	public void store(UUID key, String data, long expectedDuration);
	
	/**
	 * retrieve and remove data from storage
	 * @param key
	 * @return
	 */
	public String retrieve(UUID key);
	
	/**
	 * remove data from storage.
	 * @param key
	 */
	public void remove(UUID key);
}
