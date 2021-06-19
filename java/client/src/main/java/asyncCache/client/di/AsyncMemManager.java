package asyncCache.client.di;

import asyncCache.client.AsyncMemManager.SetupObject;
import asyncMemManager.common.di.AsyncMemSerializer;

public interface AsyncMemManager {
	public <T> SetupObject<T> manage(String flowKey, T object, AsyncMemSerializer<T> serializer);
}
