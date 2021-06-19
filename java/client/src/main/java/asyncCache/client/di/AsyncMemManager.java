package asyncCache.client.di;

import asyncCache.client.AsyncMemManager.SetupObject;
import asyncMemManager.common.di.Serializer;

public interface AsyncMemManager {
	public <T> SetupObject<T> manage(String flowKey, T object, Serializer<T> serializer);
}
