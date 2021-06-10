package asyncCache.client.di;

import asyncCache.client.AsyncMemManager.SetupObject;
import asyncMemManager.common.di.BinarySerializer;

public interface AsyncMemManager {
	public <T> SetupObject<T> manage(String flowKey, T object, BinarySerializer<T> serializer);
}
