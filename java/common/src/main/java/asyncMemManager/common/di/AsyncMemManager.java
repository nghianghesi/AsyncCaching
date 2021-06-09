package asyncMemManager.common.di;

import asyncMemManager.common.AsyncMemManager.SetupObject;

public interface AsyncMemManager {
	public <T> SetupObject<T> manage(String flowKey, T object, BinarySerializer<T> serializer);
}
