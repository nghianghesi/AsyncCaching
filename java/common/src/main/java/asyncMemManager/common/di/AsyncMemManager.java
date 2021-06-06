package asyncMemManager.common.di;

import asyncMemManager.common.AsyncMemManager.ManagedObject;

public interface AsyncMemManager {
	public <T> ManagedObject<T> manage(String flowKey, T object, BinarySerializer<T> serializer);
}
