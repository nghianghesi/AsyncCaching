package asyncMemManager.common.di;

public interface AsyncMemManager {
	public <T> T manage(String flowKey, T object, BinarySerializer<T> serializer);
}
