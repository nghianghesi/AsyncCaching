package asyncMemManager.common.di;

public interface AsyncMemSerializer<T> {
	public String serialize(T object);
	public T deserialize(String data);
	public long estimateObjectSize(T object);
}
