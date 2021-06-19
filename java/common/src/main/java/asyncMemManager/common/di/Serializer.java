package asyncMemManager.common.di;

public interface Serializer<T> {
	public String serialize(Object object);
	public T deserialize(String data);
	public long estimateObjectSize(Object object);
}
