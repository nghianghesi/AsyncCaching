package asyncMemManager.common.di;

public interface BinarySerializer<T> {
	public byte[] serialize(Object object);
	public T deserialize(byte[] data);
	public long estimateObjectSize(Object object);
}
