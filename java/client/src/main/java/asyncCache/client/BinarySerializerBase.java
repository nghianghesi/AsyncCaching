package asyncCache.client;

import java.util.function.Function;

import asyncMemManager.common.di.BinarySerializer;

class BinarySerializerBase {
	
	private Function<Object, byte[]> serialzeFunc;
	private Function<byte[], Object> deserializeFunc;
	private Function<Object, Long> estimateObjectSizeFunc;
	
	
	@SuppressWarnings("unchecked")
	public <T> BinarySerializerBase(BinarySerializer<T> serializer)
	{
		this.serialzeFunc = (obj) -> {
			return serialize((T)obj);
		};		
		
		this.deserializeFunc = (data) -> {
			return serializer.deserialize(data);
		};
		
		this.estimateObjectSizeFunc = (obj) -> {
			return serializer.estimateObjectSize((T)obj);
		};
	}
	
	public byte[] serialize(Object object)
	{
		return this.serialzeFunc.apply(object);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T deserialize(byte[] data)
	{
		return (T)this.deserializeFunc.apply(data);
	}
	
	public long estimateObjectSize(Object object)
	{
		return this.estimateObjectSizeFunc.apply(object);
	}
}
