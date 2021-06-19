package asyncCache.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import asyncMemManager.common.di.Serializer;

class BinarySerializerBase {
	
	private static Map<Object, BinarySerializerBase> instances = new ConcurrentHashMap<Object, BinarySerializerBase>();
	
	private Function<Object, String> serialzeFunc;
	private Function<String, Object> deserializeFunc;
	private Function<Object, Long> estimateObjectSizeFunc;
	
	// it's ok to in-thread safe here, as object override wouldn't cause any issue.
	public static <T> BinarySerializerBase getBinarySerializerBaseInstance(Serializer<T> serializer)
	{
		BinarySerializerBase inst = BinarySerializerBase.instances.getOrDefault(serializer.getClass(), null);
		if(inst == null)
		{
			BinarySerializerBase.instances.put(serializer, inst = new BinarySerializerBase(serializer));
		}
		return inst;
	}
	
	@SuppressWarnings("unchecked")
	private <T> BinarySerializerBase(Serializer<T> serializer)
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
	
	public String serialize(Object object)
	{
		return this.serialzeFunc.apply(object);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T deserialize(String data)
	{
		return (T)this.deserializeFunc.apply(data);
	}
	
	public long estimateObjectSize(Object object)
	{
		return this.estimateObjectSizeFunc.apply(object);
	}
}
