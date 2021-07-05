package asyncMemManager.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import asyncMemManager.client.di.AsyncMemSerializer;


class SerializerGeneral {
	
	private static Map<Object, SerializerGeneral> instances = new ConcurrentHashMap<Object, SerializerGeneral>();
	
	private Function<Object, String> serialzeFunc;
	private Function<String, Object> deserializeFunc;
	private Function<Object, Long> estimateObjectSizeFunc;
	
	// it's ok to in-thread safe here, as object override wouldn't cause any issue.
	@SuppressWarnings("unchecked")
	public static <T> SerializerGeneral getSerializerBaseInstance(AsyncMemSerializer<T> serializer)
	{
		SerializerGeneral inst = SerializerGeneral.instances.getOrDefault(serializer.getClass(), null);
		if(inst == null)
		{
			inst = new SerializerGeneral();
			
			inst.serialzeFunc = (obj) -> {
				return serializer.serialize((T)obj);
			};		
			
			inst.deserializeFunc = (data) -> {
				return serializer.deserialize(data);
			};
			
			inst.estimateObjectSizeFunc = (obj) -> {
				return serializer.estimateObjectSize((T)obj);
			};
			
			SerializerGeneral.instances.put(serializer.getClass(), inst);
		}
		
		return SerializerGeneral.instances.get(serializer.getClass());
	}

    private SerializerGeneral()
    {}	
	
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
