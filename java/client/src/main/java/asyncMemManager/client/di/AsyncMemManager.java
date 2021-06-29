package asyncMemManager.client.di;

import java.util.function.Consumer;
import java.util.function.Function;

public interface AsyncMemManager extends AutoCloseable{
	public <T> SetupObject<T> manage(String flowKey, T object, AsyncMemSerializer<T> serializer);
	public String debugInfo(); 
	
	public interface SetupObject<T> extends AutoCloseable{
		public AsyncObject<T> asyncObject();
		public T o();
	}
	
	public interface AsyncObject<T> extends AutoCloseable
	{		
		public <R> R supply(Function<T,R> f);
		public void apply(Consumer<T> f);
	}
}
