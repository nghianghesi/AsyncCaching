package asyncCache.client.di;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import asyncMemManager.common.di.AsyncMemSerializer;

public interface AsyncMemManager extends AutoCloseable{
	public <T> SetupObject<T> manage(String flowKey, T object, AsyncMemSerializer<T> serializer);
	
	public interface SetupObject<T> extends AutoCloseable{
		public AsyncObject<T> asyncObject();
		public T o();
	}
	
	public interface AsyncObject<T> extends AutoCloseable
	{		
		public <R> CompletableFuture<R> async(Function<T,R> f);
		public <R> R supply(Function<T,R> f);
	}
}
