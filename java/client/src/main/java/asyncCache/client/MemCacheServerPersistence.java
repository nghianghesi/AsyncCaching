package asyncCache.client;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import asyncMemManager.common.di.Persistence;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;

public class MemCacheServerPersistence implements Persistence{
	private AsyncCachingREST restClient;
	
	public MemCacheServerPersistence(String asyncCachingUrl) {
		ConnectionPool pool = new ConnectionPool(5, 1, TimeUnit.MINUTES);

		OkHttpClient client = new OkHttpClient.Builder()
									  .readTimeout(Duration.ofSeconds(20))
		                              .connectionPool(pool)
		                              .build();
		
		Retrofit retrofit = new Retrofit.Builder()
	            .client(client)
			    .baseUrl(asyncCachingUrl)
			    .addConverterFactory(GsonConverterFactory.create())
			    .build();
		
		this.restClient = retrofit.create(AsyncCachingREST.class);
	}
	
	@Override
	public CompletableFuture<Void> store(UUID key, String data, long expectedDuration) {
		CompletableFuture<Void> res = new CompletableFuture<Void>();
		Call<Void> call = this.restClient.store(key, data, expectedDuration);
		call.enqueue(new Callback<Void>() {
			@Override
		    public void onResponse(Call<Void> call, Response<Void> response) {
		        res.complete(null);  
		    }

		    @Override
		    public void onFailure(Call<Void> call, Throwable t) {
		        // Log error here since request failed
		    	res.completeExceptionally(t);
		    }
		});
		return res;
	}

	@Override
	public String retrieve(UUID key) {
		try {
			return this.restClient.retrieve(key).execute().body();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void remove(UUID key) {
		try {
			this.restClient.remove(key).execute().body();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	private static interface AsyncCachingREST
	{
		@POST("/cache/store/{key}/{expectedDuration}")
		public Call<Void> store(@Path("key")UUID key, @Body String data, @Path("expectedDuration") long expectedDuration) ;

		@GET("/cache/retrieve/{key}")
		public Call<String> retrieve(@Path("key") UUID key);

		@GET("/cache/remove/{key}")
		public Call<Void> remove(@Path("key") UUID key);
	}

}
