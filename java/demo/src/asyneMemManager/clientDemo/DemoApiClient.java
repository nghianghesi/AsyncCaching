package asyneMemManager.clientDemo;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.converter.scalars.ScalarsConverterFactory;
import retrofit2.http.GET;

public class DemoApiClient {
	private DemoREST restClient;
	
	public DemoApiClient(String apiUrl) {
		ConnectionPool pool = new ConnectionPool(20, 1, TimeUnit.MINUTES);

		OkHttpClient client = new OkHttpClient.Builder()
									  .readTimeout(Duration.ofSeconds(20))
		                              .connectionPool(pool)
		                              .build();
		
		Retrofit retrofit = new Retrofit.Builder()
	            .client(client)
			    .baseUrl(apiUrl)
			    .addConverterFactory(ScalarsConverterFactory.create())
			    .addConverterFactory(GsonConverterFactory.create())
			    .build();
		
		this.restClient = retrofit.create(DemoREST.class);		
	}
	
	public String doSomeThing(){
		try {
			return this.restClient.doSomeThing().execute().body();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "Do something failed";
		}
	}
	
	public CompletableFuture<String> doSomeThingAsync(){
		CompletableFuture<String> res = new CompletableFuture<String>();
		
		this.restClient.doSomeThing().enqueue(new Callback<String>() 
		{		     
		    @Override
		    public void onResponse(Call<String> call, Response<String> response) 
		    {
		    	res.complete(response.body());
		    }
		 
		    @Override
		    public void onFailure(Call<String> call, Throwable t) 
		    {
		    	res.complete("Do something failed");
		    }
		});
		return res;
	}
	
	public String doSomeOtherThing(){
		try {
			return this.restClient.doSomeOtherThing().execute().body();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "Do some other thing failed";
		}
	}

	public CompletableFuture<String> doSomeOtherThingAsync(){

		CompletableFuture<String> res = new CompletableFuture<String>();
		this.restClient.doSomeOtherThing().enqueue(new Callback<String>() 
		{		     
		    @Override
		    public void onResponse(Call<String> call, Response<String> response) 
		    {
		    	res.complete(response.body());
		    }
		 
		    @Override
		    public void onFailure(Call<String> call, Throwable t)
		    {
		    	res.complete("Do some other thing failed");
		    }
		});
		return res;
	}
	
	
	private static interface DemoREST
	{
		@GET("/demo/dosomething")
		public Call<String> doSomeThing() ;

		@GET("/demo/dosomeotherthing")
		public Call<String> doSomeOtherThing() ;
	}
}
