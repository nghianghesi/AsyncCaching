package asyneMemManager.clientDemo;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
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
	
	public String doSomeOtherThing(){
		try {
			return this.restClient.doSomeOtherThing().execute().body();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "Do some other thing failed";
		}
	}
	
    // don't use async request here, as it cause memory overflow when large amount of request queued.
    // instead, leverage AsycnMemManger + task to queue it.
	private static interface DemoREST
	{
		@GET("/demo/dosomething")
		public Call<String> doSomeThing() ;

		@GET("/demo/dosomeotherthing")
		public Call<String> doSomeOtherThing() ;
	}
}
