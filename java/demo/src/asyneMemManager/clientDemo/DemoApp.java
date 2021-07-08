package asyneMemManager.clientDemo;

import java.io.InvalidObjectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import asyncMemManager.client.AvgWaitTimeCalculator;
import asyncMemManager.client.MemCacheServerPersistence;
import asyncMemManager.client.di.*;
import asyneMemManager.clientDemo.model.TestEntity;

public class DemoApp {

	public static void main(String[] args) {
		ExecutorService executor = Executors.newFixedThreadPool(10);		
		ExecutorService otherExecutor = Executors.newFixedThreadPool(5);

		int capacity = 300 * TestEntity.LARGE_PROPERTY_SIZE;
		int initialSize = 20;
		int cleanupInterval = 3600;
		int candlePoolSize = 10;
		Map<String, asyncMemManager.common.FlowKeyConfiguration> flowKeyConfig = new HashMap<>();
		asyncMemManager.common.Configuration config = new asyncMemManager.common.Configuration(capacity, initialSize, cleanupInterval, candlePoolSize, flowKeyConfig);
				
		Persistence memCachePersistence = new MemCacheServerPersistence("http://localhost:8080/");
		HotTimeCalculator hotTimeCalculator = new AvgWaitTimeCalculator(500);
		AsyncMemManager memManager = new asyncMemManager.client.AsyncMemManager(config, hotTimeCalculator, memCachePersistence);
		
		List<CompletableFuture<Void>> tasks = new ArrayList<>();
		int n = 10000;
		for (int i=0; i<n; i++)
		{			
			final AsyncMemManager.SetupObject<TestEntity> setupEntity = memManager.manage("DemoFlow", TestEntity.initLargeObject(), TestEntity.TestEntityAsyncMemSerializer.Instance);
			final AsyncMemManager.AsyncObject<TestEntity> e12 = setupEntity.asyncObject();
			final AsyncMemManager.AsyncObject<TestEntity> e3 = setupEntity.asyncObject();
			final int idx = i;
			
			CompletableFuture<Void> t = CompletableFuture.runAsync(()->{
				System.out.println("1st Async "+ idx + " " + e12.supply((o)-> getSomeText(o)));
				ThreadSleep(50 + new Random().nextInt(50));
			}, executor);
			tasks.add(t);
			
			tasks.add(
				t.thenRunAsync(()->{					
					CloseAsyncObject(e12); // close as this is last e12 access
					
					e12.apply((o) ->{ System.out.println("2nd Async "+ idx +" "+ getSomeText(o)); });
					
					System.out.println(memManager.debugInfo());					
					ThreadSleep(100 + new Random().nextInt(50));
				}, executor));
			
			tasks.add(t.thenRunAsync(()->{				
					CloseAsyncObject(e3); // close as this is last e3 access
					
					System.out.println("3rd Async "+ idx +" "+ e3.supply((o)-> getSomeText(o)));
					
					System.out.println(memManager.debugInfo());
					
					ThreadSleep(150 + new Random().nextInt(50));
				}, executor));
			
			tasks.add(DoSomethingOther(otherExecutor, setupEntity, idx)
					.whenComplete((r,e) -> {
						System.out.println("Other: " + memManager.debugInfo());
					}));
			
			CloseSetupObject(setupEntity);
			
			ThreadSleep(2 + new Random().nextInt(10));
		}
		
		System.out.println("All tasks queued");
		CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).join();
		System.out.println("All tasks completed");
		System.out.println(memManager.debugInfo());
		executor.shutdown();
		otherExecutor.shutdown();
		
		try {
			memManager.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void ThreadSleep(long mi) {
		try {
			Thread.sleep(mi);
		} catch (InterruptedException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}
	}
	
	private static void CloseAsyncObject(AsyncMemManager.AsyncObject<TestEntity> e)
	{
		try {
			e.close();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	private static void CloseSetupObject(AsyncMemManager.SetupObject<TestEntity> e)
	{
		try {
			e.close();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	private static String getSomeText(TestEntity o)
	{
		try {
			return o.getSomeText();
		} catch (InvalidObjectException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	private static CompletableFuture<Void> DoSomethingOther(ExecutorService otherExecutor, AsyncMemManager.SetupObject<TestEntity> setupEntity, int idx)
	{
		final AsyncMemManager.AsyncObject<TestEntity> e = setupEntity.asyncObject();

		
		return CompletableFuture.runAsync(()->{
			System.out.println("1st Other "+ idx + " " + e.supply((o) -> getSomeText(o)));
			
			ThreadSleep(50 + new Random().nextInt(50));
		}, otherExecutor)
		.thenRunAsync(()->{					
				CloseAsyncObject(e);// close as this is last access
				
				e.apply((o) ->{
					System.out.println("2nd Other "+ idx +" "+ getSomeText(o));
				});
				ThreadSleep(100 + new Random().nextInt(50));
			}, otherExecutor);
	}
}
