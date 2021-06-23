package asyneMemManager.clientDemo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import asyncCache.client.MemCacheServerPersistence;
import asyncCache.client.di.AsyncMemManager;
import asyncMemManager.common.di.HotTimeCalculator;
import asyncMemManager.common.di.Persistence;
import asyneMemManager.clientDemo.model.RandomHotTimeCalculator;
import asyneMemManager.clientDemo.model.TestEntity;

public class DemoApp {

	public static void main(String[] args) {
		ExecutorService executor = Executors.newFixedThreadPool(3);
		

		int capacity = 20 * TestEntity.LARGE_PROPERTY_SIZE;
		int initialSize = 20;
		int cleanupInterval = 3600;
		int candlePoolSize = 4;
		Map<String, asyncMemManager.common.FlowKeyConfiguration> flowKeyConfig = new HashMap<>();
		asyncMemManager.common.Configuration config = new asyncMemManager.common.Configuration(capacity, initialSize, cleanupInterval, candlePoolSize, flowKeyConfig);
				
		Persistence memCachePersistence = new MemCacheServerPersistence("http://localhost:8080/");
		HotTimeCalculator hotTimeCalculator = new RandomHotTimeCalculator();
		AsyncMemManager memManager = new asyncCache.client.AsyncMemManager(config, hotTimeCalculator, memCachePersistence);
		
		// TODO Auto-generated method stub
		List<CompletableFuture<Void>> tasks = new ArrayList<>();
		int n = 800;
		for (int i=0; i<n; i++)
		{			
			System.out.print("Queuing "+i);
			final AsyncMemManager.SetupObject<TestEntity> setupEntity = memManager.manage("DemoFlow", TestEntity.initLargeObject(), TestEntity.TestEntityAsyncMemSerializer.Instance);
			final AsyncMemManager.AsyncObject<TestEntity> e12 = setupEntity.asyncObject();
			final AsyncMemManager.AsyncObject<TestEntity> e3 = setupEntity.asyncObject();
			final int idx = i;
			
			CompletableFuture<Void> t = CompletableFuture.runAsync(()->{
				System.out.println("First Async "+ idx + e12.supply((o)->o.getSomeText()));
				try {
					Thread.sleep(1000 + new Random().nextInt()%1000);
				} catch (InterruptedException ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
				}
			}, executor);
			
			tasks.add(
				t.thenRunAsync(()->{					
					try {
						e12.close(); // close as this is last access
					} catch (Exception ex) {
						// TODO Auto-generated catch block
						ex.printStackTrace();
					}
					
					e12.apply((o) ->{
						System.out.println("2nd Async "+ idx + o.getSomeText());
					});
					
					try {
						Thread.sleep(1000 + new Random().nextInt()%1000);
					} catch (InterruptedException ex) {
						// TODO Auto-generated catch block
						ex.printStackTrace();
					}
				}, executor));
			
			tasks.add(t.thenRunAsync(()->{				
					try {
						e3.close(); // close as this is last e3 access
					} catch (Exception ex) {
						// TODO Auto-generated catch block
						ex.printStackTrace();
					}
					
					System.out.println("3rd Async "+ idx + e3.supply((o)->o.getSomeText()));
					try {
						Thread.sleep(1000 + new Random().nextInt()%1000);
					} catch (InterruptedException ex) {
						// TODO Auto-generated catch block
						ex.printStackTrace();
					} 
				}, executor));
			
			try {
				setupEntity.close();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
			try {
				Thread.sleep(10);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		System.out.println("All tasks queued");
		CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).join();
		System.out.print("All tasks completed");
		executor.shutdown();
		
		try {
			memManager.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
