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

		int capacity = 400 * TestEntity.LARGE_PROPERTY_SIZE;
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
				System.out.println("1st Async "+ idx + " " + e12.supply((o)->{
					try {
						return o.getSomeText();
					} catch (InvalidObjectException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return null;
				}));
				try {
					Thread.sleep(50 + new Random().nextInt(50));
				} catch (InterruptedException ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
				}
			}, executor);
			tasks.add(t);
			
			tasks.add(
				t.thenRunAsync(()->{					
					try {
						e12.close(); // close as this is last access
					} catch (Exception ex) {
						// TODO Auto-generated catch block
						ex.printStackTrace();
					}
					
					e12.apply((o) ->{
						try {
							System.out.println("2nd Async "+ idx +" "+ o.getSomeText());
						} catch (InvalidObjectException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					});
					
					System.out.println(memManager.debugInfo());
					
					try {
						Thread.sleep(100 + new Random().nextInt(50));
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
					
					System.out.println("3rd Async "+ idx +" "+ e3.supply((o)->{
						try {
							return o.getSomeText();
						} catch (InvalidObjectException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						return null;
					}));
					
					System.out.println(memManager.debugInfo());
					
					try {
						Thread.sleep(150 + new Random().nextInt(50));
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
				Thread.sleep(2 + new Random().nextInt(10));
			} catch (InterruptedException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			} 
		}
		
		System.out.println("All tasks queued");
		CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).join();
		System.out.println("All tasks completed");
		System.out.println(memManager.debugInfo());
		executor.shutdown();
		
		try {
			memManager.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
