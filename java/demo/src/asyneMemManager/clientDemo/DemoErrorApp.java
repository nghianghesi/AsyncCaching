package asyneMemManager.clientDemo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import asyneMemManager.clientDemo.model.TestEntity;

public class DemoErrorApp {	

	public static void main(String[] args) {
		ExecutorService executor = Executors.newFixedThreadPool(10);
		
		// TODO Auto-generated method stub
		List<CompletableFuture<Void>> tasks = new ArrayList<>();
		int n = 100;
		for (int i=0; i<n; i++)
		{			
			final TestEntity e = TestEntity.initLargeObject();
			final int idx = i;
			tasks.add(
				CompletableFuture.runAsync(()->{
					System.out.println("First Async "+ idx + e.getSomeText());
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}, executor)
				.thenRunAsync(()->{
					System.out.println("2nd Async "+ idx + e.getSomeText()); 
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}, executor)
				.thenRunAsync(()->{
					System.out.println("3rd Async "+ idx + e.getSomeText());
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} 
				}, executor));
		}
		
		System.out.println("All tasks queued. expecting out of heap error ....");
		CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).join();
		System.out.print("All tasks completed");
		executor.shutdown();
	}

}
