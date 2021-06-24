package asyneMemManager.clientDemo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import asyneMemManager.clientDemo.model.TestEntity;

public class DemoErrorApp {	

	public static void main(String[] args) {
		ExecutorService executor = Executors.newFixedThreadPool(5);
		
		// TODO Auto-generated method stub
		List<CompletableFuture<Void>> tasks = new ArrayList<>();
		int n = 800;
		for (int i=0; i<n; i++)
		{						
			System.out.print("Queuing "+i);
			final TestEntity e = TestEntity.initLargeObject();
			final int idx = i;
			
			CompletableFuture<Void> t=CompletableFuture.runAsync(()->{
				System.out.println("First Async "+ idx + e.getSomeText());
				try {
					Thread.sleep(500 + new Random().nextInt(500));
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}, executor); 
			tasks.add(
				t.thenRunAsync(()->{
					System.out.println("2nd Async "+ idx + e.getSomeText()); 
					try {
						Thread.sleep(1000 + new Random().nextInt(500));
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}, executor));
			tasks.add(
				t.thenRunAsync(()->{
					System.out.println("3rd Async "+ idx + e.getSomeText());
					try {
						Thread.sleep(1500 + new Random().nextInt(500));
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} 
				}, executor));	

			
			try {
				Thread.sleep(10 + new Random().nextInt(100));
			} catch (InterruptedException ex) {
				// TODO Auto-generated catch block
				ex.printStackTrace();
			} 			
		}
		
		System.out.println("All tasks queued. expecting out of heap error ....");
		CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0])).join();
		System.out.print("All tasks completed");
		executor.shutdown();
	}

}
