package asyncCache.common;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Supplier;

public class SingleReadMemcache implements asyncCache.common.di.SingleReadMemcache {
	
	private Configuration config;
	private asyncCache.common.di.HotTimeCalculator coldTimeCalculator;
	private asyncCache.common.di.Persistence persistence;
	private PriorityBlockingQueue<Queue<CacheNode>> candlesPool;
	private List<Queue<CacheNode>> candles;
	private ConcurrentHashMap<UUID, CacheNode> map;
	private long usedSize;
	private Object usedSizeKey = new Object(); 
	private ExecutorService executorService;

	public SingleReadMemcache(Configuration config,
								asyncCache.common.di.HotTimeCalculator coldTimeCalculator, 
								asyncCache.common.di.Persistence persistence) 
	{
		this.config = config;
		this.coldTimeCalculator = coldTimeCalculator;
		this.persistence = persistence;
		this.map = new ConcurrentHashMap<>(this.config.initialSize);
		this.candlesPool = new PriorityBlockingQueue<>(this.config.candlePoolSize, 
														(c1, c2) -> Integer.compare(c1.size(), c2.size()));
		this.candles = new ArrayList<>(this.config.candlePoolSize);
		
		// init candle pool
		for(int i = 0; i < config.candlePoolSize; i++)
		{
			Queue<CacheNode> candle = new PriorityQueue<>();
			this.candlesPool.add(candle);
			this.candles.add(candle);
		}
		
		this.executorService = Executors.newFixedThreadPool(this.config.candlePoolSize);
	}
	
	@Override
	public UUID put(String flowKey, byte[] data) 
	{
		// init key, mapKey, newnode
		UUID key = UUID.randomUUID();
		CacheNode newnode = new CacheNode();
		newnode.key = key;
		newnode.data = data;
		newnode.startTime = LocalTime.now();
		
		long waitDuration = this.coldTimeCalculator.calculate(this.config, flowKey);
		newnode.hotTime = newnode.startTime.plus(waitDuration, ChronoField.MILLI_OF_SECOND.getBaseUnit());
		
		this.map.put(key, newnode);

		synchronized (newnode.key) {
			// put node to candle
			Queue<CacheNode> candle = null;
			try {
				candle = this.candlesPool.take();
			} catch (InterruptedException e) {
				return key;
			}

			newnode.candle = candle;
			candle.add(newnode);
			
			synchronized (this.usedSizeKey) {
				this.usedSize += newnode.data.length;
			}				
		}
		
		new RecursiveCompletableFuture(() -> this.isOverCapability() ? this.cleanUp() : null).run();		
		return key;
	}

	@Override
	public CompletableFuture<byte[]> retrieve(UUID key) 
	{
		CacheNode node = this.map.remove(key);
		if(node != null)
		{
			synchronized (node.key) {
				while(!this.candlesPool.remove(node.candle))
				{
					Thread.yield();
				}
				node.candle.remove(node);
			}
			
			this.candlesPool.offer(node.candle);
			return CompletableFuture.completedFuture(node.data);
		}else {
			return this.persistence.retrieve(key);
		}
	}
	
	private boolean isOverCapability()
	{
		return this.usedSize > 0 && this.usedSize > this.config.capacity;
	}
	
	private CompletableFuture<Void> cleanUp()
	{
		return CompletableFuture.completedFuture(null);
	}

	public static class FlowKeyConfiguration
	{
	
	}
	
	public static class Configuration
	{
		int initialSize;
		int capacity;
		int cleanupInterval;
		int candlePoolSize;
		Map<String, FlowKeyConfiguration> flowKeyConfig = new HashMap<>();

		public Configuration(int capacity, 
								int initialSize, 
								int cleanupInterval,
								int candlePoolSize,
								Map<String, FlowKeyConfiguration> flowKeyConfig) 
		{
			this.capacity = capacity;
			this.candlePoolSize = candlePoolSize >= 0 ? candlePoolSize : Runtime.getRuntime().availableProcessors();
			this.initialSize = initialSize > 0 ? initialSize : 100;
			this.cleanupInterval = cleanupInterval;
			this.flowKeyConfig = flowKeyConfig;
		}
	}
	
	static class CacheNode
	{
		UUID key;
		byte[] data;
		LocalTime startTime;
		LocalTime hotTime;
		Queue<CacheNode> candle;
	}
	
	private static class RecursiveCompletableFuture
	{
		Runnable runnable;
		RecursiveCompletableFuture(Supplier<CompletableFuture<Void>> s)
		{
			this.runnable = () -> {
				CompletableFuture<Void> f = s.get();
				if(f!=null)
				{
					f.thenRun(this.runnable);
				}
			};
		}
		
		void run() 
		{
			this.runnable.run();
		}
	}
}
