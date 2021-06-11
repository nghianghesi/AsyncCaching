package asyncCaching.server;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import asyncMemManager.common.Configuration;
import asyncMemManager.common.ManagedObjectQueue;
import asyncMemManager.common.di.IndexableQueuedObject;

public class AsyncMemCache implements asyncCaching.server.di.AsyncMemCache {
	private Configuration config;
	private asyncMemManager.common.di.HotTimeCalculator hotTimeCalculator;
	private asyncMemManager.common.di.Persistence persistence;
	private BlockingQueue<ManagedObjectQueue<CacheData>> candlesPool;
	private List<ManagedObjectQueue<CacheData>> candles;
	private ConcurrentHashMap<UUID, CacheData> keyToObjectMap;
	private AtomicLong usedSize;
	private Comparator<CacheData> cacheNodeComparator = (n1, n2) -> n2.hotTime.compareTo(n1.hotTime);

	//single threads to avoid collision, also, give priority to other flows
	private ExecutorService cleaningExecutor = Executors.newFixedThreadPool(1);
	
	public AsyncMemCache(Configuration config,
			asyncMemManager.common.di.HotTimeCalculator hotimeCalculator, 
			asyncMemManager.common.di.Persistence persistence) 
	{
		this.config = config;
		this.hotTimeCalculator = hotimeCalculator;
		this.persistence = persistence;
		

		this.keyToObjectMap = new ConcurrentHashMap<>(this.config.getInitialSize());
		this.candlesPool = new PriorityBlockingQueue<>(this.config.getCandlePoolSize(), 
														(c1, c2) -> Integer.compare(c1.size(), c2.size()));
		this.candles = new ArrayList<>(this.config.getCandlePoolSize());
		
		int initcandleSize = this.config.getInitialSize() / this.config.getCandlePoolSize();
		initcandleSize = initcandleSize > 0 ? initcandleSize : this.config.getInitialSize();
		
		// init candle pool
		for(int i = 0; i < config.getCandlePoolSize(); i++)
		{
			ManagedObjectQueue<CacheData> candle = new ManagedObjectQueue<>(initcandleSize, this.cacheNodeComparator); // thread-safe ensured by candlesPool
			this.candlesPool.add(candle);
			this.candles.add(candle);
		}		
	}
	
	public void cache(String flowKey, UUID key, byte[] data) 
	{
		CacheData managedObj = new CacheData();
		managedObj.key = key;
		managedObj.data = data;

		managedObj.startTime = LocalTime.now();		
		long waitDuration = this.hotTimeCalculator.calculate(this.config, flowKey);
		managedObj.hotTime = managedObj.startTime.plus(waitDuration, ChronoField.MILLI_OF_SECOND.getBaseUnit());
		
		this.keyToObjectMap.put(managedObj.key, managedObj);		
		
		// put node to candle
		ManagedObjectQueue<CacheData> candle = null;
		try {
			candle = this.candlesPool.take();
		} catch (InterruptedException e) {
			return;
		}

		synchronized (managedObj.key) {
			if (managedObj.startTime != null)
			{
				candle.add(managedObj);
				managedObj.containerCandle = candle;
			}
		}
		
		this.usedSize.addAndGet(managedObj.data.length);		
		this.candlesPool.offer(candle);
		
		if (!this.waitingForPersistence && this.isOverCapability())
		{
			cleaningExecutor.execute(this::cleanUp);
		}
	}
	
	public byte[] retrieve(UUID key) 
	{
		CacheData managedObj = this.keyToObjectMap.remove(key);
		if (managedObj != null)
		{
			synchronized (managedObj.key) {
				managedObj.startTime = null;		
			}
			return managedObj.data;
		}
		return null;
	}
	
	private boolean isOverCapability()
	{
		return this.usedSize.get() > this.config.getCapacity();
	}
	
	private volatile boolean waitingForPersistence = false;
	private void cleanUp()
	{		
		if (waitingForPersistence)
		{
			return;
		}
	
		this.waitingForPersistence = true;
		while (this.isOverCapability())
		{
			CacheData coldestNode = null;
			for (ManagedObjectQueue<CacheData> candle : this.candles)
			{
				CacheData node = candle.peek();
				if (node != null)
				{
					if (coldestNode == null || cacheNodeComparator.compare(coldestNode, node) > 0)
					{
						coldestNode = node;
					}
				}
			}
			
			if (coldestNode != null)
			{			
				ManagedObjectQueue<CacheData> coldestCandle = coldestNode.containerCandle;
				if (coldestCandle != null)
				{
					while(!this.candlesPool.remove(coldestCandle))
					{
						Thread.yield();
					}				
					
					if (coldestNode.containerCandle == coldestCandle && coldestNode == coldestCandle.peek())
					{
						coldestNode = coldestCandle.poll();						
						coldestNode.containerCandle = null;	
						this.candlesPool.offer(coldestCandle);
						
						// coldestNode was removed from candles so, never duplicate persistence.
						final CacheData persistedNode = coldestNode;
						final int datasize = coldestNode.data.length;
						this.persistence.store(coldestNode.key, coldestNode.data)
						.thenRunAsync(()->{					
							// synchronized to ensure retrieve never return null							
							synchronized (persistedNode.key) {
								persistedNode.data = null;	
							}
							
							this.usedSize.addAndGet(-datasize);
						}, this.cleaningExecutor)
						.whenComplete((o, e)->{
							this.waitingForPersistence = false;
							this.cleanUp();
						});
						
						this.candlesPool.add(coldestCandle);
						return;
					}
					this.candlesPool.add(coldestCandle);
				}
			}
		
			this.waitingForPersistence = false;
		}
	}
	
	class CacheData implements IndexableQueuedObject
	{
		/***
		 * key value from client
		 */
		UUID key;
		
		/**
		 * original object
		 */
		byte[] data;
		
		/**
		 * time when object cached
		 * startTime reset to null when object retrieved.
		 */
		LocalTime startTime;
		
		/**
		 * time object expected to be retrieved for async, this is average from previous by keyflow
		 */
		LocalTime hotTime;
		
		/**
		 * the candle contain this object, used for fast cleanup, removal
		 */
		ManagedObjectQueue<CacheData> containerCandle;
		
		/**
		 * the index of object in candle, used for fast removal
		 */
		int candleIndex;
		
		@Override
		public void setIndexInQueue(int idx)
		{
			this.candleIndex = idx;
		}
	}
}
