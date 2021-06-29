package asyncCaching.server;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import asyncCaching.server.di.Persistence;
import asyncMemManager.common.Configuration;
import asyncMemManager.common.ManagedObjectQueue;
import asyncMemManager.common.di.IndexableQueuedObject;

public class AsyncMemCache implements asyncCaching.server.di.AsyncMemCache {	
	private Configuration config;
	private Persistence persistence;
	private BlockingQueue<ManagedObjectQueue<CacheData>> candlesPool;
	private List<ManagedObjectQueue<CacheData>> candlesSrc;
	private ConcurrentHashMap<UUID, CacheData> keyToObjectMap;
	private AtomicLong usedSize = new AtomicLong(0);
	private Comparator<CacheData> cacheNodeComparator = (n1, n2) -> n2.hotTime.compareTo(n1.hotTime);

	//single threads to avoid collision, also, give priority to other flows
	private ExecutorService manageExecutor;
	
	public AsyncMemCache(Configuration config, Persistence persistence) 
	{
		this.config = config;
		this.persistence = persistence;
		

		this.keyToObjectMap = new ConcurrentHashMap<>(this.config.getInitialSize());
		this.candlesPool = new PriorityBlockingQueue<>(this.config.getCandlePoolSize(), 
														(c1, c2) -> Integer.compare(c1.size(), c2.size()));
		this.candlesSrc = new ArrayList<>(this.config.getCandlePoolSize());
		
		int initcandleSize = this.config.getInitialSize() / this.config.getCandlePoolSize();
		initcandleSize = initcandleSize > 0 ? initcandleSize : this.config.getInitialSize();
		
		int numberOfManagementThread = this.config.getCandlePoolSize();
		numberOfManagementThread = numberOfManagementThread > 0 ? numberOfManagementThread : 1;
		this.manageExecutor = Executors.newFixedThreadPool(numberOfManagementThread + 1);
		
		// init candle pool
		for(int i = 0; i < config.getCandlePoolSize(); i++)
		{
			ManagedObjectQueue<CacheData> candle = new ManagedObjectQueue<>(initcandleSize, this.cacheNodeComparator); // thread-safe ensured by candlesPool
			this.candlesPool.add(candle);
			this.candlesSrc.add(candle);
		}
	}
	
	public void cache(UUID key, String data, long expectedDuration) 
	{
		LocalTime hottime = LocalTime.now().plus(expectedDuration, ChronoField.MILLI_OF_SECOND.getBaseUnit());
		CacheData cachedObj = new CacheData(key, data, hottime);
		
		CacheData newData = this.keyToObjectMap.putIfAbsent(cachedObj.key, cachedObj);
		if (newData != null) // already added by other thread
		{
			return;
		}
		
		this.usedSize.addAndGet(data.length());
		
		this.queueManageAction(cachedObj, () ->
		{
			// get a candle for container.
			ManagedObjectQueue<CacheData> candle = this.pollCandle();
				
			candle.add(cachedObj);
			cachedObj.containerCandle = candle;			
			this.candlesPool.offer(candle);
			
			// queue cleanup
			if (this.isOverCapability() && !this.cleanupRunning.getAndSet(true)) {
				this.manageExecutor.execute(this::cleanUp);
			}
		});
	}
	
	/**
	 * queue manage action for managedObj
	 * @return current containerCandle of managedObj
	 */
	private void queueManageAction(CacheData managedObj, Runnable action)	
	{
		synchronized (managedObj) { // to ensure only one manage action executing for this managedObj
			managedObj.manageAction = managedObj.manageAction.thenRunAsync(action, this.manageExecutor);
		}
	}	
	
	public String retrieve(UUID key) 
	{
		CacheData cachedObj = this.keyToObjectMap.remove(key);
		String res = null;
		if (cachedObj != null)
		{
			res = cachedObj.data;
			if (res == null)
			{
				res = this.persistence.retrieve(cachedObj.key);					
			}else {
				this.queueManageAction(cachedObj, () ->
				{
					if (this.pollCandle(cachedObj.containerCandle) != null)
					{
						cachedObj.containerCandle.removeAt(cachedObj.candleIndex);
						this.candlesPool.offer(cachedObj.containerCandle);
						cachedObj.containerCandle = null;	
						
						this.usedSize.addAndGet(-cachedObj.data.length());
						cachedObj.data = null;
					}else
					{
						this.persistence.remove(cachedObj.key);
					}
				});
			}
		}
		
		return res;
	}
	
	public long size() {
		return this.keyToObjectMap.size();
	}
	
	private boolean isOverCapability()
	{
		return this.usedSize.get() > this.config.getCapacity();
	}
	
	private ManagedObjectQueue<CacheData> pollCandle(){
		try {
			return this.candlesPool.take();
		} catch (InterruptedException e) {
			return null;
		}
	}
	
	private ManagedObjectQueue<CacheData> pollCandle(ManagedObjectQueue<CacheData> containerCandle)
	{
		if (containerCandle != null)
		{
			while (!this.candlesPool.remove(containerCandle))
			{
				Thread.yield();
			}
		}
		return containerCandle;
	}
	
	// to ensure only 1 cleanup queued.
	private volatile AtomicBoolean cleanupRunning = new AtomicBoolean(); 	
	private void cleanUp()
	{		
		while (this.isOverCapability())
		{
			CacheData coldestCandidate = null;
			for (ManagedObjectQueue<CacheData> candle : this.candlesSrc)
			{
				CacheData node = candle.getPollCandidate();
				if (node != null)
				{
					if (coldestCandidate == null || cacheNodeComparator.compare(coldestCandidate, node) > 0)
					{
						coldestCandidate = node;
					}
				}
			}
			
			if (coldestCandidate != null)
			{		
				final CacheData coldestNode = coldestCandidate;
				
				this.queueManageAction(coldestNode, () -> {
					
					if (this.pollCandle(coldestNode.containerCandle) != null) 
					{
						coldestNode.containerCandle.removeAt(coldestNode.candleIndex);
						this.candlesPool.offer(coldestNode.containerCandle);						
						coldestNode.containerCandle = null;	
					
						// coldestNode was removed from candles so, never duplicate persistence.
						this.persistence.store(coldestNode.key, coldestNode.data);
						this.usedSize.addAndGet(-coldestNode.data.length());
						coldestNode.data = null;						
					}
					
					this.cleanUp();
				});
				
				return;
			}
			
			Thread.yield();
		}
		
		// could bit over capacity here, but it's ok.
		this.cleanupRunning.set(false);
	}
	
	class CacheData implements IndexableQueuedObject
	{
		/***
		 * key value from client
		 */
		final UUID key;
		
		/**
		 * original object
		 */
		volatile String data;
		
		/**
		 * time object expected to be retrieved for async
		 */
		final LocalTime hotTime;
		
		/**
		 * the candle contain this object, used for fast cleanup, removal
		 */
		volatile ManagedObjectQueue<CacheData> containerCandle;
		
		/**
		 * the index of object in candle, used for fast removal
		 */
		volatile int candleIndex;
		
		@Override
		public void setIndexInQueue(int idx)
		{
			this.candleIndex = idx;
		}

		@Override
		public boolean isPeekable() {
			return true;
		}
		
		volatile CompletableFuture<Void> manageAction = CompletableFuture.completedFuture(null);
		
		public CacheData(UUID key, String data, LocalTime hottime)
		{
			this.key = key;
			this.data = data;
			this.hotTime = hottime;
		}
	}
}
