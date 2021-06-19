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
import java.util.concurrent.atomic.AtomicLong;

import asyncMemManager.common.Configuration;
import asyncMemManager.common.ManagedObjectQueue;
import asyncMemManager.common.di.IndexableQueuedObject;

public class AsyncMemCache implements asyncCaching.server.di.AsyncMemCache {
	private Configuration config;
	private asyncMemManager.common.di.Persistence persistence;
	private BlockingQueue<ManagedObjectQueue<CacheData>> candlesPool;
	private List<ManagedObjectQueue<CacheData>> candlesSrc;
	private ConcurrentHashMap<UUID, CacheData> keyToObjectMap;
	private AtomicLong usedSize = new AtomicLong(0);
	private Comparator<CacheData> cacheNodeComparator = (n1, n2) -> n2.hotTime.compareTo(n1.hotTime);

	//single threads to avoid collision, also, give priority to other flows
	private ExecutorService manageExecutor;
	
	public AsyncMemCache(Configuration config,
			asyncMemManager.common.di.Persistence persistence) 
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
		CacheData cachedObj = new CacheData();
		cachedObj.key = key;
		cachedObj.data = data;	
		cachedObj.hotTime = LocalTime.now().plus(expectedDuration, ChronoField.MILLI_OF_SECOND.getBaseUnit());
		
		CacheData newData = this.keyToObjectMap.putIfAbsent(cachedObj.key, cachedObj);
		if (newData != cachedObj) // already added by other thread
		{
			return;
		}
		this.usedSize.addAndGet(data.length());
		
		this.queueManageAction(newData, () ->
		{
			// get a candle for container.
			ManagedObjectQueue<CacheData> candle = null;
			try {
				candle = this.candlesPool.take();
			} catch (InterruptedException e) {
				return;
			}
	
			if (cachedObj.containerCandle == null)
			{
				candle.add(cachedObj);
				cachedObj.containerCandle = candle;
			}
			
			this.usedSize.addAndGet(cachedObj.data.length());		
			this.candlesPool.offer(candle);
			
			this.queueCleanUp();
		});
	}
	
	/**
	 * queue manage action for managedObj
	 * @return current containerCandle of managedObj
	 */
	private void queueManageAction(CacheData managedObj, Runnable action)	
	{
		synchronized (managedObj.hotTime) { // to ensure only one manage action queued for this managedObj
			managedObj.manageAction = managedObj.manageAction.thenRunAsync(action);
		}
	}	
	
	public String retrieve(UUID key) 
	{
		CacheData cachedObj = this.keyToObjectMap.remove(key);
		
		if (cachedObj != null)
		{
			synchronized (cachedObj.key) {
				String res = cachedObj.data;
				if (res == null)
				{
					res = this.persistence.retrieve(cachedObj.key);					
				}else {
					// remove from cache
					this.usedSize.addAndGet(-res.length());

					if (cachedObj.containerCandle != null)
					{
						this.queueManageAction(cachedObj, () ->
						{
							if (cachedObj.containerCandle != null)
							{
								while(!this.candlesPool.remove(cachedObj.containerCandle))
								{
									Thread.yield();
								}
								
								if (cachedObj.candleIndex >= 0 && cachedObj.candleIndex < cachedObj.containerCandle.size())
								{
									cachedObj.containerCandle.removeAt(cachedObj.candleIndex);				
								}
								
								this.candlesPool.add(cachedObj.containerCandle);
								
								cachedObj.data = null;
							}
						});
					}
				}
				
				return res;
			}
		}
		return null;
	}
	
	private boolean isOverCapability()
	{
		return this.usedSize.get() > this.config.getCapacity();
	}
	
	// these 2 values to ensure only 1 cleanup queued.
	private Object queueCleanupKey = new Object(); 
	private volatile Boolean waitingForPersistence = false; 
	
	private void queueCleanUp() {		
		if (!this.waitingForPersistence && this.isOverCapability()) {
			synchronized (this.queueCleanupKey) { // ensure only 1 cleanup action queue & executing
				if (!this.waitingForPersistence && this.isOverCapability())
				{
					this.waitingForPersistence = true;
					this.manageExecutor.execute(this::cleanUp);
				}
			}
		}
	}
	
	private void cleanUp()
	{		
		boolean queuedManagement = false;
		while (!queuedManagement && this.isOverCapability())
		{
			ManagedObjectQueue<CacheData>.PollCandidate coldestCandidate = null;
			for (ManagedObjectQueue<CacheData> candle : this.candlesSrc)
			{
				ManagedObjectQueue<CacheData>.PollCandidate node = candle.getPollCandidate();
				if (node != null)
				{
					if (coldestCandidate == null || cacheNodeComparator.compare(coldestCandidate.getObject(), node.getObject()) > 0)
					{
						coldestCandidate = node;
					}
				}
			}
			
			if (coldestCandidate != null)
			{		
				final ManagedObjectQueue<CacheData>.PollCandidate coldestCandidateFinal = coldestCandidate;
				final CacheData coldestNode = coldestCandidate.getObject();
				final ManagedObjectQueue<CacheData> coldestCandle = coldestNode.containerCandle; 
				
				this.queueManageAction(coldestNode, () -> {
					if (coldestCandle != null)
					{					
						
						boolean queuedPersistance = false;

						while(!this.candlesPool.remove(coldestCandle))
						{
							Thread.yield();
						}				
						
						if (coldestNode.containerCandle == coldestCandle)
						{
							CacheData removedNode = coldestCandle.removeAt(coldestCandidateFinal.getIdx());
							if (removedNode == coldestNode)
							{
								coldestNode.containerCandle = null;	
							
								// coldestNode was removed from candles so, never duplicate persistence.
								final CacheData persistedNode = coldestNode;
								final int datasize = coldestNode.data.length();
								long expectedDuration = LocalTime.now().until(persistedNode.hotTime, ChronoField.MILLI_OF_SECOND.getBaseUnit());
								this.persistence.store(coldestNode.key, coldestNode.data, expectedDuration)
								.thenRunAsync(()->{					
									// synchronized to ensure retrieve never return null							
									synchronized (persistedNode.key) {
										persistedNode.data = null;
									}
									
									this.usedSize.addAndGet(-datasize);
	
									this.cleanUp();
								}, this.manageExecutor);
								queuedPersistance = true;
							}
							
							if(!queuedPersistance) {
								// add back if not processing
								coldestCandle.add(removedNode);
							}
						}
						
						// add back to pool after used.
						this.candlesPool.offer(coldestCandle);
						
						if (queuedPersistance) {
							return;
						}
					}
				});
			}
			Thread.yield();
		}
		
		this.waitingForPersistence = false;
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
		String data;
		
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

		@Override
		public boolean isPeekable() {
			// TODO Auto-generated method stub
			return true;
		}
		
		CompletableFuture<Void> manageAction = CompletableFuture.completedFuture(null);
	}
}
