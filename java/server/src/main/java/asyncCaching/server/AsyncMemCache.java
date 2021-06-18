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
import java.util.function.Consumer;

import asyncMemManager.common.Configuration;
import asyncMemManager.common.ManagedObjectQueue;
import asyncMemManager.common.di.IndexableQueuedObject;

public class AsyncMemCache implements asyncCaching.server.di.AsyncMemCache {	
	
	// this is for special marker only.
	static private ManagedObjectQueue<CacheData> queuedForManageCandle = new ManagedObjectQueue<CacheData>(0, null);

	private Configuration config;
	private asyncMemManager.common.di.Persistence persistence;
	private BlockingQueue<ManagedObjectQueue<CacheData>> candlesPool;
	private List<ManagedObjectQueue<CacheData>> candlesSrc;
	private ConcurrentHashMap<UUID, CacheData> keyToObjectMap;
	private AtomicLong usedSize;
	private Comparator<CacheData> cacheNodeComparator = (n1, n2) -> n2.hotTime.compareTo(n1.hotTime);

	//single threads to avoid collision, also, give priority to other flows
	private ExecutorService manageExecutor;
	
	public AsyncMemCache(Configuration config,
			asyncMemManager.common.di.HotTimeCalculator hotimeCalculator, 
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
	
	public void cache(UUID key, byte[] data, long expectedDuration) 
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
		
		this.queueManageAction(newData, ManagementState.None, (ManagedObjectQueue<CacheData> nullUnused) ->
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
			
			this.usedSize.addAndGet(cachedObj.data.length);		
			this.candlesPool.offer(candle);
			
			this.queueCleanUp();
		});
	}
	
	/**
	 * queue manage action for managedObj
	 * @return current containerCandle of managedObj
	 */
	private boolean queueManageAction(CacheData managedObj, ManagementState expectedCurrentState, Consumer<ManagedObjectQueue<CacheData>> action)	
	{
		if (managedObj.getManagementState() == expectedCurrentState) { 
			synchronized (managedObj.hotTime) { // to ensure only one manage action queued for this managedObj
				if (managedObj.getManagementState() == expectedCurrentState) 
				{ 
					ManagedObjectQueue<CacheData> containerCandle = managedObj.setManagementState(AsyncMemCache.queuedForManageCandle);					
					this.manageExecutor.execute(() -> action.accept(containerCandle));
					return true;
				}
			}
		}
		
		return false;
	}	
	
	public byte[] retrieve(UUID key) 
	{
		CacheData cachedObj = this.keyToObjectMap.remove(key);
		if (cachedObj != null)
		{
			synchronized (cachedObj.key) {
				byte[] res = cachedObj.data;
				if (res == null)
				{
					res = this.persistence.retrieve(cachedObj.key);					
				}else {
					this.usedSize.addAndGet(-res.length);
				}
				
				this.queueManageAction(cachedObj, ManagementState.Managing, (ManagedObjectQueue<CacheData> candle) ->
				{
					while(!this.candlesPool.remove(candle))
					{
						Thread.yield();
					}
					
					if (cachedObj.candleIndex >= 0 && cachedObj.candleIndex < candle.size())
					{
						candle.removeAt(cachedObj.candleIndex);				
					}
					
					this.candlesPool.add(candle);
				});
				
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
				
				queuedManagement = this.queueManageAction(coldestNode, ManagementState.Managing, (coldestCandle) -> {
					boolean queuedPersistance = false;
					if (coldestCandle != null)
					{
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
								final int datasize = coldestNode.data.length;
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
							}else {
								// add node back if not processed
								coldestCandle.add(coldestNode);							
							}	
							
							if(!queuedPersistance) {
								// add back if not processing
								coldestCandle.add(removedNode);
							}						
						}
						
						// add back to pool after used.
						this.candlesPool.offer(coldestCandle);
								
						if (!queuedPersistance) {
							this.cleanUp();
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
		byte[] data;
		
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
		
		/**
		 * get management state to have associated action.
		 * this is for roughly estimate, as not ensured thread-safe.
		 */
		ManagementState getManagementState()
		{
			if (this.containerCandle == null)
			{
				return ManagementState.None;
			}else if (this.containerCandle == AsyncMemCache.queuedForManageCandle){
				return ManagementState.Queued;
			}else {
				return ManagementState.Managing;
			}
		}
		
		/**
		 * return previous containerCandel
		 */
		ManagedObjectQueue<CacheData> setManagementState(ManagedObjectQueue<CacheData> containerCandle)
		{
			ManagedObjectQueue<CacheData> prev = this.containerCandle;
			this.containerCandle = containerCandle;
			return prev;
		}	
	}
	
	static enum ManagementState
	{
		None,
		Queued,
		Managing,
	}
}
