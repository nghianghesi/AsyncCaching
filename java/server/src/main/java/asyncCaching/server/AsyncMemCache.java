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
import asyncMemManager.common.ReadWriteLock;
import asyncMemManager.common.ReadWriteLock.ReadWriteLockableObject;
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
		CacheData cachedObj = new CacheData();
		cachedObj.key = key;
		cachedObj.data = data;

		cachedObj.startTime = LocalTime.now();		
		long waitDuration = this.hotTimeCalculator.calculate(this.config, flowKey);
		cachedObj.hotTime = cachedObj.startTime.plus(waitDuration, ChronoField.MILLI_OF_SECOND.getBaseUnit());
		
		CacheData newData = this.keyToObjectMap.putIfAbsent(cachedObj.key, cachedObj);
		if (newData != cachedObj) // already added by other thread
		{
			return;
		}
		
		// put node to candle
		ManagedObjectQueue<CacheData> candle = null;
		try {
			candle = this.candlesPool.take();
		} catch (InterruptedException e) {
			return;
		}

		ReadWriteLock<CacheData> lock = cachedObj.lockManage();
		if (cachedObj.containerCandle == null)
		{
			candle.add(cachedObj);
			cachedObj.containerCandle = candle;
		}
		lock.unlock();
		
		this.usedSize.addAndGet(cachedObj.data.length);		
		this.candlesPool.offer(candle);
		
		if (!this.waitingForPersistence && this.isOverCapability())
		{
			cleaningExecutor.execute(this::cleanUp);
		}
	}
	
	public byte[] retrieve(UUID key) 
	{
		CacheData cachedObj = this.keyToObjectMap.remove(key);
		if (cachedObj != null)
		{
			ReadWriteLock<CacheData>lock = cachedObj.lockRead();
			if (cachedObj.data == null)
			{
				ReadWriteLock<CacheData> manageLock = lock.upgrade();
				cachedObj.data = this.persistence.retrieve(cachedObj.key);
				manageLock.downgrade();
			}
			byte[] res = cachedObj.data;
			lock.unlock();
			return res;
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
			ManagedObjectQueue<CacheData>.PollCandidate coldestCandidate = null;
			for (ManagedObjectQueue<CacheData> candle : this.candles)
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
				CacheData coldestNode = coldestCandidate.getObject();
				ManagedObjectQueue<CacheData> coldestCandle = coldestNode.containerCandle;
				if (coldestCandle != null)
				{
					while(!this.candlesPool.remove(coldestCandle))
					{
						Thread.yield();
					}				
					
					if (coldestNode.containerCandle == coldestCandle)
					{
						coldestNode = coldestCandle.removeAt(coldestCandidate.getIdx());
						if (coldestNode == coldestCandidate.getObject())
						{
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
							
							// add back to pool after processing
							this.candlesPool.add(coldestCandle);
							return;
						}else {
							// add node back if not processed
							coldestCandle.add(coldestNode);
						}
					}
					
					// add back to pool if node not processed
					this.candlesPool.add(coldestCandle);
				}
			}
			Thread.yield();
		}
		this.waitingForPersistence = false;
	}
	
	class CacheData implements IndexableQueuedObject, ReadWriteLockableObject
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

		@Override
		public boolean isPeekable() {
			// TODO Auto-generated method stub
			return true;
		}
		
		/**
		 * used for read/write locking this cache object. 
		 */
		private int accessCounter = 0;
		
		/**
		 * read locking, retrieve flows
		 */
		ReadWriteLock<CacheData> lockRead()
		{
			return new ReadWriteLock.ReadLock<CacheData>(this);
		}		
		
		/**
		 * manage locking, used for cleanup, add 
		 */
		ReadWriteLock<CacheData> lockManage()
		{
			return new ReadWriteLock.WriteLock<CacheData>(this);
		}
		
		public int getLockFactor() {
			return this.accessCounter;
		}
		
		public void addLockFactor(int lockfactor) {
			this.accessCounter += lockfactor;
		}
		
		public Object getKeyLocker() {
			return this.key;
		}		
	}
}
