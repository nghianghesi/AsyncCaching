package asyncMemManager.common;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import asyncMemManager.common.AsyncMemManager.ManagedObjectBase.KeyLock;
import asyncMemManager.common.AsyncMemManager.ManagedObjectBase.ManageKeyLock;
import asyncMemManager.common.AsyncMemManager.ManagedObjectBase.ReadKeyLock;
import asyncMemManager.common.di.BinarySerializer;

public class AsyncMemManager implements asyncMemManager.common.di.AsyncMemManager, AutoCloseable {
	
	private Configuration config;
	private asyncMemManager.common.di.HotTimeCalculator coldTimeCalculator;
	private asyncMemManager.common.di.Persistence persistence;
	private BlockingQueue<ManagedObjectQueue> candlesPool;
	private List<ManagedObjectQueue> candles;
	private ConcurrentHashMap<UUID, ManagedObjectBase> keyToObjectMap;
	private AtomicLong usedSize;
	private Comparator<ManagedObjectBase> coldCacheNodeComparator = (n1, n2) -> n2.hotTime.compareTo(n1.hotTime);

	private ExecutorService manageExecutor = Executors.newFixedThreadPool(1);
	private ExecutorService cleaningExecutor = Executors.newFixedThreadPool(1);
	
	/**
	 * Construct Async Mem Manager
	 * @param config
	 * @param coldTimeCalculator
	 * @param persistence
	 */
	public AsyncMemManager(Configuration config,
								asyncMemManager.common.di.HotTimeCalculator coldTimeCalculator, 
								asyncMemManager.common.di.Persistence persistence) 
	{
		this.config = config;
		this.coldTimeCalculator = coldTimeCalculator;
		this.persistence = persistence;
		this.keyToObjectMap = new ConcurrentHashMap<>(this.config.initialSize);
		this.candlesPool = new PriorityBlockingQueue<>(this.config.candlePoolSize, 
														(c1, c2) -> Integer.compare(c1.size(), c2.size()));
		this.candles = new ArrayList<>(this.config.candlePoolSize);
		
		int initcandleSize = this.config.initialSize / this.config.candlePoolSize;
		initcandleSize = initcandleSize > 0 ? initcandleSize : this.config.initialSize;
		// init candle pool
		for(int i = 0; i < config.candlePoolSize; i++)
		{
			ManagedObjectQueue candle = new ManagedObjectQueue(initcandleSize, this.coldCacheNodeComparator); // thread-safe ensured by candlesPool
			this.candlesPool.add(candle);
			this.candles.add(candle);
		}
	}
	
	/***
	 * put object to cache
	 * @param flowKey
	 * @param object
	 * @param serializer
	 * @return key for retrieve object from cache.
	 */
	@Override
	public <T> SetupObject<T> manage(String flowKey, T object, BinarySerializer<T> serializer) 
	{
		// init key, mapKey, newnode
		if (object == null)
		{
			return null;
		}
		
		ManagedObject<T> managedObj = new ManagedObject<T>(object);
		managedObj.serializer = new BinarySerializerBase(serializer);
		managedObj.startTime = LocalTime.now();
		managedObj.estimatedSize = serializer.estimateObjectSize(object);
		
		long waitDuration = this.coldTimeCalculator.calculate(this.config, flowKey);
		managedObj.hotTime = managedObj.startTime.plus(waitDuration, ChronoField.MILLI_OF_SECOND.getBaseUnit());	
		return new SetupObject<T>(managedObj);
	}

	@Override
	public void close() throws Exception {
		for (int i=0; i<this.candles.size(); i++)
		{
			this.candlesPool.take();
		}
		
		this.cleaningExecutor.shutdown();
		this.manageExecutor.shutdown();

		for (ManagedObjectBase managedObject: this.keyToObjectMap.values())
		{
			if(managedObject.object == null)
			{
				this.persistence.remove(managedObject.key);
			}
		}
		
		this.keyToObjectMap.clear();
	}
	
	private void startTracking(ManagedObjectBase managedObj) {
		this.manageExecutor.execute(()->{ // use single thread executor to avoid collision start stop tracking
			this.keyToObjectMap.put(managedObj.key, managedObj);		
			
			// put node to candle
			ManagedObjectQueue candle = null;
			try {
				candle = this.candlesPool.take();
			} catch (InterruptedException e) {
				return;
			}
	
			candle.add(managedObj);
			managedObj.containerCandle = candle;
			
			this.usedSize.addAndGet(managedObj.estimatedSize);
			
			this.candlesPool.offer(candle);
			if (!this.waitingForPersistence && this.isOverCapability())
			{
				cleaningExecutor.execute(this::cleanUp);
			}
		});
	}
	
	private void resumeTracking(ManagedObjectBase managedObj) {
		this.manageExecutor.execute(()->{ // use single thread executor to avoid collision start stop tracking
			if (managedObj.containerCandle == null)
			{
				// put node to candle
				ManagedObjectQueue candle = null;
				try {
					candle = this.candlesPool.take();
				} catch (InterruptedException e) {
					return;
				}
		
				if (managedObj.containerCandle == null && managedObj.asyncCounter.get() > 0) 
				{
					candle.add(managedObj);
					managedObj.containerCandle = candle;
					
					this.usedSize.addAndGet(managedObj.estimatedSize);
				}
				
				this.candlesPool.offer(candle);
				if (this.isOverCapability())
				{
					cleaningExecutor.execute(this::cleanUp);
				}
			}
		});
	}
	
	private void removeFromManagement(ManagedObjectBase managedObj) {
		if (managedObj.containerCandle != null) { // use single thread executor to ensure non-collise start stop
			while(!this.candlesPool.remove(managedObj.containerCandle))
			{
				Thread.yield();
			}			
			
			KeyLock lock = managedObj.lockManage();
			ManagedObjectQueue candle = managedObj.containerCandle;
			managedObj.containerCandle.removeAt(managedObj.candleIndex);
			managedObj.containerCandle = null;
			this.usedSize.addAndGet(-managedObj.estimatedSize);
			lock.unlock();

			this.candlesPool.offer(candle);
		}
		
		this.keyToObjectMap.remove(managedObj.key);
		this.persistence.remove(managedObj.key);
	}
	
	private boolean isOverCapability()
	{
		return this.usedSize.get() > this.config.capacity;
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
			ManagedObjectBase coldestNode = null;
			for (ManagedObjectQueue candle : this.candles)
			{
				ManagedObjectBase node = candle.peek();
				if (node != null)
				{
					if (coldestNode == null || coldCacheNodeComparator.compare(coldestNode, node) > 0)
					{
						coldestNode = node;
					}
				}
			}
			
			if (coldestNode != null)
			{			
				ManagedObjectQueue coldestCandle = coldestNode.containerCandle;
				if (coldestCandle != null)
				{
					while(!this.candlesPool.remove(coldestCandle))
					{
						Thread.yield();
					}				
					
					if (coldestNode.containerCandle == coldestCandle)
					{
						if (coldestNode == coldestCandle.peek())
						{
							coldestNode = coldestCandle.poll();						
							coldestNode.containerCandle = null;	
							this.candlesPool.offer(coldestCandle);
							
							// coldestNode was removed from candles so, never duplicate persistence.
							final ManagedObjectBase persistedNode = coldestNode;
							this.persistence.store(coldestNode.key, coldestNode.serializer.serialize(coldestNode.object))
							.thenRunAsync(()->{					
								// synchronized to ensure retrieve never return null
								KeyLock keylock = persistedNode.lockManage();
								persistedNode.object = null;
								keylock.unlock();
								
								this.usedSize.addAndGet(-persistedNode.estimatedSize);
							}, this.cleaningExecutor)
							.whenComplete((o, e)->{
								this.waitingForPersistence = false;
								this.cleanUp();
							});
							
							return;
						}else {
							this.candlesPool.offer(coldestCandle);
						}
					}else {
						this.candlesPool.add(coldestCandle);
					}
				}
			}
		}
		
		this.waitingForPersistence = false;
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
	
	abstract class ManagedObjectBase
	{
		/***
		 * key value to lookup object, this is auto unique generated00
		 */
		UUID key;
		
		/**
		 * original object
		 */
		Object object;
		
		/**
		 * time object managed
		 */
		LocalTime startTime;
		
		/**
		 * time object expected to be retrieved for async, this is average from previous by keyflow
		 */
		LocalTime hotTime;
		
		/**
		 * estimated by serializer, size of object
		 */
		long estimatedSize;
		
		/**
		 * the candle contain this object, used for fast cleanup, removal
		 */
		ManagedObjectQueue containerCandle;
		
		/**
		 * the index of object in candle, used for fast removal
		 */
		int candleIndex;
		
		/**
		 * the serializer to ser/des object for persistence.
		 */
		BinarySerializerBase serializer;
		
		/**
		 * counting of setup flows, object start to be managed when all setup closed
		 */
		AtomicInteger setupCounter = new AtomicInteger(0);
		
		/**
		 * counting of async flows, object stop to be managed when all aync closed
		 */
		AtomicInteger asyncCounter = new AtomicInteger(0);
		
		/**
		 * start tracking object for optimize memory space. called when all setup flow closed
		 */
		void startTracking() {
			AsyncMemManager.this.startTracking(this);
		}
		
		/**
		 * resume tracking, called when all async flow reload object from persistence
		 */
		void resumeTracking() {
			AsyncMemManager.this.resumeTracking(this);
		}
		
		/**
		 * stop tracking, called when all async flow closed
		 */
		void stopTracking() {
			AsyncMemManager.this.removeFromManagement(this);
		}
	
		/**
		 * used for read/write locking this managed object. 
		 */
		private int accessCounter = 0;
		
		/**
		 * read locking, used for async flows access object, to ensure data not interfered
		 */
		ReadKeyLock lockRead()
		{
			while (true)
			{
				synchronized (this.key) {
					if (this.accessCounter <= 0)
					{
						return new ReadKeyLock();
					}
				}
				Thread.yield();
			}				
		}
		
		
		/**
		 * manage locking, used for cleanup, remove process, to ensure data not interfered 
		 */
		ManageKeyLock lockManage()
		{
			while (true)
			{
				synchronized (this.key) {
					if (this.accessCounter == 0)
					{
						return new ManageKeyLock();
					}
				}
				Thread.yield();
			}
		}
		
		/**
		 * key lock for read/manage.
		 */
		abstract class KeyLock {
			protected boolean unlocked = false;
			abstract int lockFactor();
			
			KeyLock()
			{
				ManagedObjectBase.this.accessCounter += this.lockFactor();
			}

			void unlock() {
				synchronized (ManagedObjectBase.this.key) {
					if (!this.unlocked)
					{
						this.unlocked = true;
						ManagedObjectBase.this.accessCounter -= this.lockFactor();
					}
				}
			}	
		}			
		
		/**
		 * Read key lock {@link ManagedObjectBase#lockRead()}
		 */
		class ReadKeyLock extends KeyLock
		{
			@Override
			int lockFactor() {
				return 1;
			}
			
			public ManageKeyLock upgradeToManageLock() {
				this.unlock();
				return ManagedObjectBase.this.lockManage();
			}
		}
		
		/**
		 * Manage key lock {@link ManagedObjectBase#lockManage()}
		 */
		class ManageKeyLock extends KeyLock
		{
			@Override
			int lockFactor() {
				return -1;
			}
			
			public ReadKeyLock downgradeReadKeyLock () {
				this.unlock();
				return ManagedObjectBase.this.lockRead();
			}
		}
	}
	
	/**
	 * Generic class for ManagedObject
	 */
	class ManagedObject<T> extends ManagedObjectBase
	{
		public ManagedObject(T obj)
		{
			this.object = obj;
			this.key = UUID.randomUUID();
		}
	}
	
	/**
	 * Object for async flow {@link ManagedObjectBase#asyncCounter}
	 */
	public class AsyncObject<T> implements AutoCloseable
	{
		ManagedObject<T> managedObject;
		AsyncObject(ManagedObject<T> managedObject) {
			this.managedObject = managedObject;			
			this.managedObject.asyncCounter.addAndGet(1);
		}
		
		@SuppressWarnings("unchecked")
		public CompletableFuture<T> o(){
			ReadKeyLock lock = this.managedObject.lockRead();
			CompletableFuture<T> res;
			if (this.managedObject.object != null)
			{
				res = CompletableFuture.completedFuture((T)this.managedObject.object);				
			}else {
				ManageKeyLock manageLock = lock.upgradeToManageLock();
				if (this.managedObject.object == null) 
				{
					this.managedObject.object = this.managedObject.serializer.deserialize(AsyncMemManager.this.persistence.retrieve(this.managedObject.key));					
					
				} 
				
				lock = manageLock.downgradeReadKeyLock();
				res = CompletableFuture.completedFuture((T)this.managedObject.object);
			}
			lock.unlock();
			return res;
		}
		
		@Override
		public void close() throws Exception {
			if (this.managedObject.asyncCounter.addAndGet(-1) == 0)
			{
				this.managedObject.stopTracking();
			}
		}		
	}
	
	/**
	 * Object for setup flow {@link ManagedObjectBase#setupCounter}
	 */	
	public class SetupObject<T> implements AutoCloseable
	{
		ManagedObject<T> managedObject;
		SetupObject(ManagedObject<T> managedObject) {
			this.managedObject = managedObject;
			this.managedObject.setupCounter.addAndGet(1);
		}
		
		public AsyncObject<T> asyncObject(){
			return new AsyncObject<T>(this.managedObject);
		}
		
		@SuppressWarnings("unchecked")
		public T o() {
			return (T) managedObject.object;
		}

		@Override
		public void close() throws Exception {
			if (this.managedObject.asyncCounter.get() > 0 
					&& this.managedObject.setupCounter.addAndGet(-1) == 0)
			{
				this.managedObject.startTracking();
			}
		}
	}
	
	/**
	 * Custom Queue implementation for candle queue of managed objects.
	 */
	private static class ManagedObjectQueue{
		private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
		
		ManagedObjectBase[] queue;
	    private int size = 0;
	    private final Comparator<ManagedObjectBase> comparator;
	    
	    public ManagedObjectQueue(int initSize, Comparator<ManagedObjectBase> comparator) {
	        this.queue = new ManagedObjectBase[initSize];
	        this.comparator = comparator;
	    }
	    
	    private void grow(int minCapacity) {
	        int oldCapacity = queue.length;
	        // Double size if small; else grow by 50%
	        int newCapacity = oldCapacity
	                + ((oldCapacity < 64) ? (oldCapacity + 2) : (oldCapacity >> 1));
	        // overflow-conscious code
	        if (newCapacity - MAX_ARRAY_SIZE > 0)
	            newCapacity = hugeCapacity(minCapacity);
	        queue = Arrays.copyOf(queue, newCapacity);
	    }
	    
	    private static int hugeCapacity(int minCapacity) {
	        if (minCapacity < 0) // overflow
	            throw new OutOfMemoryError();
	        return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
	    }
	    
	    public ManagedObjectBase peek() {
	        return (size == 0) ? null : queue[0];
	    }
	    
	    public ManagedObjectBase poll() {
	        if (size == 0)
	            return null;
	        int s = --size;
	        ManagedObjectBase result = queue[0];
	        ManagedObjectBase x = queue[s];
	        queue[s] = null;
	        if (s != 0)
	        	siftDownUsingComparator(0, x);
	        return result;
	    }

	    public boolean add(ManagedObjectBase e) {
	        if (e == null)
	            throw new NullPointerException();
	        int i = size;
	        if (i >= queue.length)
	            grow(i + 1);
	        size = i + 1;
	        if (i == 0)
	            queue[0] = e;
	        else
	        	siftUpUsingComparator(i, e);
	        return true;
	    }	    

	    private ManagedObjectBase removeAt(int i) {
	        // assert i >= 0 && i < size;
	        int s = --size;
	        if (s == i) // removed last element
	            queue[i] = null;
	        else {
	        	ManagedObjectBase moved = queue[s];
	            queue[s] = null;
	            siftDownUsingComparator(i, moved);
	            if (queue[i] == moved) {
	            	siftUpUsingComparator(i, moved);
	                if (queue[i] != moved)
	                    return moved;
	            }
	        }
	        return null;
	    }
	    
	    private void siftUpUsingComparator(int k, ManagedObjectBase x) {
	        while (k > 0) {
	            int parent = (k - 1) >>> 1;
	            ManagedObjectBase e = queue[parent];
	            if (comparator.compare(x, e) >= 0)
	                break;
	            queue[k] = e;
	            e.candleIndex = k;
	            k = parent;
	        }
	        queue[k] = x;
	        x.candleIndex = k;
	    }    
	    
	    private void siftDownUsingComparator(int k, ManagedObjectBase x) {
	        int half = size >>> 1;
	        while (k < half) {
	            int child = (k << 1) + 1;
	            ManagedObjectBase c = queue[child];
	            int right = child + 1;
	            if (right < size && comparator.compare(c, queue[right]) > 0)
	                c = queue[child = right];
	            if (comparator.compare(x, c) <= 0)
	                break;
	            queue[k] = c;
	            c.candleIndex = k;
	            k = child;
	        }
	        queue[k] = x;
	        x.candleIndex = k;
	    }
	    
	    public int size() 
	    {
	    	return this.size;
	    }
	}
}
