package asyncMemManager.common;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import asyncMemManager.common.AsyncMemManager.ManagedObjectBase.KeyLock;
import asyncMemManager.common.di.BinarySerializer;

public class AsyncMemManager implements asyncMemManager.common.di.AsyncMemManager {
	
	private Configuration config;
	private asyncMemManager.common.di.HotTimeCalculator coldTimeCalculator;
	private asyncMemManager.common.di.Persistence persistence;
	private BlockingQueue<ManagedObjectQueue> candlesPool;
	private List<ManagedObjectQueue> candles;
	private ConcurrentHashMap<UUID, ManagedObjectBase> keyToObjectMap;
	private long usedSize;
	private Object usedSizeKey = new Object(); 
	private Comparator<ManagedObjectBase> coldCacheNodeComparator = (n1, n2) -> n2.hotTime.compareTo(n1.hotTime);

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
			ManagedObjectQueue candle = new ManagedObjectQueue(initcandleSize, this.coldCacheNodeComparator);
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
		
		ManagedObject<T> managedObj = new ManagedObject<T>();
		managedObj.key = UUID.randomUUID();
		managedObj.object = object;
		managedObj.serializer = new BinarySerializerBase(serializer);
		managedObj.startTime = LocalTime.now();
		managedObj.estimatedSize = serializer.estimateObjectSize(object);
		
		long waitDuration = this.coldTimeCalculator.calculate(this.config, flowKey);
		managedObj.hotTime = managedObj.startTime.plus(waitDuration, ChronoField.MILLI_OF_SECOND.getBaseUnit());	
		return new SetupObject<T>(managedObj);
	}
	
	private void startTracking(ManagedObjectBase managedObj) {
		this.keyToObjectMap.put(managedObj.key, managedObj);		
		
		// put node to candle
		ManagedObjectQueue candle = null;
		try {
			candle = this.candlesPool.take();
		} catch (InterruptedException e) {
			return;
		}

		synchronized (candle) {
			candle.add(managedObj);
			managedObj.containerCandle = candle;						
		}		
		
		synchronized (this.usedSizeKey) {
			this.usedSize += managedObj.estimatedSize;
		}
		
		new RecursiveCleanup(() -> this.isOverCapability() ? this.cleanUp() : null).run();	
	}
	
	private void stopTracking(ManagedObjectBase managedObj) {		
		if (managedObj != null)
		{		
			if (managedObj.containerCandle != null) {
				while(!this.candlesPool.remove(managedObj.containerCandle))
				{
					Thread.yield();
				}			
				
				KeyLock lock = managedObj.lockManage();
				ManagedObjectQueue candle = managedObj.containerCandle;
				managedObj.containerCandle.removeAt(managedObj.candleIndex);
				managedObj.containerCandle = null;				
				lock.unlock();
	
				this.candlesPool.add(candle);
			}
			
			this.keyToObjectMap.remove(managedObj.key);
			this.persistence.remove(managedObj.key);
		}		
	}
	
	private boolean isOverCapability()
	{
		return this.usedSize > 0 && this.usedSize > this.config.capacity;
	}
	
	private CompletableFuture<Void> cleanUp()
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
			while(!this.candlesPool.remove(coldestNode.containerCandle))
			{
				Thread.yield();
			}				
			
			synchronized (coldestNode.containerCandle) {
				coldestNode = coldestNode.containerCandle.poll(); // it's ok if coldestNode may be updated to warmer by other thread.
				coldestNode.containerCandle = null;				
			}

			this.candlesPool.add(coldestNode.containerCandle);
			
			// coldestNode was removed from candles so, never duplicate persistence.
			if (coldestNode != null)
			{
				final ManagedObjectBase persistedNode = coldestNode;
				final CompletableFuture<Void> res = new CompletableFuture<>();
				this.persistence.store(coldestNode.key, coldestNode.serializer.serialize(coldestNode.object))
				.thenRun(()->{
					
					// synchronized to ensure retrieve never return null
					KeyLock keylock = persistedNode.lockManage();
					persistedNode.object = null;
					keylock.unlock();
					
					synchronized (this.usedSizeKey) {
						this.usedSize -= persistedNode.estimatedSize;
					}
					
					res.complete(null);
				});
				return res;
			}
		}
		
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
	
	abstract class ManagedObjectBase
	{
		UUID key;
		Object object;
		LocalTime startTime;
		LocalTime hotTime;
		long estimatedSize;
		ManagedObjectQueue containerCandle;
		int candleIndex;
		BinarySerializerBase serializer;
		
		AtomicInteger setupCounter = new AtomicInteger(0);
		AtomicInteger asyncCounter = new AtomicInteger(0);
		
		void startTracking() {
			AsyncMemManager.this.startTracking(this);
		}
		
		void stopTracking() {
			AsyncMemManager.this.stopTracking(this);
		}
	
		private int accessCounter = 0;
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
			
			@Override
			protected void finalize() throws Throwable {
				if (this.unlocked)
				{
					this.unlock();
				}
			}		
		}			
		
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
	
	class ManagedObject<T> extends ManagedObjectBase
	{
		@SuppressWarnings("unchecked")
		public T setup()
		{
			return (T)this.object;
		}
		
		@SuppressWarnings("unchecked")
		public CompletableFuture<T> async()
		{
			return  CompletableFuture.completedFuture((T)this.object);
		}
	}
	
	public class AsyncObject<T>  implements AutoCloseable
	{
		ManagedObject<T> managedObject;
		AsyncObject(ManagedObject<T> managedObject) {
			this.managedObject = managedObject;			
			this.managedObject.asyncCounter.addAndGet(1);
		}
		
		@Override
		public void close() throws Exception {
			if (this.managedObject.asyncCounter.addAndGet(-1) == 0)
			{
				this.managedObject.stopTracking();
			}
		}		
	}
	
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

		@Override
		public void close() throws Exception {
			if (this.managedObject.setupCounter.addAndGet(-1) == 0)
			{
				this.managedObject.startTracking();
			}
		}
	}	
	
	private static class RecursiveCleanup
	{
		private static RecursiveCleanup singleInstance = null; // only 1 cleanup action queued.
		private static synchronized boolean setSingleInstance(RecursiveCleanup newinstance)
		{
			if (singleInstance==null)
			{
				singleInstance = newinstance;
				return true;
			}
			return false;
		}
		
		Runnable runnable;
		RecursiveCleanup(Supplier<CompletableFuture<Void>> s)
		{
			if (s != null && setSingleInstance(this))
			{
				this.runnable = () -> {
					CompletableFuture<Void> f = s.get();
					if (f != null)
					{
						f.thenRun(this.runnable);
					}else {
						setSingleInstance(null);
					}
				};
			}
		}
		
		void run() 
		{
			if (this.runnable != null) {
				this.runnable.run();
			}
		}
	}
	
	private static class ManagedObjectQueue{
		private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
		
		ManagedObjectBase[] queue;
	    private int size = 0;
	    private final Comparator<ManagedObjectBase> comparator;
	    int modCount = 0;
	    
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
	        modCount++;
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
	        modCount++;
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
	        modCount++;
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
