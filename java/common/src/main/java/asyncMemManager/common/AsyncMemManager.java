package asyncMemManager.common;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Supplier;

import asyncMemManager.common.AsyncMemManager.ManagedObject.KeyLock;
import asyncMemManager.common.di.BinarySerializer;

public class AsyncMemManager implements asyncMemManager.common.di.AsyncMemManager {
	
	private Configuration config;
	private asyncMemManager.common.di.HotTimeCalculator coldTimeCalculator;
	private asyncMemManager.common.di.Persistence persistence;
	private BlockingQueue<Queue<ManagedObject>> candlesPool;
	private List<Queue<ManagedObject>> candles;
	private ConcurrentHashMap<UUID, ManagedObject> keyToObjectMap;
	private ConcurrentHashMap<Object, ManagedObject> objectManageMap;
	private long usedSize;
	private Object usedSizeKey = new Object(); 
	private Comparator<ManagedObject> coldCacheNodeComparator = (n1, n2) -> n2.hotTime.compareTo(n1.hotTime);

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
		this.objectManageMap = new ConcurrentHashMap<>(this.config.initialSize);
		this.candlesPool = new PriorityBlockingQueue<>(this.config.candlePoolSize, 
														(c1, c2) -> Integer.compare(c1.size(), c2.size()));
		this.candles = new ArrayList<>(this.config.candlePoolSize);
		
		// init candle pool
		for(int i = 0; i < config.candlePoolSize; i++)
		{
			Queue<ManagedObject> candle = new PriorityQueue<>(this.coldCacheNodeComparator);
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
	@SuppressWarnings("unchecked")
	@Override
	public <T> T manage(String flowKey, T object, BinarySerializer<T> serializer) 
	{
		// init key, mapKey, newnode
		if (object == null || Proxy.isProxyClass(object.getClass()))
		{
			return object;
		}
		
		ManagedObject managedObj = new ManagedObject();
		managedObj.key = UUID.randomUUID();
		managedObj.object = object;
		managedObj.serializer = new BinarySerializerBase(serializer);
		managedObj.startTime = LocalTime.now();
		managedObj.estimatedSize = serializer.estimateObjectSize(object);
		
		long waitDuration = this.coldTimeCalculator.calculate(this.config, flowKey);
		managedObj.hotTime = managedObj.startTime.plus(waitDuration, ChronoField.MILLI_OF_SECOND.getBaseUnit());
		
		this.keyToObjectMap.put(managedObj.key, managedObj);
		this.objectManageMap.put(object, managedObj);
		// put node to candle
		Queue<ManagedObject> candle = null;
		try {
			candle = this.candlesPool.take();
		} catch (InterruptedException e) {
			return null;
		}

		synchronized (candle) {
			candle.add(managedObj);
			managedObj.containerCandle = candle;						
		}		
		
		synchronized (this.usedSizeKey) {
			this.usedSize += managedObj.estimatedSize;
		}
		
		new RecursiveCompletableFuture(() -> this.isOverCapability() ? this.cleanUp() : null).run();		
		return (T) Proxy.newProxyInstance(object.getClass().getClassLoader(),
                new Class[] { object.getClass() },
                managedObj.new ManagedObjectProxyHandler());
	}
	
	private boolean isOverCapability()
	{
		return this.usedSize > 0 && this.usedSize > this.config.capacity;
	}
	
	private CompletableFuture<Void> cleanUp()
	{
		ManagedObject coldestNode = null;
		for (Queue<ManagedObject> candle : this.candles)
		{
			ManagedObject node = candle.peek();
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

			this.candlesPool.offer(coldestNode.containerCandle);
			
			// coldestNode was removed from candles so, never duplicate persistence.
			if (coldestNode != null)
			{
				final ManagedObject persistedNode = coldestNode;
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
	
	class ManagedObject
	{
		UUID key;
		Object object;
		LocalTime startTime;
		LocalTime hotTime;
		long estimatedSize;
		Queue<ManagedObject> containerCandle;
		BinarySerializerBase serializer;

		@Override
		protected void finalize() throws Throwable {
			// TODO Auto-generated method stub			
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
				ManagedObject.this.accessCounter += this.lockFactor();
			}

			void unlock() {
				synchronized (ManagedObject.this.key) {
					if (!this.unlocked)
					{
						this.unlocked = true;
						ManagedObject.this.accessCounter -= this.lockFactor();
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
				return ManagedObject.this.lockManage();
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
				return ManagedObject.this.lockRead();
			}
		}
		
		class ManagedObjectProxyHandler implements InvocationHandler
		{
			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
				ReadKeyLock keylock = ManagedObject.this.lockRead();
				
				if (ManagedObject.this.object == null) {
					ManageKeyLock manageLock = keylock.upgradeToManageLock();
					if(ManagedObject.this.object == null)
					{
						ManagedObject.this.object = AsyncMemManager.this.persistence.retrieve(ManagedObject.this.key);
					}
					
					keylock = manageLock.downgradeReadKeyLock();
				}
				
				Object res = method.invoke(object, args);
				keylock.unlock();
				return res;			
			}
		}		
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
