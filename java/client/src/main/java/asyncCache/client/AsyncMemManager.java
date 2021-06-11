package asyncCache.client;

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
import java.util.function.Function;

import asyncCache.client.AsyncMemManager.ManagedObjectBase.KeyLock;
import asyncCache.client.AsyncMemManager.ManagedObjectBase.ManageKeyLock;
import asyncCache.client.AsyncMemManager.ManagedObjectBase.ReadKeyLock;
import asyncMemManager.common.Configuration;
import asyncMemManager.common.ManagedObjectQueue;
import asyncMemManager.common.di.BinarySerializer;
import asyncMemManager.common.di.IndexableQueuedObject;

public class AsyncMemManager implements asyncCache.client.di.AsyncMemManager, AutoCloseable {
	
	private Configuration config;
	private asyncMemManager.common.di.HotTimeCalculator hotTimeCalculator;
	private asyncMemManager.common.di.Persistence persistence;
	private BlockingQueue<ManagedObjectQueue<ManagedObjectBase>> candlesPool;
	private List<ManagedObjectQueue<ManagedObjectBase>> candles;
	private ConcurrentHashMap<UUID, ManagedObjectBase> keyToObjectMap;
	private AtomicLong usedSize;
	private Comparator<ManagedObjectBase> cacheNodeComparator = (n1, n2) -> n2.hotTime.compareTo(n1.hotTime);

	//single threads to avoid collision, also, give priority to other flows
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
		this.hotTimeCalculator = coldTimeCalculator;
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
			ManagedObjectQueue<ManagedObjectBase> candle = new ManagedObjectQueue<>(initcandleSize, this.cacheNodeComparator); // thread-safe ensured by candlesPool
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
		
		long waitDuration = this.hotTimeCalculator.calculate(this.config, flowKey);
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
			ManagedObjectQueue<ManagedObjectBase> candle = null;
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
				ManagedObjectQueue<ManagedObjectBase> candle = null;
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
			ManagedObjectQueue<ManagedObjectBase> candle = managedObj.containerCandle;
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
			ManagedObjectBase coldestNode = null;
			for (ManagedObjectQueue<ManagedObjectBase> candle : this.candles)
			{
				ManagedObjectBase node = candle.peek();
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
				ManagedObjectQueue<ManagedObjectBase> coldestCandle = coldestNode.containerCandle;
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
						final ManagedObjectBase persistedNode = coldestNode;
						
						final KeyLock lock = persistedNode.lockManage();
						if (persistedNode.asyncCounter.get()>0)
						{
							this.persistence.store(coldestNode.key, coldestNode.serializer.serialize(coldestNode.object))
							.thenRunAsync(()->{					
								// synchronized to ensure retrieve never return null
								persistedNode.object = null;								
								this.usedSize.addAndGet(-persistedNode.estimatedSize);
							}, this.cleaningExecutor)
							.whenComplete((o, e)->{
								lock.unlock();
								this.waitingForPersistence = false;
								this.cleanUp();
							});
							
							this.candlesPool.offer(coldestCandle);
							return;
						}else {
							lock.unlock();
						}
					}
					
					this.candlesPool.add(coldestCandle);
				}
			}
		}
		
		this.waitingForPersistence = false;
	}
	
	abstract class ManagedObjectBase implements IndexableQueuedObject
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
		ManagedObjectQueue<ManagedObjectBase> containerCandle;
		
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
		
		@Override
		public void setIndexInQueue(int idx)
		{
			this.candleIndex = idx;
		}
		
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
		abstract class KeyLock implements AutoCloseable{
			protected boolean unlocked = false;
			abstract int lockFactor();
			protected KeyLock updownLock = null;
			
			KeyLock()
			{
				ManagedObjectBase.this.accessCounter += this.lockFactor();
			}

			void unlock() {
				if(updownLock != null)
				{
					this.updownLock.unlock();
				}else {
					synchronized (ManagedObjectBase.this.key) {
						if (!this.unlocked)
						{
							this.unlocked = true;
							ManagedObjectBase.this.accessCounter -= this.lockFactor();
						}
					}
				}
			}	
			
			@Override
			public void close() throws Exception {
				if (this.updownLock!=null)
				{
					this.updownLock.close();
				}else if ( !this.unlocked)
				{
					this.unlock();
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
			
			public void upgradeToManageLock() {
				this.unlock();
				this.updownLock = ManagedObjectBase.this.lockManage(); 
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
			
			public void downgradeReadKeyLock () {
				this.unlock();
				this.updownLock = ManagedObjectBase.this.lockRead();
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
			final ReadKeyLock lock = this.managedObject.lockRead();
			CompletableFuture<T> res;
			if (this.managedObject.object != null)
			{
				res = CompletableFuture
						.completedFuture((T)this.managedObject.object);				
			}else {
				lock.upgradeToManageLock();
				if (this.managedObject.object == null) 
				{
					this.managedObject.object = this.managedObject.serializer.deserialize(AsyncMemManager.this.persistence.retrieve(this.managedObject.key));					
					
				} 
				
				res = CompletableFuture
						.completedFuture((T)this.managedObject.object);
			}
			res.whenComplete((r, e) ->{ lock.unlock();});
			return res;
		}
		
		@SuppressWarnings("unchecked")
		public <R> R a(Function<T,R>f) throws Exception {
			try(ReadKeyLock lock = this.managedObject.lockRead()){
				if (this.managedObject.object == null)
				{
					lock.upgradeToManageLock();
					if (this.managedObject.object == null) 
					{
						this.managedObject.object = this.managedObject.serializer.deserialize(AsyncMemManager.this.persistence.retrieve(this.managedObject.key));
					}
				}
				
				R res = f.apply((T)this.managedObject.object);
				lock.unlock();
				return res;
			}
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
}
