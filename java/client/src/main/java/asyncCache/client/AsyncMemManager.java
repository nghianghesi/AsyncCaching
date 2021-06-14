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
import asyncMemManager.common.Configuration;
import asyncMemManager.common.ManagedObjectQueue;
import asyncMemManager.common.ReadWriteLock;
import asyncMemManager.common.ReadWriteLock.ReadWriteLockableObject;
import asyncMemManager.common.di.BinarySerializer;
import asyncMemManager.common.di.IndexableQueuedObject;

public class AsyncMemManager implements asyncCache.client.di.AsyncMemManager, AutoCloseable {
	
	// this is for special marker only.
	static private ManagedObjectQueue<ManagedObjectBase> queuedForManageCandle = new ManagedObjectQueue<AsyncMemManager.ManagedObjectBase>(0, null);
	
	private Configuration config;
	private asyncMemManager.common.di.HotTimeCalculator hotTimeCalculator;
	private asyncMemManager.common.di.Persistence persistence;
	private BlockingQueue<ManagedObjectQueue<ManagedObjectBase>> candlesPool;	
	private List<ManagedObjectQueue<ManagedObjectBase>> candles;
	private Set<UUID> cachedKeys;
	private AtomicLong usedSize;
	private Comparator<ManagedObjectBase> cacheNodeComparator = (n1, n2) -> n2.hotTime.compareTo(n1.hotTime);

	//single threads to avoid collision, also, give priority to other flows
	private ExecutorService manageExecutor;
	
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
		this.cachedKeys = ConcurrentHashMap.newKeySet(this.config.getInitialSize());
		this.candlesPool = new PriorityBlockingQueue<>(this.config.getCandlePoolSize(), 
														(c1, c2) -> Integer.compare(c1.size(), c2.size()));
		this.candles = new ArrayList<>(this.config.getCandlePoolSize());
		
		int numberOfManagementThread = this.config.getCandlePoolSize() / 2;
		numberOfManagementThread = numberOfManagementThread > 0 ? numberOfManagementThread : 1;
		this.manageExecutor = Executors.newFixedThreadPool(numberOfManagementThread + 1);
		
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
		
		BinarySerializerBase baseSerializer = BinarySerializerBase.getBinarySerializerBaseInstance(serializer);
		LocalTime startTime = LocalTime.now();
		long estimatedSize = serializer.estimateObjectSize(object);
		
		ManagedObject<T> managedObj = new ManagedObject<>(object, startTime, estimatedSize, baseSerializer);
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
		
		this.manageExecutor.shutdown();

		for (UUID cachedKey: this.cachedKeys)
		{
			this.persistence.remove(cachedKey);
		}
		
		this.cachedKeys.clear();
	}
	
	private void startTracking(ManagedObjectBase managedObj) {
		if (managedObj.getManagementState() == ManagementState.None)
		{
			synchronized (managedObj) {	// to ensure only one manage action queued for this managedObj
				if (managedObj.getManagementState() == ManagementState.None)
				{			
					managedObj.setManagementState(AsyncMemManager.queuedForManageCandle);
					this.manageExecutor.execute(()->{
						
						// put node to candle
						ManagedObjectQueue<ManagedObjectBase> candle = null;
						try {
							candle = this.candlesPool.take();
						} catch (InterruptedException e) {
							return;
						}
				
						candle.add(managedObj);
						managedObj.setManagementState(candle);
						
						this.usedSize.addAndGet(managedObj.estimatedSize);
						
						this.candlesPool.offer(candle);
						
						this.queueCleanUp();
					});
				}
			}
		}
	}
	
	private void resumeTracking(ManagedObjectBase managedObj) {
		if (managedObj.getManagementState() == ManagementState.None)
		{
			synchronized (managedObj) {	// to ensure only one manage action queued for this managedObj
				if (managedObj.getManagementState() == ManagementState.None)
				{				
					managedObj.setManagementState(AsyncMemManager.queuedForManageCandle);
					this.manageExecutor.execute(()->{ 
						if (managedObj.getManagementState() == ManagementState.Queued)
						{
							// put node to candle
							ManagedObjectQueue<ManagedObjectBase> candle = null;
							try {
								candle = this.candlesPool.take();
							} catch (InterruptedException e) {
								return;
							}
					
							if (managedObj.asyncCounter.get() > 0)  // check again toensure nothing changed.
							{
								candle.add(managedObj);
								managedObj.setManagementState(candle);
								
								this.usedSize.addAndGet(managedObj.estimatedSize);
							}
							
							this.candlesPool.offer(candle);
							this.queueCleanUp();
						}
					});
				}
			}
		}
	}
	
	private void removeFromManagement(ManagedObjectBase managedObj) {
		if (managedObj.getManagementState() == ManagementState.Managed) { 
			synchronized (managedObj) { // to ensure only one manage action queued for this managedObj
				if (managedObj.getManagementState() == ManagementState.Managed) { 
					final ManagedObjectQueue<ManagedObjectBase> containerCandle = managedObj.setManagementState(AsyncMemManager.queuedForManageCandle);
					
					this.manageExecutor.execute(()->{
						while(!this.candlesPool.remove(containerCandle))
						{
							Thread.yield();
						}
						
						containerCandle.removeAt(managedObj.candleIndex);
						managedObj.setManagementState(null);
						this.usedSize.addAndGet(-managedObj.estimatedSize);
		
						if (this.cachedKeys.remove(managedObj.key)) 
						{		
							this.persistence.remove(managedObj.key);
						}

						this.candlesPool.offer(containerCandle);
					});
				}
			}
		}
	}
	
	private boolean isOverCapability()
	{
		return this.usedSize.get() > this.config.getCapacity();
	}
	
	// these 2 values to ensure only 1 cleanup queued.
	private volatile Object queueCleanupKey = new Object(); 
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
	
	/**
	 * this is expected to be run in manage executor, by queueCleanUp
	 */
	private void cleanUp()
	{
		while (this.isOverCapability())
		{
			ManagedObjectQueue<ManagedObjectBase>.PollCandidate coldestCandidate = null;
			for (ManagedObjectQueue<ManagedObjectBase> candle : this.candles)
			{
				ManagedObjectQueue<ManagedObjectBase>.PollCandidate node = candle.getPollCandidate();
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
				final ManagedObjectQueue<ManagedObjectBase>.PollCandidate coldestCandidateFinal = coldestCandidate;
				final ManagedObjectBase coldestObj = coldestCandidate.getObject();
				
				if (coldestObj.getManagementState() == ManagementState.Managed) {
					synchronized (coldestObj) {
						if (coldestObj.getManagementState() == ManagementState.Managed) // check again to ensure only 1 manage action queued for this object.
						{
							final ManagedObjectQueue<ManagedObjectBase> coldestCandle = coldestObj.setManagementState(AsyncMemManager.queuedForManageCandle);						
							
							this.manageExecutor.execute(()->{						
								while(!this.candlesPool.remove(coldestCandle))
								{
									Thread.yield();
								}
								
								ManagedObjectBase removedObj = coldestCandle.removeAt(coldestCandidateFinal.getIdx());						
								if (coldestObj == removedObj)  // check again to ensure nothing changed by other thread
								{						
									coldestObj.setManagementState(null);	
									this.candlesPool.offer(coldestCandle);
									
									final ReadWriteLock<ManagedObjectBase> lock = coldestObj.lockManage();
									if (coldestObj.asyncCounter.get()>0)
									{
										this.persistence.store(coldestObj.key, coldestObj.serializer.serialize(coldestObj.object))
										.thenRunAsync(()->{					
											// synchronized to ensure retrieve never return null
											coldestObj.object = null;								
											this.usedSize.addAndGet(-coldestObj.estimatedSize);
											this.cachedKeys.add(coldestObj.key);
		
											lock.unlock();
											this.waitingForPersistence = false;
											this.cleanUp();
										}, this.manageExecutor);
										
										return;
									}else {
										// unlock if not processing
										lock.unlock();
									}
								}else {
									// add back if not processing
									coldestCandle.add(removedObj);
								}
							});
							
							return; // done with this clean up, other cleanup will be called inside manage executor.
						}
					}
				}
			}
			
			Thread.yield();
		}		
		this.waitingForPersistence = false;
	}
	
	abstract class ManagedObjectBase implements IndexableQueuedObject, ReadWriteLockableObject
	{
		/***
		 * key value to lookup object, this is auto unique generated00
		 */
		final UUID key;
		
		/**
		 * original object
		 */
		Object object;
		
		/**
		 * time object managed
		 */
		final LocalTime startTime;
		
		/**
		 * time object expected to be retrieved for async, this is average from previous by keyflow
		 */
		LocalTime hotTime;
		
		/**
		 * estimated by serializer, size of object
		 */
		final long estimatedSize;
		
		/**
		 * the candle contain this object, used for fast cleanup, removal
		 */
		private ManagedObjectQueue<ManagedObjectBase> containerCandle;
		
		/**
		 * the index of object in candle, used for fast removal
		 */
		int candleIndex;
		
		/**
		 * the serializer to ser/des object for persistence.
		 */
		final BinarySerializerBase serializer;	

		/**
		 * init  ManagedObject 
		 */
		public ManagedObjectBase(LocalTime startTime, long estimatedSize, BinarySerializerBase serializer) {
			this.key = UUID.randomUUID();
			this.startTime = startTime;
			this.estimatedSize = estimatedSize;
			this.serializer = serializer;
		}

		/**
		 * counting of setup flows, object start to be managed when all setup closed
		 */
		final AtomicInteger setupCounter = new AtomicInteger(0);
		
		/**
		 * counting of async flows, object stop to be managed when all aync closed
		 */
		final AtomicInteger asyncCounter = new AtomicInteger(0);
		
		/**
		 * get management state to have associated action.
		 * this is for roughly estimate, as not ensured thread-safe.
		 */
		ManagementState getManagementState()
		{
			if (this.containerCandle == null)
			{
				return ManagementState.None;
			}else if (this.containerCandle == AsyncMemManager.queuedForManageCandle){
				return ManagementState.Queued;
			}else {
				return ManagementState.Managed;
			}
		}
		
		/**
		 * return previous containerCandel
		 */
		ManagedObjectQueue<ManagedObjectBase> setManagementState(ManagedObjectQueue<ManagedObjectBase> containerCandle)
		{
			ManagedObjectQueue<ManagedObjectBase> prev = this.containerCandle;
			this.containerCandle = containerCandle;
			return prev;
		}
				
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
		
		@Override
		public boolean isPeekable() {
			return this.accessCounter == 0 && this.getManagementState() == ManagementState.Managed;
		}
		
		/**
		 * read locking, used for async flows access object, to ensure data not interfered
		 */
		ReadWriteLock<ManagedObjectBase> lockRead()
		{
			return new ReadWriteLock.ReadLock<ManagedObjectBase>(this);
		}		
		
		/**
		 * manage locking, used for cleanup, remove process, to ensure data not interfered 
		 */
		ReadWriteLock<ManagedObjectBase> lockManage()
		{
			return new ReadWriteLock.WriteLock<ManagedObjectBase>(this);
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
	
	static enum ManagementState
	{
		None,
		Queued,
		Managed,
	}
	
	/**
	 * Generic class for ManagedObject
	 */
	class ManagedObject<T> extends ManagedObjectBase
	{
		ManagedObject(T obj, LocalTime startTime, long estimatedSize, BinarySerializerBase serializer)
		{
			super(startTime, estimatedSize, serializer);
			this.object = obj;
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
		
		/**
		 * provide original object asynchronously.
		 */
		@SuppressWarnings("unchecked")
		public <R> CompletableFuture<R> async(Function<T,R> f){
			final ReadWriteLock<ManagedObjectBase> lock = this.managedObject.lockRead();
			CompletableFuture<T> res;
			if (this.managedObject.object != null)
			{
				res = CompletableFuture
						.completedFuture((T)this.managedObject.object);				
			}else {
				ReadWriteLock<ManagedObjectBase> manageLock = lock.upgrade();
				if (this.managedObject.object == null) 
				{
					this.managedObject.object = this.managedObject.serializer.deserialize(AsyncMemManager.this.persistence.retrieve(this.managedObject.key));					
					
				} 
				manageLock.downgrade();
				res = CompletableFuture
						.completedFuture((T)this.managedObject.object);
			}
			return res.thenApplyAsync((o) -> f.apply(o)).whenComplete((r, e) -> lock.unlock());
		}
		
		/**
		 * run method provided by caller synchronously  
		 */
		@SuppressWarnings("unchecked")
		public <R> R apply(Function<T,R> f) throws Exception {
			try(ReadWriteLock<ManagedObjectBase> lock = this.managedObject.lockRead()){
				if (this.managedObject.object == null)
				{
					ReadWriteLock<ManagedObjectBase> manageLock = lock.upgrade();
					if (this.managedObject.object == null) 
					{
						this.managedObject.object = this.managedObject.serializer.deserialize(AsyncMemManager.this.persistence.retrieve(this.managedObject.key));
					}
					manageLock.downgrade();
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
