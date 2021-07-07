namespace AsyncMemManager.Client
{
    using DI;
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Text;

    using AsyncMemManager.Common;

    public class AsyncMemManagerImpl : IAsyncMemManager
    {        
        // this is for special marker only.
        private static readonly ManagedObjectQueue<ManagedObjectBase> queuedForManageCandle = new ManagedObjectQueue<ManagedObjectBase>(0, null);
        private static readonly ManagedObjectQueue<ManagedObjectBase> obsoletedManageCandle = new ManagedObjectQueue<ManagedObjectBase>(0, null);

        private Configuration config;
        private IHotTimeCalculator hotTimeCalculator;
        private IPersistence persistence;
        private LinkedList<ManagedObjectQueue<ManagedObjectBase>> candlesPool;	
        private List<ManagedObjectQueue<ManagedObjectBase>> candlesSrc;
        private long usedSize = 0;
        private readonly IComparer<ManagedObjectBase> cacheNodeComparator = new CandleComparer();


        public AsyncMemManagerImpl(Configuration config,
                                    IHotTimeCalculator coldTimeCalculator, 
                                    IPersistence persistence) 
        {
            this.config = config;
            this.hotTimeCalculator = coldTimeCalculator;
            this.persistence = persistence;
            this.candlesPool = new LinkedList<ManagedObjectQueue<ManagedObjectBase>>();
            this.candlesSrc = new List<ManagedObjectQueue<ManagedObjectBase>>(this.config.CandlePoolSize);
            
            int numberOfManagementThread = this.config.CandlePoolSize;
            numberOfManagementThread = numberOfManagementThread > 0 ? numberOfManagementThread : 1;
            
            int initcandleSize = this.config.InitialSize / this.config.CandlePoolSize;
            initcandleSize = initcandleSize > 0 ? initcandleSize : this.config.InitialSize;
            
            // init candle pool
            for(int i = 0; i < config.CandlePoolSize; i++)
            {
                ManagedObjectQueue<ManagedObjectBase> candle = new ManagedObjectQueue<ManagedObjectBase>(initcandleSize, this.cacheNodeComparator); // thread-safe ensured by candlesPool
                this.candlesPool.AddFirst(candle);
                this.candlesSrc.Add(candle);
            }
        }

        public ISetupObject<T> Manage<T>(string flowKey, T obj, IAsyncMemSerializer<T> serializer) where T : class
        {
            		// init key, mapKey, newnode
            if (obj == null)
            {
                return null;
            }
            
            SerializerGeneral baseSerializer = SerializerGeneral.GetSerializerBaseInstance(serializer);
            long estimatedSize = serializer.EstimateObjectSize(obj);
            
            ManagedObject<T> managedObj = new ManagedObject<T>(flowKey, obj,  estimatedSize, baseSerializer);
            
            return new SetupObject<T>(this, managedObj);
        }  
        
        public string DebugInfo()
        {            
            StringBuilder res = new StringBuilder();
            res.Append("Used:"); res.Append(this.usedSize);
            
            long countItems = 0;
            foreach(ManagedObjectQueue<ManagedObjectBase> queue in this.candlesSrc)
            {
                countItems += queue.Size;
            }

            res.Append(" Items:"); res.Append(countItems);
            return res.ToString();
        }        

        public void Dispose(){
            lock (this.candlesPool)
            {
                this.candlesPool.Clear();
            }
		
            foreach (ManagedObjectQueue<ManagedObjectBase> candle in this.candlesSrc) {
                while (candle.Size > 0)
                {
                    ManagedObjectBase managedObj = candle.GetAndRemoveAt(candle.Size - 1);		
                    Interlocked.Add(ref this.usedSize, -managedObj.estimatedSize);	
                    this.persistence.Remove(managedObj.key);
                }
            }
        } 
	
        private void Track(ManagedObjectBase managedObj) {		
            this.DoManageAction(managedObj, ManagementState.None | ManagementState.Managing, (containerCandle) => {
                    
                long nextwaitDuration = this.hotTimeCalculator.Calculate(this.config, managedObj.flowKey, managedObj.numberOfAccess);
                managedObj.hotTime = managedObj.startTime.AddMilliseconds(nextwaitDuration);	
                bool needcheckRemove = true;
                if (containerCandle == null) // unmanaged, probably none or cached.
                {
                    // put node to candle	
                    
                    bool shouldTracking = true;
                    if (this.usedSize + managedObj.estimatedSize > this.config.Capacity) {
                        ManagedObjectBase coldestNode = this.GetColdestCandidate();
                        
                        if (coldestNode != null && this.cacheNodeComparator.Compare(managedObj, coldestNode) > 0)
                        {
                            // storing to cache to reserve space for new object.
                            this.DoManageAction(coldestNode, ManagementState.Managing, 
                                    (ManagedObjectQueue<ManagedObjectBase> coldestCandle) => { 
                                        this.Cache(coldestCandle, coldestNode); 
                                    });
                        }else {						 
                            this.PersistObject(managedObj);
                            managedObj.SetManagementState(null);
                            shouldTracking = false;
                            needcheckRemove = false;
                        }
                    }
                    
                    if (shouldTracking)
                    {
                        ManagedObjectQueue<ManagedObjectBase> candle = null;
                        candle = this.PollCandle();
                
                        if (!managedObj.IsObsoleted()) {
                            candle.Add(managedObj);
                            Interlocked.Add(ref this.usedSize, managedObj.estimatedSize);
                            managedObj.SetManagementState(candle);					
                        }else {
                            needcheckRemove = false;
                            managedObj.SetManagementState(null);
                        }				
                        
                        this.OfferCandleBackAfterUsed(candle);
                    }
                } else {
                    this.PollCandle(containerCandle);
                    
                    managedObj.hotTime = managedObj.hotTime.AddMilliseconds(nextwaitDuration);
                    if (!managedObj.IsObsoleted()) {
                        containerCandle.SyncPriorityAt(managedObj.indexInCandle);
                    }							
                    managedObj.SetManagementState(containerCandle); // restore management state --> unlock other queueing
                    
                    this.OfferCandleBackAfterUsed(containerCandle);
                }			

                if (needcheckRemove && managedObj.IsObsoleted()) { // to void other remove failed to be queued while this action running.
                    this.RemoveFromManagement(managedObj);
                }			

                this.CleanUp();
            });
        }
	
        private void RemoveFromManagement(ManagedObjectBase managedObj) {
            this.DoManageAction(managedObj, ManagementState.Managing, (ManagedObjectQueue<ManagedObjectBase> containerCandle) => {
                this.PollCandle(containerCandle);
                
                containerCandle.GetAndRemoveAt(managedObj.indexInCandle);
                Interlocked.Add(ref this.usedSize, -managedObj.estimatedSize);							
                managedObj.SetManagementState(AsyncMemManagerImpl.obsoletedManageCandle);
                    
                this.OfferCandleBackAfterClean(containerCandle);
            });
        }	
	             
	
        /**
        * execute manage action for managedObj, ensure only one action queued per object, bypass this request if other action queued.
        */
        private bool DoManageAction(ManagedObjectBase managedObj, ManagementState expectedCurrentState, Action<ManagedObjectQueue<ManagedObjectBase>> action)	
        {
            bool queued = false;
            ManagedObjectQueue<ManagedObjectBase> containerCandle = null;
            ManagementState state = managedObj.ManagementState;
            if ((expectedCurrentState & state) > 0) { 
                lock (managedObj) { 
                    if (state == managedObj.ManagementState) // state unchanged. 
                    { 
                        containerCandle = managedObj.SetManagementState(AsyncMemManagerImpl.queuedForManageCandle);
                        queued = true;
                    }
                }
                
                if(queued)
                {
                    action(containerCandle);
                }
            }
            
            return queued;
        }
        
        private bool IsOverCapability()
        {
            return this.usedSize > this.config.Capacity;
        }
        
        private ManagedObjectQueue<ManagedObjectBase>  PollCandle(){
            ManagedObjectQueue<ManagedObjectBase> res = null;
            
            while (res == null)
            {
                lock(this.candlesPool){                                                            
                    if(this.candlesPool.Any())
                    {
                        res = this.candlesPool.First();
                        this.candlesPool.RemoveFirst();
                    }
                }

                if(res == null)
                {
                    Thread.Yield();
                }
            }

            return res;
        }
        
        private ManagedObjectQueue<ManagedObjectBase>  PollCandle(ManagedObjectQueue<ManagedObjectBase> containerCandle)
        {
            bool removed = false;
            while (!removed)
            {
                lock(this.candlesPool)
                {
                    removed = this.candlesPool.Remove(containerCandle);
                }

                if (!removed)
                {
                    Thread.Yield();
                }
            }
            return containerCandle;
        }

        private void OfferCandleBackAfterClean(ManagedObjectQueue<ManagedObjectBase> containerCandle)
        {
            lock(this.candlesPool)
            {
                this.candlesPool.AddFirst(containerCandle);
            }
        }

        private void OfferCandleBackAfterUsed(ManagedObjectQueue<ManagedObjectBase> containerCandle)
        {
            lock(this.candlesPool)
            {
                this.candlesPool.AddLast(containerCandle);
            }
        }
        
        private ManagedObjectBase GetColdestCandidate()
        {			
            ManagedObjectBase coldestCandidate = null;
            foreach (ManagedObjectQueue<ManagedObjectBase> candle in this.candlesSrc)
            {
                ManagedObjectBase node = candle.GetPollCandidate();
                if (node != null)
                {
                    if (coldestCandidate == null || cacheNodeComparator.Compare(coldestCandidate, node) > 0)
                    {
                        coldestCandidate = node;
                    }
                }
            }
            
            return coldestCandidate;
        }
	
        private void PersistObject(ManagedObjectBase managedObject)
        {
            if (managedObject.asyncCounter > 0)
            {
                ReadWriteLock<ManagedObjectBase> locker = managedObject.LockManage();
                if(managedObject.obj != null && !managedObject.IsObsoleted())
                {
                    long expectedDuration = (long)(managedObject.hotTime - DateTime.Now).TotalMilliseconds;
                    this.persistence.Store(managedObject.key, managedObject.serializer.Serialize(managedObject.obj), expectedDuration);
                    managedObject.obj = null;
                }

                locker.Unlock();			
            }
        }
        
        /*
        * need containerCandle as managedObject's containerCandle may be marked as queued.
        */
        private void Cache(ManagedObjectQueue<ManagedObjectBase> containerCandle, ManagedObjectBase managedObject) {
            this.PollCandle(containerCandle);
            
            containerCandle.GetAndRemoveAt(managedObject.indexInCandle);
            Interlocked.Add(ref this.usedSize, -managedObject.estimatedSize);
            this.PersistObject(managedObject);
            managedObject.SetManagementState(null);
                    
            this.OfferCandleBackAfterClean(containerCandle);
        }
        /**
        * this is expected to be run in manage executor, by queueCleanUp
        */
        private void CleanUp()
        {
            while (this.IsOverCapability())
            {
                bool isReduced = false;
                // find the coldest candidate
                ManagedObjectBase coldestObject = this.GetColdestCandidate();
                
                // candidate founded
                if (coldestObject != null)
                {							
                    isReduced = this.DoManageAction(coldestObject, ManagementState.Managing, 
                            (ManagedObjectQueue<ManagedObjectBase> coldestCandle) => {
                                this.Cache(coldestCandle, coldestObject);
                        });
                }
                
                if (!isReduced)
                {
                    Thread.Yield();
                }
            }
        }

        private class AsyncMemManagerObjectContainer
        {
            protected readonly AsyncMemManagerImpl manager;
            protected readonly ManagedObjectBase managedObj;
            protected AsyncMemManagerObjectContainer(AsyncMemManagerImpl m, ManagedObjectBase o)
            {
                this.manager = m;
                this.managedObj = o;
            }
        }
	
        private class SetupObject<T> : AsyncMemManagerObjectContainer, ISetupObject<T> where T : class
        {            
            public SetupObject(AsyncMemManagerImpl manager, ManagedObjectBase obj) : base (manager, obj){
            }

            public IAsyncObject<T> AsyncO()
            {                
                return new AsyncObject<T>(this.manager, managedObj);
            }

            public T O()
            {                
			    return (T) this.managedObj.obj;
            }

            public void Close(){
                this.managedObj.doneSetup = true;
                if (this.managedObj.asyncCounter > 0)
                {
                    this.manager.Track(this.managedObj);
                }
            }

            public void Dispose(){                
                this.Close();
            }
        }
        
        private class AsyncObject<T> : AsyncMemManagerObjectContainer, IAsyncObject<T> where T : class
        {		           
            public AsyncObject(AsyncMemManagerImpl manager, ManagedObjectBase obj) : base (manager, obj){
                Interlocked.Increment(ref this.managedObj.asyncCounter);
            }

            public R Supply<R>(Func<T,R> f)
            {
         		ReadWriteLock<ManagedObjectBase> locker = this.managedObj.LockRead();
                this.LoadFromStoreIfNeeded(locker);
                R res = f(this.managedObj.obj as T);
                locker.Unlock();			
                this.TrackIfNeeded();
                return res;
            }

            public void Apply(Action<T> f)
            {
                ReadWriteLock<ManagedObjectBase> locker = this.managedObj.LockRead();
                this.LoadFromStoreIfNeeded(locker);
                f(this.managedObj.obj as T);
                locker.Unlock();			
                this.TrackIfNeeded();
            }

            public void Close()
            {
                if (Interlocked.Decrement(ref this.managedObj.asyncCounter) == 0 && this.managedObj.doneSetup)
                {
                    this.manager.RemoveFromManagement(this.managedObj);
                }
            }

            public void Dispose(){
                this.Close();
            }
		
            private void LoadFromStoreIfNeeded(ReadWriteLock<ManagedObjectBase> currentReadlock) {
                if (this.managedObj.obj == null)
                {
                    ReadWriteLock<ManagedObjectBase> manageLock = currentReadlock.Upgrade();
                    if (this.managedObj.obj == null) 
                    {
                        this.managedObj.obj = this.managedObj.serializer.Deserialize<T>(this.manager.persistence.Retrieve(this.managedObj.key));
                    }
                    
                    manageLock.Downgrade();
                } 
                
                DateTime now = DateTime.Now;
                long waittime = (long)(now - this.managedObj.startTime).TotalMilliseconds;
                this.manager.hotTimeCalculator.Stats(this.manager.config, this.managedObj.flowKey, this.managedObj.numberOfAccess, waittime);
                this.managedObj.startTime = now;
                this.managedObj.numberOfAccess++;
            }
            
            private void TrackIfNeeded() {
                if (this.managedObj.asyncCounter > 0 && this.managedObj.doneSetup)
                {
                    this.manager.Track(this.managedObj);
                }
            }            
        }
        
        private abstract class ManagedObjectBase : IndexableQueuedObject, IReadWriteLockableObject
        {
            /***
            * key value to lookup object, this is auto unique generated
            * also used as key for synchronize access vs management
            */
            internal readonly Guid key;
            internal readonly object keyLocker = new object();
            
            /**
            * flow key, this is used for estimate waiting time
            */
            internal readonly string flowKey;
            
            /**
            * original object
            */
            internal volatile object obj;
            
            /**
            * time object managed
            */
            internal DateTime startTime;
            
            /**
            * time object expected to be retrieved for async, this is average from previous by keyflow
            */
            internal DateTime hotTime;
            
            /**
            * estimated by serializer, size of object
            */
            internal readonly long estimatedSize;
            
            /**
            * the candle contain this object, used for fast cleanup, removal
            */
            private volatile ManagedObjectQueue<ManagedObjectBase> containerCandle;
            
            /**
            * the index of object in candle, used for fast removal
            */
            internal volatile int indexInCandle = -1;
            
            internal volatile int numberOfAccess = 0;
            
            /**
            * the serializer to ser/des object for persistence.
            */
            internal readonly SerializerGeneral serializer;

            /**
            * init  ManagedObject 
            */
            public ManagedObjectBase(string flowKey, long estimatedSize, SerializerGeneral serializer) {
                this.flowKey = flowKey;
                this.key = Guid.NewGuid();
                this.startTime = this.hotTime = DateTime.Now;
                this.estimatedSize = estimatedSize;
                this.serializer = serializer;
            }

            /**
            * if object still being setup. object start to be managed setup closed
            */
            internal volatile bool doneSetup = false;
            
            /**
            * counting of async flows, object stop to be managed when all aync closed
            */
            internal volatile int asyncCounter = 0;
            
            internal bool IsObsoleted() {
                return this.doneSetup && this.asyncCounter == 0;
            }
            
            /**
            * get management state to have associated action.
            * this is for roughly estimate, as not ensured thread-safe.
            */
            internal ManagementState ManagementState
            {
                get{
                    lock (this) {
                        ManagedObjectQueue<ManagedObjectBase> c = this.containerCandle;  
                        if (c == null)
                        {
                            return ManagementState.None;
                        }else if (c == AsyncMemManagerImpl.queuedForManageCandle){
                            return ManagementState.Queued;
                        }else if (c == AsyncMemManagerImpl.obsoletedManageCandle){
                            return ManagementState.Obsoleted;
                        }else {
                            return ManagementState.Managing;
                        }
                    }
                }
            }
            
            /**
            * return previous containerCandel
            */
            internal ManagedObjectQueue<ManagedObjectBase> SetManagementState(ManagedObjectQueue<ManagedObjectBase> containerCandle)
            {
                lock (this) {
                    ManagedObjectQueue<ManagedObjectBase> prev = this.containerCandle;
                    this.containerCandle = containerCandle;
                    return prev;				
                }
            }
        
            /**
            * used for read/write locking this managed object. 
            */
            private volatile int readWriteCounter = 0;
            
            public void SetIndexInQueue(int idx)
            {
                this.indexInCandle = idx;
            }
            
            /**
            * whether this object is available for cleanup
            */
            public bool IsPeekable() {
                return this.readWriteCounter == 0 && this.ManagementState == ManagementState.Managing && this.indexInCandle >= 0;
            }
            
            /**
            * read locking, used for async flows access object, to ensure data not interfered
            */
            internal ReadWriteLock<ManagedObjectBase> LockRead()
            {
                return new ReadWriteLock<ManagedObjectBase>.ReadLock(this);
            }
            
            /**
            * manage locking, used for cleanup, remove process, to ensure data not interfered 
            */
            internal ReadWriteLock<ManagedObjectBase> LockManage()
            {
                return new ReadWriteLock<ManagedObjectBase>.WriteLock(this);
            }
            
            public int LockStatus => this.readWriteCounter;
            
            public void AddLockFactor(int lockfactor) {
                this.readWriteCounter += lockfactor;
            }
            
            public object LockerKey => this.keyLocker;
        }
        
        [Flags]
        private enum ManagementState
        {
            None = 1,
            Queued = 0,
            Managing = 2,
            Obsoleted = 4
        }
	
	    /**
	        * Generic class for ManagedObject
        */
        private class ManagedObject<T> : ManagedObjectBase
        {
            public ManagedObject(string flowKey, T obj, long estimatedSize, SerializerGeneral serializer)
                : base(flowKey, estimatedSize, serializer)
            {                
                this.obj = obj;
            }
        } 

        private class CandleComparer : IComparer<ManagedObjectBase> 
        {
            public int Compare(ManagedObjectBase n1, ManagedObjectBase n2){
                return n2.IsObsoleted() ? 1 : 
                        n1.IsObsoleted() ? -1 : 
                            n2.hotTime.CompareTo(n1.hotTime);
            }
        }

        private class SerializerGeneral
        {	
            private static IDictionary<Type, SerializerGeneral> instances = new ConcurrentDictionary<Type, SerializerGeneral>();
            
            private Func<object, string> serialzeFunc;
            private Func<string, object> deserializeFunc;
            private Func<object, long> estimateObjectSizeFunc;
            
            // it's ok to in-thread safe here, as object override wouldn't cause any issue.
            public static SerializerGeneral GetSerializerBaseInstance<T>(IAsyncMemSerializer<T> serializer) where T : class
            {
                
                if(!(SerializerGeneral.instances.TryGetValue(serializer.GetType(), out SerializerGeneral inst) && inst != null))
                {
                    inst = new SerializerGeneral();

                    inst.serialzeFunc = (obj) => {
                        return serializer.Serialize(obj as T);
                    };		
                    
                    inst.deserializeFunc = (data) => {
                        return serializer.Deserialize(data);
                    };
                    
                    inst.estimateObjectSizeFunc = (obj) => {
                        return serializer.EstimateObjectSize(obj as T);
                    };

                    SerializerGeneral.instances.Add(serializer.GetType(), inst);
                }
                
                return SerializerGeneral.instances[serializer.GetType()];
            }

            private SerializerGeneral()
            {}
                       
            public string Serialize(object obj)
            {
                return this.serialzeFunc(obj);
            }
            
            public T Deserialize<T>(string data) where T : class
            {
                return this.deserializeFunc(data) as T;
            }
            
            public long EstimateObjectSize(object obj)
            {
                return this.estimateObjectSizeFunc(obj);
            }
        }    
    }
}
